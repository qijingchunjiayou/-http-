#include "cloud_upload.h"
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "cloud_config.h"
#include "board_config.h"
#include "cJSON.h"
#include "imu_calibration.h"
#include "esp_crt_bundle.h"
#include "esp_http_client.h"
#include "esp_log.h"
#include "esp_heap_caps.h"
#include "esp_mac.h"
#include "esp_netif_sntp.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include "glove_identity.h"
#include "glove_recording.h"
#include "cloud_presence.h"
#include "wifi_transport.h"

/*
 * 左手录制上传管线：
 *
 *   本地帧 ─┐
 *           ├─ 待配对缓冲 ─ 配对任务 ─ 上传队列 ─ HTTP 任务
 *   右手帧 ─┘
 *
 * - 双向对时由 glove_sync 执行，右手携带映射到左手时间轴的采样中点。
 * - 只有同一计划采样时隙、同步有效且中点差不超过门限的帧才配对。
 * - CSV 时间为计划时隙，原始时间和实际中点差保留在诊断字段中。
 * - 配对最多等待 PAIR_HOLD_MS 让后到的对侧帧入队；缺帧超过
 *   PAIR_GIVE_UP_MS 后按单手上传。
 * - 每批经 HTTPS POST 发送，服务端写入数据库后回 ACK；未确认时
 *   保留原批次并重发，服务端按 device/session/sequence 幂等去重。
 *
 * 右手固件不启动本模块，只经 ESP-NOW 发送。
 */
#define PENDING_CAPACITY   40
#define UPLOAD_QUEUE_SIZE  6000 /* 120 seconds at 50 Hz, in external RAM. */
#define BATCH_PAIRS        40
#define BODY_SIZE          70000
#define PARTIAL_BATCH_MS   500
#define HTTP_TIMEOUT_MS 10000
#define HTTP_REPLY_SIZE 1024

#define PAIR_HOLD_MS       40
#define PAIR_TOLERANCE_MS  5
#define PAIR_GIVE_UP_MS    200

#define NTP_VALID_UNIX     1704067200

typedef struct {
    imu_frame_t frame;
    int64_t aligned_ms;   /* 统一到左手启动毫秒时间轴 */
    int64_t source_ms;    /* 该手自己的原始 timestamp_ms */
    uint64_t session;
    uint8_t mac[6];       /* 右手 STA MAC */
} pending_frame_t;

typedef struct {
    pending_frame_t items[PENDING_CAPACITY];
    unsigned count;
} pending_buffer_t;

typedef struct {
    imu_frame_t left;
    imu_frame_t right;
    int64_t left_source_ms;
    int64_t right_source_ms;
    int64_t aligned_ms;
    int64_t unix_ms;
    uint64_t session;
    uint8_t right_mac[6];
    uint32_t sequence;
    bool has_left;
    bool has_right;
} pair_t;

static const char *TAG = "cloud_upload";

static char s_local_device[20];
static bool s_initialized;

static pending_buffer_t s_pending_left;
static pending_buffer_t s_pending_right;
static QueueHandle_t s_upload_queue;
static volatile unsigned s_batch_pending;
static StaticQueue_t s_upload_queue_control;
static uint8_t *s_upload_queue_storage;
static portMUX_TYPE s_pending_lock = portMUX_INITIALIZER_UNLOCKED;

static uint32_t s_left_dropped;
static uint32_t s_right_dropped;

bool cloud_upload_has_pending(void)
{
    return s_initialized && (s_batch_pending ||
        uxQueueMessagesWaiting(s_upload_queue) ||
        s_pending_left.count || s_pending_right.count);
}
static uint32_t s_pair_dropped;
static uint32_t s_pairs_emitted;
static uint32_t s_pairs_uploaded;
static uint32_t s_frames_uploaded;
static uint32_t s_solo_left;
static uint32_t s_solo_right;

/* 仅配对任务访问。 */
static uint64_t s_pair_session;
static uint32_t s_pair_sequence;
static int64_t s_session_first_slot;

static bool append(char *body, size_t *used, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int n = vsnprintf(body + *used, BODY_SIZE - *used, fmt, args);
    va_end(args);
    if (n < 0 || (size_t)n >= BODY_SIZE - *used) return false;
    *used += (size_t)n;
    return true;
}

static void format_mac(const uint8_t mac[6], char out[20])
{
    snprintf(out, 20, "%02x%02x%02x%02x%02x%02x",
             mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

static int64_t current_unix_ms(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec >= NTP_VALID_UNIX
        ? (int64_t)now.tv_sec * 1000 + now.tv_usec / 1000
        : 0;
}

static bool pending_push(pending_buffer_t *buffer, const pending_frame_t *item)
{
    bool ok = false;
    portENTER_CRITICAL(&s_pending_lock);
    if (buffer->count < PENDING_CAPACITY) {
        buffer->items[buffer->count++] = *item;
        ok = true;
    }
    portEXIT_CRITICAL(&s_pending_lock);
    return ok;
}

/* 调用者必须持有 s_pending_lock。 */
static void pending_remove(pending_buffer_t *buffer, unsigned index)
{
    if (index + 1 < buffer->count) {
        memmove(&buffer->items[index], &buffer->items[index + 1],
                (buffer->count - index - 1) * sizeof(buffer->items[0]));
    }
    --buffer->count;
}

/* 调用者必须持有 s_pending_lock。 */
static int pending_oldest(const pending_buffer_t *buffer, pending_frame_t *out)
{
    if (buffer->count == 0) return -1;
    unsigned best = 0;
    for (unsigned i = 1; i < buffer->count; ++i) {
        if (buffer->items[i].aligned_ms < buffer->items[best].aligned_ms) {
            best = i;
        }
    }
    *out = buffer->items[best];
    return (int)best;
}

/* 调用者必须持有 s_pending_lock。 */
static int pending_nearest(const pending_buffer_t *buffer, uint64_t session,
                           const imu_frame_t *reference, int64_t *diff_out)
{
    int best = -1;
    int64_t best_diff = INT64_MAX;
    for (unsigned i = 0; i < buffer->count; ++i) {
        if (buffer->items[i].session != session) continue;
        const imu_frame_t *candidate = &buffer->items[i].frame;
        if (!candidate->sync_valid || !reference->sync_valid ||
            candidate->scheduled_us != reference->scheduled_us) continue;
        int64_t diff_us = candidate->aligned_us - reference->aligned_us;
        if (diff_us < 0) diff_us = -diff_us;
        if (diff_us > PAIR_TOLERANCE_MS * 1000) continue;
        int64_t diff = diff_us / 1000;
        if (diff < 0) diff = -diff;
        if (diff < best_diff) {
            best = (int)i;
            best_diff = diff;
        }
    }
    if (best >= 0 && diff_out != NULL) *diff_out = best_diff;
    return best;
}

static void emit_pair(const pending_frame_t *left, const pending_frame_t *right)
{
    const pending_frame_t *first = left != NULL ? left : right;
    if (first == NULL) return;

    pair_t pair = {0};
    pair.session = first->session;
    pair.aligned_ms = first->frame.sync_valid ? first->frame.scheduled_us / 1000 : first->aligned_ms;
    pair.unix_ms = current_unix_ms();
    pair.has_left = left != NULL;
    pair.has_right = right != NULL;
    if (left != NULL) {
        pair.left = left->frame;
        pair.left_source_ms = left->source_ms;
    }
    if (right != NULL) {
        pair.right = right->frame;
        pair.right_source_ms = right->source_ms;
        memcpy(pair.right_mac, right->mac, sizeof(pair.right_mac));
    }

    if (pair.session != s_pair_session) {
        s_pair_session = pair.session;
        s_pair_sequence = 0;
        s_session_first_slot = first->frame.scheduled_us;
    }
    if (first->frame.sync_valid && s_session_first_slot >= 0 &&
        first->frame.scheduled_us >= s_session_first_slot) {
        pair.sequence = (uint32_t)((first->frame.scheduled_us - s_session_first_slot) /
                                  (IMU_SAMPLE_INTERVAL_MS * 1000));
        s_pair_sequence = pair.sequence + 1;
    } else pair.sequence = s_pair_sequence++;

    if (pair.has_left) {
        pair.left.timestamp_ms = pair.aligned_ms;
        pair.left.sequence = pair.sequence;
    }
    if (pair.has_right) {
        pair.right.timestamp_ms = pair.aligned_ms;
        pair.right.sequence = pair.sequence;
    }

    if ((s_pairs_emitted % 100) == 0) {
        ESP_LOGI(TAG, "PAIR slot=%" PRId64 " paired=%d delta_us=%" PRId64 " left_scan=%" PRIu32 " right_scan=%" PRIu32,
                 first->frame.scheduled_us, left && right,
                 left && right ? right->frame.aligned_us-left->frame.aligned_us : 0,
                 left ? left->frame.scan_us : 0, right ? right->frame.scan_us : 0);
    }
    ++s_pairs_emitted;
    if (xQueueSend(s_upload_queue, &pair, 0) != pdTRUE) {
        ++s_pair_dropped;
        if (s_pair_dropped == 1 || (s_pair_dropped % 100) == 0) {
            ESP_LOGW(TAG, "上传队列已满，丢弃配对=%" PRIu32, s_pair_dropped);
        }
    }

    if ((s_pairs_emitted % 500) == 0) {
        ESP_LOGI(TAG,
                 "配对进度：共%" PRIu32 "对，左单=%" PRIu32 "，右单=%" PRIu32
                 "，待上传=%u，配对丢弃=%" PRIu32,
                 s_pairs_emitted, s_solo_left, s_solo_right,
                 (unsigned)uxQueueMessagesWaiting(s_upload_queue),
                 s_pair_dropped);
    }
}

static void pair_task(void *arg)
{
    (void)arg;

    for (;;) {
        bool acted = false;

        for (;;) {
            pending_frame_t left = {0};
            pending_frame_t right = {0};
            int left_index;
            int right_index;

            portENTER_CRITICAL(&s_pending_lock);
            left_index = pending_oldest(&s_pending_left, &left);
            right_index = pending_oldest(&s_pending_right, &right);
            portEXIT_CRITICAL(&s_pending_lock);

            if (left_index < 0 && right_index < 0) break;

            bool use_left = left_index >= 0 &&
                            (right_index < 0 ||
                             left.aligned_ms <= right.aligned_ms);
            pending_frame_t oldest = use_left ? left : right;
            int64_t now_ms = esp_timer_get_time() / 1000;

            /* 等对侧最近帧入队，再决定配对还是单手上传。 */
            if (now_ms - oldest.aligned_ms < PAIR_HOLD_MS) break;

            int other_index;
            int64_t diff = INT64_MAX;

            portENTER_CRITICAL(&s_pending_lock);
            if (use_left) {
                other_index = pending_nearest(&s_pending_right, left.session,
                                              &left.frame, &diff);
            } else {
                other_index = pending_nearest(&s_pending_left, right.session,
                                              &right.frame, &diff);
            }
            portEXIT_CRITICAL(&s_pending_lock);

            bool pair_now = other_index >= 0 && diff <= PAIR_TOLERANCE_MS;
            bool give_up = now_ms - oldest.aligned_ms >= PAIR_GIVE_UP_MS;
            if (!pair_now && !give_up) break;

            pending_frame_t chosen_left = {0};
            pending_frame_t chosen_right = {0};

            portENTER_CRITICAL(&s_pending_lock);
            if (pair_now) {
                if (use_left) {
                    chosen_left = s_pending_left.items[left_index];
                    chosen_right = s_pending_right.items[other_index];
                    pending_remove(&s_pending_right, (unsigned)other_index);
                    pending_remove(&s_pending_left, (unsigned)left_index);
                } else {
                    chosen_right = s_pending_right.items[right_index];
                    chosen_left = s_pending_left.items[other_index];
                    pending_remove(&s_pending_left, (unsigned)other_index);
                    pending_remove(&s_pending_right, (unsigned)right_index);
                }
            } else if (use_left) {
                chosen_left = s_pending_left.items[left_index];
                pending_remove(&s_pending_left, (unsigned)left_index);
                ++s_solo_left;
            } else {
                chosen_right = s_pending_right.items[right_index];
                pending_remove(&s_pending_right, (unsigned)right_index);
                ++s_solo_right;
            }
            portEXIT_CRITICAL(&s_pending_lock);

            if (pair_now) {
                emit_pair(&chosen_left, &chosen_right);
            } else if (use_left) {
                emit_pair(&chosen_left, NULL);
            } else {
                emit_pair(NULL, &chosen_right);
            }

            acted = true;
            break; /* 重新从两个缓冲中找最早帧 */
        }

        if (!acted) {
            vTaskDelay(pdMS_TO_TICKS(10));
        }
    }
}

static bool append_frame(char *body, size_t *used, const imu_frame_t *frame,
                         int64_t unix_ms)
{
    unsigned valid = 0;
    unsigned mag = 0;
    for (unsigned ch = 0; ch < IMU_TOTAL_CHANNELS; ++ch) {
        if (frame->channel[ch].valid) valid |= 1U << ch;
        if (frame->channel[ch].has_mag) mag |= 1U << ch;
    }

    if (!append(body, used, "[%" PRId64 ",%" PRIu32 ",%u,%u,",
                frame->timestamp_ms, frame->sequence, valid, mag)) return false;
    if (unix_ms) {
        if (!append(body, used, "%" PRId64, unix_ms)) return false;
    } else if (!append(body, used, "null")) {
        return false;
    }

    for (unsigned ch = 0; ch < IMU_TOTAL_CHANNELS; ++ch) {
        const imu_channel_data_t *d = &frame->channel[ch];
        if (!append(body, used, ",%d,%d,%d,%d,%d,%d",
                    d->ax, d->ay, d->az, d->gx, d->gy, d->gz)) return false;
        if (ch == IMU_ICM20948_INDEX &&
            !append(body, used, ",%d,%d,%d", d->mx, d->my, d->mz)) {
            return false;
        }
    }
    return append(body, used, "]");
}

static bool batch_has(const pair_t *batch, unsigned count, bool left)
{
    for (unsigned i = 0; i < count; ++i) {
        if (left ? batch[i].has_left : batch[i].has_right) return true;
    }
    return false;
}

static const uint8_t *batch_right_mac(const pair_t *batch, unsigned count)
{
    for (unsigned i = 0; i < count; ++i) {
        if (batch[i].has_right) return batch[i].right_mac;
    }
    return NULL;
}

static bool append_frames(char *body, size_t *used, const pair_t *batch,
                          unsigned count, bool left)
{
    if (!append(body, used, "\"frames\":[")) return false;
    bool first = true;
    for (unsigned i = 0; i < count; ++i) {
        bool has = left ? batch[i].has_left : batch[i].has_right;
        if (!has) continue;
        if (!append(body, used, "%s", first ? "" : ",")) return false;
        const imu_frame_t *frame = left ? &batch[i].left : &batch[i].right;
        if (!append_frame(body, used, frame, batch[i].unix_ms)) return false;
        first = false;
    }
    return append(body, used, "]");
}

static bool append_source_timestamps(char *body, size_t *used,
                                     const pair_t *batch, unsigned count,
                                     bool left)
{
    if (!append(body, used, "\"source_timestamps\":[")) return false;
    bool first = true;
    for (unsigned i = 0; i < count; ++i) {
        bool has = left ? batch[i].has_left : batch[i].has_right;
        if (!has) continue;
        int64_t source = left ? batch[i].left_source_ms : batch[i].right_source_ms;
        if (!append(body, used, "%s%" PRId64, first ? "" : ",", source)) {
            return false;
        }
        first = false;
    }
    return append(body, used, "]");
}

/* Per-hand diagnostics; dataset export remains exactly 77 columns. */
static bool append_sync_quality(char *body, size_t *used, const pair_t *batch,
                                unsigned count, bool left)
{
    if (!append(body, used, ",\"sync_quality\":[")) return false;
    bool first = true;
    for (unsigned i = 0; i < count; ++i) {
        const pair_t *p = &batch[i];
        if (!(left ? p->has_left : p->has_right)) continue;
        const imu_frame_t *f = left ? &p->left : &p->right;
        int64_t delta = p->has_left && p->has_right ? p->right.aligned_us - p->left.aligned_us : 0;
        if (!append(body, used, "%s[%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%d,%d,%" PRId64 "]",
                    first ? "" : ",", f->source_us, f->aligned_us,
                    f->scheduled_us, f->clock_offset_us, f->scan_us,
                    f->sync_rtt_us, f->sync_age_ms, f->sync_valid,
                    p->has_left && p->has_right, delta)) return false;
        first = false;
    }
    return append(body, used, "]");
}

static bool append_calibrated(char *body, size_t *used, const pair_t *batch,
                              unsigned count)
{
    if (!append(body, used, ",\"calibration_version\":\"%s\",\"calibrated\":[",
                imu_calibration_version())) return false;

    bool first = true;
    for (unsigned i = 0; i < count; ++i) {
        if (!batch[i].has_left) continue;
        if (!append(body, used, "%s[", first ? "" : ",")) return false;
        for (int ch = 0; ch < 3; ++ch) {
            float output[6];
            bool valid = imu_calibration_apply(ch, &batch[i].left.channel[ch],
                                               output);
            for (int axis = 0; axis < 6; ++axis) {
                if (!append(body, used, "%s", ch || axis ? "," : "")) {
                    return false;
                }
                if (valid) {
                    if (!append(body, used, "%.5f", (double)output[axis])) {
                        return false;
                    }
                } else if (!append(body, used, "null")) {
                    return false;
                }
            }
        }
        if (!append(body, used, "]")) return false;
        first = false;
    }
    return append(body, used, "]");
}

/* 双手单请求：{"session":"...","hands":[{"device":...,"hand":"left",...},...]} */
static bool encode_combined(char *body, const pair_t *batch, unsigned count,
                            size_t *used)
{
    char batch_id[80];
    snprintf(batch_id, sizeof(batch_id), "%016" PRIx64 "-%" PRIu32 "-%" PRIu32,
             batch[0].session, batch[0].sequence, batch[count - 1].sequence);
    *used = 0;
    if (!append(body, used,
                "{\"type\":\"ingest\",\"batch_id\":\"%s\",\"session\":\"%016" PRIx64 "\",\"hands\":[",
                batch_id, batch[0].session)) return false;

    bool first_hand = true;
    if (batch_has(batch, count, true)) {
        if (!append(body, used, "%s{\"device\":\"%s\",\"hand\":\"left\",",
                    first_hand ? "" : ",", s_local_device)) return false;
        if (!append_frames(body, used, batch, count, true)) return false;
        if (!append(body, used, ",")) return false;
        if (!append_source_timestamps(body, used, batch, count, true)) {
            return false;
        }
        if (!append_sync_quality(body, used, batch, count, true)) return false;
        if (!append_calibrated(body, used, batch, count)) return false;
        if (!append(body, used, "}")) return false;
        first_hand = false;
    }

    if (batch_has(batch, count, false)) {
        char device[20];
        format_mac(batch_right_mac(batch, count), device);
        if (!append(body, used, "%s{\"device\":\"%s\",\"hand\":\"right\",",
                    first_hand ? "" : ",", device)) return false;
        if (!append_frames(body, used, batch, count, false)) return false;
        if (!append(body, used, ",")) return false;
        if (!append_source_timestamps(body, used, batch, count, false)) {
            return false;
        }
        if (!append_sync_quality(body, used, batch, count, false)) return false;
        if (!append(body, used, "}")) return false;
    }

    return append(body, used, "]}");
}

typedef struct {
    char data[HTTP_REPLY_SIZE];
    size_t used;
    bool overflow;
} http_reply_t;

static esp_err_t http_event_handler(esp_http_client_event_t *event)
{
    http_reply_t *reply = event->user_data;
    if (event->event_id == HTTP_EVENT_ON_DATA && reply != NULL &&
        event->data != NULL && event->data_len > 0) {
        size_t available = sizeof(reply->data) - 1 - reply->used;
        size_t copy = (size_t)event->data_len;
        if (copy > available) {
            reply->overflow = true;
            copy = available;
        }
        memcpy(reply->data + reply->used, event->data, copy);
        reply->used += copy;
        reply->data[reply->used] = '\0';
    }
    return ESP_OK;
}

static bool http_post_json(esp_http_client_handle_t client, const char *body,
                           size_t length, http_reply_t *reply)
{
    reply->used = 0;
    reply->overflow = false;
    reply->data[0] = '\0';
    esp_http_client_set_url(client, GLOVE_CLOUD_URL);
    esp_http_client_set_method(client, HTTP_METHOD_POST);
    esp_http_client_set_header(client, "Content-Type", "application/json");
    esp_http_client_set_header(client, "Authorization", "Bearer " GLOVE_CLOUD_TOKEN);
    esp_http_client_set_post_field(client, body, (int)length);
    esp_err_t err = esp_http_client_perform(client);
    int status = esp_http_client_get_status_code(client);
    esp_http_client_set_post_field(client, NULL, 0);
    if (err != ESP_OK || status != 200 || reply->overflow) {
        ESP_LOGW(TAG, "HTTP POST 未确认：err=%s status=%d response_bytes=%u",
                 esp_err_to_name(err), status, (unsigned)reply->used);
        esp_http_client_close(client);
        return false;
    }
    return true;
}

static bool http_ack_matches(const char *reply, int expected_rows)
{
    cJSON *root = cJSON_Parse(reply);
    if (root == NULL) return false;
    const cJSON *accepted = cJSON_GetObjectItemCaseSensitive(root, "accepted");
    bool ok = cJSON_IsNumber(accepted) && accepted->valueint == expected_rows;
    cJSON_Delete(root);
    return ok;
}

static bool upload_pairs(esp_http_client_handle_t client, char *body,
                         http_reply_t *reply, const pair_t *batch,
                         unsigned count)
{
    if (count == 0) return true;
    size_t length;
    int64_t encode_start = esp_timer_get_time();
    if (!encode_combined(body, batch, count, &length)) {
        ESP_LOGE(TAG, "HTTP 批次超出缓冲，保留并停止自动重试");
        return false;
    }
    int64_t encode_us = esp_timer_get_time() - encode_start;
    char batch_id[80];
    snprintf(batch_id, sizeof(batch_id), "%016" PRIx64 "-%" PRIu32 "-%" PRIu32,
             batch[0].session, batch[0].sequence, batch[count - 1].sequence);
    int expected_rows = 0;
    for (unsigned i = 0; i < count; ++i) {
        expected_rows += batch[i].has_left + batch[i].has_right;
    }
    int64_t send_start = esp_timer_get_time();
    bool acknowledged = http_post_json(client, body, length, reply) &&
        http_ack_matches(reply->data, expected_rows);
    int64_t transfer_us = esp_timer_get_time() - send_start;
    if (!acknowledged) {
        ESP_LOGW(TAG, "批次未收到有效 ACK，保留原批重发：batch_id=%s", batch_id);
        return false;
    }

    s_pairs_uploaded += count;
    int64_t capacity = (int64_t)count * 1000000 /
                       (encode_us + transfer_us > 0 ? encode_us + transfer_us : 1);
    int64_t lag_ms = esp_timer_get_time() / 1000 - batch[0].aligned_ms;
    ESP_LOGI(TAG, "HTTP 批次 %u 对，%u B，编码=%lld ms，ACK往返=%lld ms，"
             "处理能力=%lld 对/s，待上传=%u，本批最早数据滞后=%lld ms，free_heap=%u B",
             count, (unsigned)length, (long long)(encode_us / 1000),
             (long long)(transfer_us / 1000), (long long)capacity,
             (unsigned)uxQueueMessagesWaiting(s_upload_queue), (long long)lag_ms,
             (unsigned)esp_get_free_heap_size());
    return true;
}

static void upload_task(void *arg)
{
    (void)arg;

    char *body = malloc(BODY_SIZE);
    if (body == NULL) {
        ESP_LOGE(TAG, "上传模块内存分配失败");
        vTaskDelete(NULL);
        return;
    }
    ESP_LOGI(TAG, "HTTP 上传缓冲：body=%d B，free_heap=%u B",
             BODY_SIZE, (unsigned)esp_get_free_heap_size());

    bool clock_started = false;
    http_reply_t reply = {0};
    const esp_http_client_config_t http_config = {
        .url = GLOVE_CLOUD_URL,
        .event_handler = http_event_handler,
        .user_data = &reply,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .timeout_ms = HTTP_TIMEOUT_MS,
        .keep_alive_enable = true,
    };
    esp_http_client_handle_t client = esp_http_client_init(&http_config);
    if (client == NULL) {
        ESP_LOGE(TAG, "HTTP 客户端初始化失败");
        free(body);
        vTaskDelete(NULL);
        return;
    }
    /* 整批数据不能放任务栈上（曾导致 cloud_upload 栈溢出复位）。 */
    static pair_t batch[BATCH_PAIRS];
    unsigned count = 0;
    uint64_t batch_session = 0;
    int64_t batch_started_ms = 0;
    uint32_t last_logged = 0;
    int64_t last_wait_log_ms = 0;
    int64_t last_sntp_retry_ms = 0;
    int64_t last_presence_ms = -5000;
    bool time_valid_logged = false;

    ESP_LOGI(TAG, "HTTP 上传就绪：%s，device=%s，等待服务端逐批确认",
             GLOVE_CLOUD_URL, s_local_device);
    ESP_LOGI(TAG, "双手对齐：hold=%d ms，tolerance=%d ms，give_up=%d ms，"
             "batch=%d 对",
             PAIR_HOLD_MS, PAIR_TOLERANCE_MS, PAIR_GIVE_UP_MS, BATCH_PAIRS);
    ESP_LOGI(TAG, "队列=%d 对，pair_t=%u B，free_heap=%u B",
             UPLOAD_QUEUE_SIZE, (unsigned)sizeof(pair_t),
             (unsigned)esp_get_free_heap_size());
    ESP_LOGI(TAG, "CH0-2 校准版本：%s", imu_calibration_version());

    for (;;) {
        int64_t wait_now_ms = esp_timer_get_time() / 1000;
        if (!wifi_transport_is_connected()) {
            clock_started = false;
            esp_http_client_close(client);
            if (wait_now_ms - last_wait_log_ms >= 5000) {
                last_wait_log_ms = wait_now_ms;
                ESP_LOGW(TAG, "上传等待：Wi-Fi 未连接");
            }
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }
        if (!clock_started) {
            esp_err_t sntp_ret = esp_netif_sntp_start();
            ESP_LOGI(TAG, "SNTP 已启动：%s", esp_err_to_name(sntp_ret));
            clock_started = true;
        }

        time_t wall = time(NULL);
        if (wall < NTP_VALID_UNIX) {
            if (wait_now_ms - last_sntp_retry_ms >= 15000) {
                last_sntp_retry_ms = wait_now_ms;
                esp_err_t sntp_ret = esp_netif_sntp_start();
                if (sntp_ret != ESP_ERR_INVALID_STATE) {
                    ESP_LOGW(TAG, "重新启动 SNTP：%s", esp_err_to_name(sntp_ret));
                }
            }
            if (wait_now_ms - last_wait_log_ms >= 5000) {
                last_wait_log_ms = wait_now_ms;
                ESP_LOGW(TAG, "上传等待：SNTP 未校时（time=%lld）",
                         (long long)wall);
            }
            vTaskDelay(pdMS_TO_TICKS(500));
            continue;
        }
        if (!time_valid_logged) {
            time_valid_logged = true;
            ESP_LOGI(TAG, "SNTP 校时完成：time=%lld", (long long)wall);
        }

        if (wait_now_ms - last_presence_ms >= 5000) {
            if (cloud_presence_send(client) != ESP_OK) {
                ESP_LOGW(TAG, "HTTP heartbeat 失败，保留上传队列重试");
                vTaskDelay(pdMS_TO_TICKS(500));
                continue;
            }
            last_presence_ms = wait_now_ms;
        }

        int64_t now_ms = esp_timer_get_time() / 1000;
        while (count < BATCH_PAIRS) {
            pair_t next;
            if (xQueuePeek(s_upload_queue, &next, 0) != pdTRUE) break;

            if (count > 0) {
                if (next.session != batch_session) break;
                const uint8_t *mac = batch_right_mac(batch, count);
                if (mac != NULL && next.has_right &&
                    memcmp(mac, next.right_mac, sizeof(next.right_mac)) != 0) {
                    break;
                }
            }
            if (xQueueReceive(s_upload_queue, &next, 0) != pdTRUE) break;
            if (count == 0) {
                batch_session = next.session;
                batch_started_ms = now_ms;
            }
            batch[count++] = next;
            s_batch_pending = count;
        }

        if (count == 0) {
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }

        bool full = count >= BATCH_PAIRS;
        bool stale = now_ms - batch_started_ms >= PARTIAL_BATCH_MS;
        bool changed = false;
        pair_t next;
        if (xQueuePeek(s_upload_queue, &next, 0) == pdTRUE) {
            changed = next.session != batch_session;
            const uint8_t *mac = batch_right_mac(batch, count);
            if (!changed && mac != NULL && next.has_right) {
                changed = memcmp(mac, next.right_mac,
                                 sizeof(next.right_mac)) != 0;
            }
        }

        if (full || stale || changed) {
            uint32_t uploaded_before = s_pairs_uploaded;
            if (upload_pairs(client, body, &reply, batch, count)) {
                last_presence_ms = wait_now_ms;
                if (s_pairs_uploaded != uploaded_before) {
                    for (unsigned i = 0; i < count; ++i) {
                        s_frames_uploaded += batch[i].has_left + batch[i].has_right;
                    }
                }
                count = 0;
                s_batch_pending = 0;
                if (s_pairs_uploaded - last_logged >= 500) {
                    last_logged = s_pairs_uploaded;
                    ESP_LOGI(TAG, "已上传 %" PRIu32 " 对（双手合计 %" PRIu32
                             " 帧）", s_pairs_uploaded, s_frames_uploaded);
                }
            } else {
                vTaskDelay(pdMS_TO_TICKS(500));
            }
        } else {
            vTaskDelay(pdMS_TO_TICKS(10));
        }
    }
}

esp_err_t cloud_upload_init(void)
{
    if (s_initialized) return ESP_OK;

    uint8_t mac[6];
    esp_read_mac(mac, ESP_MAC_WIFI_STA);
    format_mac(mac, s_local_device);

    ESP_LOGI(TAG, "初始化前 free_heap=%u B",
             (unsigned)esp_get_free_heap_size());
    s_upload_queue_storage = heap_caps_malloc(UPLOAD_QUEUE_SIZE * sizeof(pair_t),
                                              MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (s_upload_queue_storage == NULL) return ESP_ERR_NO_MEM;
    s_upload_queue = xQueueCreateStatic(UPLOAD_QUEUE_SIZE, sizeof(pair_t),
                                       s_upload_queue_storage, &s_upload_queue_control);
    if (s_upload_queue == NULL) {
        heap_caps_free(s_upload_queue_storage);
        s_upload_queue_storage = NULL;
        return ESP_ERR_NO_MEM;
    }
    ESP_LOGI(TAG, "队列=%d 对（%u B/对），free_heap=%u B",
             UPLOAD_QUEUE_SIZE, (unsigned)sizeof(pair_t),
             (unsigned)esp_get_free_heap_size());

    esp_sntp_config_t sntp = ESP_NETIF_SNTP_DEFAULT_CONFIG("ntp.aliyun.com");
    sntp.start = false;
    esp_err_t ret = esp_netif_sntp_init(&sntp);
    if (ret != ESP_OK && ret != ESP_ERR_INVALID_STATE) {
        vQueueDelete(s_upload_queue);
        s_upload_queue = NULL;
        heap_caps_free(s_upload_queue_storage);
        s_upload_queue_storage = NULL;
        return ret;
    }

    /* 优先级不高于主循环：TLS 握手若抢占主任务会拖慢采样和录制心跳。 */
    if (xTaskCreate(upload_task, "cloud_upload", 12288, NULL, 1, NULL) != pdPASS) {
        vQueueDelete(s_upload_queue);
        s_upload_queue = NULL;
        heap_caps_free(s_upload_queue_storage);
        s_upload_queue_storage = NULL;
        return ESP_ERR_NO_MEM;
    }
    if (xTaskCreate(pair_task, "glove_pair", 4096, NULL, 2, NULL) != pdPASS) {
        /* 上传任务已运行但会一直等待空队列；返回错误由主程序记录。 */
        return ESP_ERR_NO_MEM;
    }

    s_initialized = true;
    return ESP_OK;
}

void cloud_upload_submit(const imu_frame_t *frame)
{
    if (!s_initialized || frame == NULL) return;

    uint64_t session;
    if (!glove_recording_get(&session)) return;

    pending_frame_t item = {
        .frame = *frame,
        .aligned_ms = frame->timestamp_ms,
        .source_ms = frame->timestamp_ms,
        .session = session,
    };
    if (!pending_push(&s_pending_left, &item)) {
        ++s_left_dropped;
        if (s_left_dropped == 1 || (s_left_dropped % 100) == 0) {
            ESP_LOGW(TAG, "左手待配对缓冲已满，丢弃=%" PRIu32, s_left_dropped);
        }
    }
}

void cloud_upload_submit_remote(const uint8_t src_mac[6], uint64_t session,
                                const imu_frame_t *frame)
{
    if (!s_initialized || frame == NULL || src_mac == NULL) return;
    /* Never invent a left-clock timestamp for an unsynchronized right frame. */
    if (!frame->sync_valid || frame->scheduled_us < 0) return;

    uint64_t current;
    if (!glove_recording_get(&current)) return;
    if (session == 0 || session != current) {
        /* 右手还没切换到本次录制，不能混入当前 CSV。 */
        return;
    }

    pending_frame_t item = {
        .frame = *frame,
        .aligned_ms = frame->aligned_us / 1000,
        .source_ms = frame->timestamp_ms,
        .session = session,
    };
    memcpy(item.mac, src_mac, sizeof(item.mac));

    if (!pending_push(&s_pending_right, &item)) {
        ++s_right_dropped;
        if (s_right_dropped == 1 || (s_right_dropped % 100) == 0) {
            ESP_LOGW(TAG, "右手待配对缓冲已满，丢弃=%" PRIu32, s_right_dropped);
        }
    }
}
