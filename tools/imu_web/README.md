# IMU recorder

Public URL: https://yeyouzzz.cn/imu/

Two ingestion paths share one SQLite/WAL store:

| Path | Who uses it | Delivery |
| --- | --- | --- |
| `POST /api/ingest` | 旧固件（左手 HTTP 批量上传） | 整批确认 |
| `WSS /api/stream` `type=frame` | 新固件（左右手各自逐帧直传） | 逐帧确认，事务提交后才发确认 |

## 逐帧 WSS 协议（当前方案）

左右手各自保持一条 WSS 长连接，每采到一帧就单独发一条消息。右手不再把 IMU 帧经 ESP-NOW 转给左手，ESP-NOW 只保留对时、开始/停止、共享 `session` 和开始前的双手自检。

握手：`GET /api/stream`，`Upgrade: websocket`，`Authorization: Bearer <IMU_TOKEN>`，公网经 Nginx `/imu/api/stream` 代理。TLS 由 Nginx 终结，服务端只监听 `127.0.0.1`。

单帧消息：

```json
{"type":"frame","device":"A0:B1:C2:D3:E4:F5","hand":"left",
 "session":"651234567890123456","sequence":1043,
 "timestamp":60000,"source_us":60040000,"aligned_us":60041000,
 "scheduled_us":60040000,"clock_offset_us":-148000000,
 "scan_us":3100,"sync_rtt_us":900,"sync_age_ms":120,"sync_valid":true,
 "valid_mask":2047,"mag_mask":1024,
 "channels":[[ax,ay,az,gx,gy,gz], "... CH0–CH9", [ax,ay,az,gx,gy,gz,mx,my,mz]]}
```

- `device` 允许 MAC 原样（`:`/`.` 也接受）；`session` 仍是 `[A-Za-z0-9_-]{1,80}`。
- `channels` 也可用扁平 69 个值的 `values` 代替。
- `valid_mask` / `mag_mask` 位 `n` 对应 CH n。无效通道**一律存 NULL**，不接受全零冒充有效读数。
- `timestamp`（毫秒）是本机单调时间；`source_us` 本机扫描中点，`aligned_us` 换算到左手时间轴的中点，`scheduled_us` 共同时间轴上的计划时隙。`sync_valid=false` 时后两者可为 `null`。
- 服务端另存 `received_unix_ms` 作诊断，**绝不**用它代替采样时间。

确认（数据库事务提交后才发送）：

```json
{"type":"frame_ack","device":"A0:B1:C2:D3:E4:F5","hand":"left","session":"651234567890123456",
 "sequence":1043,"saved":true,"inserted":true,"pair":"paired","pair_id":88}
```

重复帧返回 `saved:true, inserted:false, duplicate:true`，不重复插入。校验失败或数据库错误返回 `{"type":"error","error":"..."}`，**不会**出现 `saved`，设备应保留该帧并重发。设备不需要等上一帧确认再发下一帧；断线重连后补发未确认帧即可，幂等键 `(device, session, sequence)` 保证不会重复入库。

## 服务端配对

配对只看 `session` + `scheduled_us` + `aligned_us` + `sync_valid`，**不看 sequence，也不看到达顺序**：

| 状态 | 含义 |
| --- | --- |
| `paired` | 双方同步有效、计划时隙相同、`aligned_us` 差值 ≤ 容差（默认 5 ms） |
| `late_paired` | 同上，但对侧是在等待窗口超时之后才到 |
| `pending` | 只收到一只手，仍在等待窗口内（默认 200 ms） |
| `missing_left` / `missing_right` | 等待超时，对侧始终没来；原始数据保留 |
| `out_of_tolerance` | 双手都在同一时隙，但中点差超过容差，不配成同一时刻 |
| `sync_invalid` | 该帧时钟同步无效，只保存不配对 |
| `slot_taken` | 同一只手在同一时隙发了第二帧，保存但不参与配对 |

超时的时隙由后台 janitor 关闭，**不会阻塞后续时隙**；晚到帧仍会补配成 `late_paired`。不同 `session` 绝不混配。

可调参数（环境变量，写入 `/etc/glove-imu.env`）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `IMU_SLOT_US` | 20000 | 计划时隙宽度，用于换算 `frame_id` |
| `IMU_PAIR_TOLERANCE_US` | 5000 | 中点差容差 |
| `IMU_PAIR_WAIT_MS` | 200 | 单手等待窗口 |
| `IMU_WINDOW_FRAMES` | 30 | 推理窗口帧数（1 帧不足以识别一个手语动作） |
| `IMU_INFER_QUEUE` | 64 | 推理队列容量 |

## 推理接口

推理**不在收帧路径上**。配对成功后只把 `session` 放进有界队列，队列满时计数并在 stderr 明确记录（帧已入库，不丢数据，窗口计算是幂等的）。窗口结果写入 `inference_runs` 表，`/api/pairing/stats` 可查。

模型尚未接上，`infer()` 固定返回 `model_not_ready`，**不会伪造识别结果**。接入时只需替换 `infer(window)`：窗口是一组连续 `paired` 时隙，每个元素带 `scheduled_us/left_id/right_id`。

## CSV 导出

`GET/POST /api/export.csv` 与 `pack_dataset.py` 使用同一套导出代码，格式以参考数据集为准：**77 列**，前 8 列为 `worker,v,label,active_hand,sample_id,timestamp/ms,frame_id,hand_type`，其后是 `f_0_s1…f_4_s2` 六轴与 `dorsal_` 九轴。

- `frame_id` = `(scheduled_us − 该 session 首个时隙) / 20 ms`，即计划时隙序号。丢帧就**留空缺号**，不压缩、不插值。
- `timestamp/ms` = 该时隙在右手时间轴上的毫秒值，左右手同一配对行相同。
- 旧固件没有时隙的数据回退为各自的 `sequence` 与 `timestamp_ms`。
- `counts=calibrated` 只校正左手 CH0–CH7（与原逻辑一致）；`format=archive` 输出诊断列，含 `source_us / aligned_us / scheduled_us / sync_valid / pair_state / pair_delta_us`。

服务端打包：

```sh
python3 tools/imu_web/pack_dataset.py --out exports/dataset.csv
python3 tools/imu_web/pack_dataset.py --session 651234567890123456 --counts calibrated --out one.csv
python3 tools/imu_web/pack_dataset.py --worker P001 --v medium --archive --out p001.csv
```

## 运维

```sh
systemctl status glove-imu
journalctl -u glove-imu -n 100
sqlite3 /var/lib/glove-imu/imu.sqlite3 '.backup /path/to/imu-backup.sqlite3'
```

`/api/pairing/stats` 给出验收计数器（收帧、入库、重复、配对、缺失、超容差、推理入队与丢弃），`/api/pairs?session=…` 可逐时隙查看配对状态。服务重启后 `initialize()` 从已确认的帧重建配对表，并把等待窗口之外的时隙标记为缺失，不会留下永远 `pending` 的时隙。

## 测试

```sh
python -m unittest discover -s tools/imu_web -p 'test_*.py'
```

- `test_server.py`：旧 HTTP 批量接口、标注、删除、编号、导出（回归）。
- `test_frame_stream.py`：逐帧确认、重复/断线补发、握手鉴权、配对与乱序/晚到/超容差/同步无效/单手超时、100 帧/秒、写入失败不确认、重启恢复、77 列布局。
- `test_pack_dataset.py`：服务端 CSV 打包与归档旁文件。
