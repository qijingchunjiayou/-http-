#include "wifi_provisioning.h"

#include <stdio.h>

#include "esp_check.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_wifi.h"
#include "glove_identity.h"
#include "network_provisioning/manager.h"
#include "network_provisioning/scheme_ble.h"

static const char *TAG = "wifi_prov";

#if CONFIG_GLOVE_HAND_LEFT
static const char *s_pop = "glove-left";
#else
static const char *s_pop = "glove-right";
#endif

static void provisioning_event_handler(void *arg, esp_event_base_t event_base,
                                       int32_t event_id, void *event_data)
{
    (void)arg;
    (void)event_data;
    if (event_base != NETWORK_PROV_EVENT) {
        return;
    }
    switch (event_id) {
    case NETWORK_PROV_START:
        ESP_LOGI(TAG, "BLE Wi-Fi provisioning started; use ESP BLE Provisioning");
        break;
    case NETWORK_PROV_WIFI_CRED_RECV:
        ESP_LOGI(TAG, "BLE provisioning received Wi-Fi credentials");
        break;
    case NETWORK_PROV_WIFI_CRED_FAIL:
        ESP_LOGE(TAG, "BLE provisioning Wi-Fi connection failed");
        break;
    case NETWORK_PROV_WIFI_CRED_SUCCESS:
        ESP_LOGI(TAG, "BLE provisioning Wi-Fi connection succeeded");
        break;
    case NETWORK_PROV_END:
        ESP_LOGI(TAG, "BLE provisioning service stopped");
        (void)network_prov_mgr_deinit();
        break;
    default:
        break;
    }
}

esp_err_t wifi_provisioning_start_if_needed(bool *started)
{
    if (started == NULL) {
        return ESP_ERR_INVALID_ARG;
    }
    *started = false;

    network_prov_mgr_config_t config = {
        .scheme = network_prov_scheme_ble,
        .scheme_event_handler = NETWORK_PROV_SCHEME_BLE_EVENT_HANDLER_FREE_BTDM,
        .app_event_handler = NETWORK_PROV_EVENT_HANDLER_NONE,
    };
    ESP_RETURN_ON_ERROR(network_prov_mgr_init(config), TAG, "BLE manager init failed");

    bool provisioned = false;
    ESP_RETURN_ON_ERROR(network_prov_mgr_is_wifi_provisioned(&provisioned), TAG,
                        "read provisioning state failed");
    if (provisioned) {
        return network_prov_mgr_deinit();
    }

    uint8_t mac[6] = {0};
    ESP_RETURN_ON_ERROR(esp_wifi_get_mac(WIFI_IF_STA, mac), TAG, "read Wi-Fi MAC failed");
    char service_name[24];
    snprintf(service_name, sizeof(service_name), "GLOVE_%s_%02X%02X",
             GLOVE_HAND_NAME_UPPER, mac[4], mac[5]);
    ESP_RETURN_ON_ERROR(esp_event_handler_register(NETWORK_PROV_EVENT,
                                                   ESP_EVENT_ANY_ID,
                                                   provisioning_event_handler,
                                                   NULL),
                        TAG, "register provisioning events failed");
    ESP_RETURN_ON_ERROR(network_prov_mgr_start_provisioning(
                            NETWORK_PROV_SECURITY_1, (const void *)s_pop,
                            service_name, NULL),
                        TAG, "BLE provisioning start failed");
    ESP_LOGI(TAG, "BLE device name: %s", service_name);
    ESP_LOGI(TAG, "BLE proof of possession: %s", s_pop);
    *started = true;
    return ESP_OK;
}
