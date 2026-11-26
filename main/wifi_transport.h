#pragma once

#include <stdbool.h>

#include "esp_err.h"

#ifdef __cplusplus
extern "C" {
#endif

esp_err_t wifi_transport_init(void);
bool wifi_transport_is_connected(void);

#ifdef __cplusplus
}
#endif
