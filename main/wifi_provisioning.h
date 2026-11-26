#pragma once

#include <stdbool.h>
#include "esp_err.h"

esp_err_t wifi_provisioning_start_if_needed(bool *started);
