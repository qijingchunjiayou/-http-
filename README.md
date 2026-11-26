# 双手机器人手套 · IMU 采集固件 + 云端记录台（HTTP 上传版）

ESP32-S3 双手手套，每手 11 路 IMU，右手经 ESP-NOW 把原始帧发给左手，
左手按 20 ms 时隙配对后通过 **HTTPS POST 批量上传**到服务器，服务器写入 SQLite 并提供网页查看/标注/导出。

本包只包含**源代码与配置**，不含编译产物、标定数据、Git 历史和任何凭据。

---

## 目录结构

```
.
├── main/                      固件源码（ESP-IDF 组件）
│   ├── main.c                 启动、任务编排
│   ├── serial_recording.c     @REC 串口指令（STATUS/CHECK/START/STOP）
│   ├── tca9548a.c             I2C 多路复用器
│   ├── mpu6050.c              CH0–CH9 指部六轴（加速度 + 陀螺）
│   ├── icm20948.c             CH10 掌部九轴（含磁力计）
│   ├── imu_acquisition.c      11 路顺序扫描，20 ms 时隙
│   ├── imu_calibration.c      标定系数应用
│   ├── glove_link.c           ESP-NOW 帧协议 + 录制控制
│   ├── glove_sync.c           四时间点双向对时
│   ├── glove_recording.c      录制会话状态
│   ├── cloud_upload.c         HTTPS 批量上传、等待服务端确认、失败重发
│   ├── cloud_presence.c       心跳
│   ├── wifi_transport.c       Wi-Fi STA / 重连
│   ├── wifi_provisioning.c    配网页面
│   ├── glove_buttons.c        按键交互
│   ├── max98357a_audio.c      语音提示（I2S）
│   ├── ssd1306.c              OLED
│   ├── battery_monitor.c      电量
│   ├── board_config.h         引脚定义
│   ├── cloud_config.example.h     ← 需复制为 cloud_config.h
│   └── wifi_credentials.example.h ← 需复制为 wifi_credentials.h
├── tools/
│   ├── idf.ps1                构建/烧录/串口控制入口（PowerShell）
│   ├── glove_serial_control.py 串口控制台（F2 开始/停止、F3 状态，921600）
│   ├── sync_cloud_token.py     从服务器同步上传令牌
│   ├── imu_ssh.py              远程运维辅助
│   ├── udp_imu_receiver.py     UDP 调试接收
│   └── imu_web/                云端记录台
│       ├── server.py           Python 标准库 + SQLite 的 HTTP 服务
│       ├── index.html / app.js / records.js / style.css / calibration_view.js
│       ├── calibration_coefficients.json   标定系数
│       ├── deploy.sh           scp 部署 + 自动备份 + 失败回滚
│       ├── glove-imu.service   systemd 单元
│       ├── capture_calibration.py / fit_calibration.py   六面标定采集与拟合
│       ├── pack_dataset.py     数据集打包
│       ├── test_server.py      服务端单元测试
│       └── test_frame_stream.py
├── CMakeLists.txt
├── partitions.csv
├── dependencies.lock
├── sdkconfig.defaults.left / .right
├── sdkconfig.left / .right
└── .gitignore / .gitattributes
```

---

## 编译

需要 **ESP-IDF 6.1**（ESP32-S3）。

```sh
idf.py -B build_left  -D SDKCONFIG=sdkconfig.left  -D SDKCONFIG_DEFAULTS=sdkconfig.defaults.left  build
idf.py -B build_right -D SDKCONFIG=sdkconfig.right -D SDKCONFIG_DEFAULTS=sdkconfig.defaults.right build
```

或直接用仓库自带的脚本（Windows）：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\idf.ps1 -Action Build -Hand left
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\idf.ps1 -Action Build -Hand right
```

烧录：

```sh
idf.py -B build_left  -p COMx flash
idf.py -B build_right -p COMy flash
```

**左右手必须分别用各自的 sdkconfig 构建**，不能混用。
`main/glove_identity.h` 通过 sdkconfig 里的 `CONFIG_GLOVE_HAND_LEFT/RIGHT` 决定手别。

---

## 首次使用前必须补两个私密头文件

这两个文件**不在本包内**（被 .gitignore 忽略），需要自己创建：

```sh
cp main/wifi_credentials.example.h main/wifi_credentials.h
cp main/cloud_config.example.h     main/cloud_config.h
```

- `wifi_credentials.h`：热点 SSID / 密码（建议手机开 **2.4 GHz** 热点）
- `cloud_config.h`：上传地址与令牌，令牌必须与服务器 `IMU_TOKEN` 一致

---

## 串口

- 波特率 **921600**，8N1，控制台走 UART0（GPIO43 TX / GPIO44 RX）
- 上电最初几字节是 ROM 日志（115200），显示乱码属正常
- 每帧一行原始数据，约 300–500 字节，50 Hz 约 20 KB/s：

```
[DATA]<timestamp_ms>,<left|right>,<IMU0 六轴>,...,<IMU9 六轴>,<IMU10 九轴>
```

- 串口控制指令（控制台内部使用）：

```
@REC <id> STATUS
@REC <id> CHECK
@REC <id> START
@REC <id> STOP
```

- **打开串口时必须 `dtr=False, rts=False`**，否则会复位 ESP32 并丢失当前 session

关闭逐帧输出：把 `main/board_config.h` 的 `IMU_SERIAL_DATA_ENABLED` 设为 0。

---

## 云端记录台

```sh
# 环境变量
export IMU_TOKEN=<设备上传令牌>
export IMU_ANNOTATION_TOKEN=<网页标注令牌>
export IMU_DB=/var/lib/glove-imu/imu.sqlite3

python tools/imu_web/server.py      # 默认监听 127.0.0.1:8765
```

生产部署参考 `tools/imu_web/glove-imu.service`，前置 Nginx 反代到独立路径 `/imu/`。

主要接口：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/ingest` | 设备批量上传（Bearer 令牌） |
| POST | `/api/heartbeat` | 设备心跳 |
| GET | `/api/status` | 设备与最近会话 |
| GET | `/api/records` | 录制列表（**不按 session 过滤**） |
| GET | `/api/frames` | 抽样帧（最多 500 条，无可靠分页） |
| GET | `/api/integrity?session=` | 单会话完整性（帧数、配对、缺口） |
| GET | `/api/export.csv` | 导出，`format=dataset\|archive`、`counts=raw\|calibrated` |
| GET/POST | `/api/annotations` | 标注读写 |

CSV 默认 **77 列**：`worker, v, label, active_hand, sample_id, timestamp/ms, frame_id, hand_type` + 69 个原始寄存器值。

测试：

```sh
python -m unittest discover -s tools/imu_web -p test_server.py
```

---

## 数据流与同步要点

- 右手每 250 ms 发起四时间点双向对时，估算到左手时钟的偏移；至少 4 次有效观测后启用
- 只有「同一录制 + 同一计划时隙 + 同步有效 + 扫描中点差 ≤5 ms」的左右帧才配对
- 配对时间由**计划时隙**决定，不能把相同 CSV 时间戳理解为硬件同时采样
- 11 路 IMU 是**顺序读取**，不是并行采样
- 上传是「攒批 → POST → 等服务端确认行数 → 释放批次」，失败整批重发，服务端按 `设备+录制+序号` 去重

---

## 本包**未包含**

| 内容 | 原因 |
|---|---|
| `build_left/` `build_right/` | 编译产物，可自行生成 |
| `.cache/` | ESP-IDF 工具链缓存，86 MB |
| `main/cloud_config.h`、`main/wifi_credentials.h` | 含凭据 |
| `diagnostics/` | 六面标定原始记录（约 10 MB 数据） |
| 任何 `.csv` 采集数据 | 患者/受试数据 |
| `.git/` | 提交历史 |
| 验证报告 PDF | 内部文档 |

---

## 已知限制

- Wi-Fi 事件 `reason=201` 表示未找到匹配接入点，需确认热点为 2.4 GHz 且已开启
- NVS 里保存过的 Wi-Fi 配置优先于代码默认值，重烧不清除；改热点请走配网页面
- 上传吞吐受服务端解析限制，实测约 15 对/秒；50 Hz 连续采集会积压，停采后需等待排空
- 标定系数为手工六面初步结果，不是逐轴实测；`status` 字段会标出 `nominal_unmeasured` 或 `scaled_legacy_fit_requires_validation`
