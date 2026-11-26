# 服务端改动说明（逐帧 WSS 直传 + 服务端配对 + 77 列 CSV）

日期：2026-11-18。对应 `prompt.txt` 中「左右手 WebSocket 逐帧直传，服务器配对」的方案。
**固件代码未改动**（`main/`、`sdkconfig.*`、`build_*` 全部保持原样），本次只改服务端。

## 1. 改了什么

| 文件 | 变动 |
| --- | --- |
| `tools/imu_web/server.py` | 新增逐帧 WSS 接收、服务端配对、推理队列、配对统计接口；导出改为按时隙编号 |
| `tools/imu_web/pack_dataset.py` | 新增：服务端打包 77 列 CSV 的 CLI |
| `tools/imu_web/test_frame_stream.py` | 新增：25 个逐帧/配对/断线/重启测试 |
| `tools/imu_web/test_pack_dataset.py` | 新增：3 个打包测试 |
| `tools/imu_web/reference_header.csv` | 新增：样例 CSV 的 77 列表头（测试基准） |
| `tools/imu_web/test_server.py` | 仅把依赖 `E:\QQ_Download\data_raw.csv` 的用例改为读仓库内表头 |
| `tools/imu_web/README.md` | 重写为当前协议的说明文档 |
| `tools/imu_web/records.js`、`index.html` | 采集记录表新增「配对」列，显示配对率与缺失/超容差明细 |

数据库为**向后兼容迁移**：只在缺列时 `ALTER TABLE ADD COLUMN`，新增 `pairs`、`inference_runs` 两张表和若干索引，历史数据与旧接口不受影响。

## 2. 协议

设备 → 服务器（每帧一条，WSS `/api/stream`，`Authorization: Bearer <IMU_TOKEN>`）：

```json
{"type":"frame","device":"A0:B1:C2:D3:E4:F5","hand":"left",
 "session":"651234567890123456","sequence":1043,
 "timestamp":60000,"source_us":60040000,"aligned_us":60041000,
 "scheduled_us":60040000,"clock_offset_us":-148000000,
 "scan_us":3100,"sync_rtt_us":900,"sync_age_ms":120,"sync_valid":true,
 "valid_mask":2047,"mag_mask":1024,
 "channels":["…11 组…"]}
```

服务器 → 设备（**事务提交后**才发）：

```json
{"type":"frame_ack","device":"A0:B1:C2:D3:E4:F5","hand":"left",
 "session":"651234567890123456","sequence":1043,
 "saved":true,"inserted":true,"pair":"paired","pair_id":88}
```

错误时返回 `{"type":"error","error":"…"}`，不含 `saved`，设备必须保留并重发。
重复帧返回 `saved:true, inserted:false, duplicate:true`。

字段映射到旧表的对应关系：`source_us → source_timestamp_ms`、`aligned_us → aligned_us`、`scheduled_us → scheduled_us`、`sync_valid → sync_valid`，其余原样入库，另存 `received_unix_ms` 仅作诊断。

## 3. 对时、配对、确认、重传、推理

1. **对时**由设备完成（保留 ESP-NOW 双向对时）。服务器直接使用 `aligned_us`，不重新对时，也不把到达时间当采样时间。
2. **配对**：按 `session` + `scheduled_us` 找时隙，再校验双方 `sync_valid` 且 `|aligned_us 差| ≤ 5 ms`。不看 sequence、不看到达顺序、不跨 session。
3. **确认**：`BEGIN IMMEDIATE` → 插入 → 配对 → 提交 → 才回 ack。提交失败返回 error，不确认。
4. **重传**：设备不必逐帧等确认；未确认帧在重连后补发，`(device, session, sequence)` 唯一键保证幂等。
5. **推理**：配对成功只入有界队列，模型未接入时返回 `model_not_ready`，结果写 `inference_runs`，不阻塞收帧。

## 4. CSV 格式（对照 `cx_20261113_083049_calibrated_left-web-20261111-v1.csv`）

77 列，前 8 列语义完全一致，并经测试逐列比对样例表头。

- `frame_id`：计划时隙序号 = `(scheduled_us − 该 session 首个 scheduled_us) / 20 ms`。样例中 `frame_id 143 → 163` 的空档正好对应 400 ms（20 个空时隙），导出同样保留空号。
- `timestamp/ms`：时隙在共同时间轴上的毫秒值；样例 `60000 / 60020 / 104100` 与 `frame_id 0/1/253` 吻合。
- 缺失通道、`mag_mask` 关闭的磁力计列留空，不填 0。
- 左右手同一配对相邻成行，先 left 后 right。

## 5. 两只手都要重烧固件吗

是。新协议要求左右手都改用 WSS 逐帧直传，旧固件（左手 HTTP 批量 + 右手 ESP-NOW 转发）仍可继续工作 —— 两条路径共存，`POST /api/ingest` 未改行为。队友推送新固件后整批切换即可。

## 6. 测试

```
python -m unittest discover -s tools/imu_web -p 'test_*.py'
Ran 47 tests … OK
```

覆盖：逐帧确认在提交之后、重复/断线重发幂等、坏帧不入库、握手鉴权、同序/乱序/晚到/超容差/同步无效/单手超时/同手重复时隙、跨 session 不混配、100 帧每秒、写库失败不确认、重启后配对重建、77 列布局与空号、标定列、归档导出的配对诊断列、旧接口全量回归。

## 7. 未完成 / 部署步骤（需你在服务器上执行）

1. 模型尚未接入：`infer()` 仍返回 `model_not_ready`。
2. 公网 `101` 需要 Nginx 为 `/imu/` 增加 WebSocket 代理，**尚未验证**：

```nginx
location /imu/api/stream {
    proxy_pass http://127.0.0.1:8765/api/stream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Authorization $http_authorization;
    proxy_read_timeout 3600s;
}
```

3. 部署（部署脚本已存在，会先备份再回滚检查）：

```sh
bash tools/imu_web/deploy.sh      # 会先备份，失败自动回滚，并检查公网 101
systemctl restart glove-imu
curl -s http://127.0.0.1:8765/api/pairing/stats
```

4. `deploy.sh` 已加入 `pack_dataset.py` 与公网 101 握手检查（`IMU_TOKEN` 需导出后才验证鉴权）。
5. 建议在 `/etc/glove-imu.env` 增设备份、容差等变量（见 README）。
