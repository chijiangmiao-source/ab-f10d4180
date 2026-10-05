# 近地轨道辐射监测站 · LEO Radiation Monitor

两台独立探头（probe-a / probe-b）提交**乱序且可能重传**的剂量读数。系统按固定的
**五分钟告警窗口**读取结果：以两探头各自“已观察到的最大时刻”之较小值减去允许迟到量
推进水位，只有 `window_end <= watermark` 的窗口才能在**同一个持久化事务**中封存
总剂量、峰值与告警等级。封存即终态：迟到包被拒绝，已发布的风险等级永不被改写。

- Python 3.13 · FastAPI · SQLite（WAL，`synchronous=FULL`，提交即 fsync）
- 宿主机/端口可通过环境变量配置，带 `/health` 健康响应
- `docker compose` 一键启动；一次性 `verify` 验收容器覆盖重传、乱序封存与恢复

## 快速开始

```bash
docker compose up --build -d          # 启动应用（健康检查通过后就绪）
curl http://localhost:8080/health

docker compose build verify           # 一次性验收（退出码即结论）
docker compose run --rm verify
```

本地无 Docker 时：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DB_PATH=./radiation.db APP_PORT=8080 python -m app.main
WORKDIR=$PWD BASE_URL=http://127.0.0.1:8080 DB_PATH=./verify.db bash scripts/verify.sh
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查 |
| POST | `/readings` | 提交一条读数（见下） |
| GET  | `/windows` | 列出所有**已封存**窗口（唯一、不可回写） |
| GET  | `/windows/{window_start}` | 按窗口起始时刻（epoch 秒，5 分钟对齐）读取封存结果 |
| GET  | `/windows/by-time?at=<秒>` | 用窗口内任意时刻读取结果 |
| GET  | `/progress` | 两探头当前进度与原始/有效水位（任一探头未出现时水位为 `null`） |
| GET  | `/violations` | 每个探头的**首个**序号违例及当时对侧进度 |

### 提交读数

```json
POST /readings
{
  "probe": "probe-a",
  "seq": 7,
  "event_id": "evt-0001",
  "observed_at": 1791190010.0,
  "dose": 42.5
}
```

响应（201）含 `status`（`new` / `replay`）、幂等回放 `token`、所属窗口，以及本次提交
新封存的窗口列表 `sealed_windows`。

## 关键语义

- **重传回放**：相同 `event_id` 且内容（探头/序号/时刻/剂量的哈希）一致 →
  返回原始 `token`，不产生重复读数；即使其窗口已封存也照常回放。
- **内容冲突**：相同 `event_id` 但内容不同 → `409 integrity_conflict`，拒绝并明确冲突。
- **乱序并入**：封存前到达的补缺包（如 seq 3 先到、seq 2 后到）被准确并入汇总。
- **封存不可回写**：封存后才落窗的**新事件** → `409 window_sealed`，汇总/峰值/等级不变。
- **水位**：`watermark = min(每探头最大 observed_at) - ALLOWED_LATENESS_SECONDS`；
  任一探头尚未出现则无水位，绝不封存。
- **序号**：跳跃只审计（`gap`，记录首个违例与对侧 `last_seq/observed_at`），不影响水位；
  `(探头, 序号)` 被不同事件重复占用 → `422 sequence_violation(regression)`。
- **唯一封存**：`windows.window_start` 为主键，封存在单条 `BEGIN IMMEDIATE` 事务内完成；
  并发提交与进程重启都只留下一条封存记录。
- **告警等级**（阈值 `ALARM_THRESHOLD`，封存时一次性计算，之后不再重算）：
  峰值 ≥ 2×阈值或总量 ≥ 3×阈值 → `critical`；峰值 ≥ 阈值或总量 ≥ 阈值 → `warning`；
  否则 `ok`。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `APP_HOST` | `0.0.0.0` | 监听地址 |
| `APP_PORT` | `8080` | 监听端口 |
| `DB_PATH` | `/data/radiation.db` | SQLite 路径（命名卷持久化） |
| `ALLOWED_LATENESS_SECONDS` | `30` | 水位允许迟到量 |
| `ALARM_THRESHOLD` | `100` | 告警阈值 |
| `PROBES` | `probe-a,probe-b` | 两个探头标识（恰好两个） |

## 验收容器 `verify`（退出码结束）

`scripts/verify.sh` 顺序执行，任一步失败即以非零退出：

1. 字节码编译 / `pip check` / 应用导入与路由检查（构建检查）
2. `pytest`：重放回放入冲突、乱序封存、迟到拒绝、水位、首个违例、**重启恢复**、
   并发封存唯一记录
3. HTTP 冒烟（`scripts/smoke.py`）：对运行中的应用覆盖重传、乱序封存与恢复后不可回写；
   可重复执行（序号按 `/progress` 续接、自动选用干净的未来窗口）

## 测试

```bash
pip install -r requirements.txt
python -m pytest -q
```
