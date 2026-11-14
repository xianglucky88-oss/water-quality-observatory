# 水环境监测与污染事件数据平台

采集水质自动站、实验室检测和污染事件数据，统一水质指标和单位，识别缺测、超标和多来源冲突。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> deduplication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录和来源批次模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验和稳定哈希。
- `identity.py`：领域主键和去重键。
- `quality.py`：领域质量规则和拒绝原因。
- `storage.py`：SQLite 结构化表、幂等写入、逐条目处理账本、原始清单、拒绝记录和批次结算。
- `pipeline.py`：只负责编排标准化、质量校验和存储；按处理账本重放未完成条目。
- `reconciliation.py`：批次状态分类、证据核对和显式恢复。
- `faults.py`：测试和演练用的一次性故障注入点。
- `application.py`：组装依赖、保存原始响应并按证据摘要自动接续未完成批次。
- `cli.py`：fixture/HTTP 两种运行入口，以及核对、恢复入口。

## 批次结算模型

一次摄取会先落地原始响应，再在同一个事务里登记原始清单摘要（`raw_manifests`）、批次行（`batches`）、
输入快照（`batch_inputs`，保存本次提交的记录与输入摘要）。之后每个条目与它的处理结论原子写入：

- 通过标准化和质量规则的条目：结构化行 `records` + 处理账本 `batch_items(outcome='accepted')`；
- 被拒绝的条目：`rejected_records` + `batch_items(outcome='rejected')`，带同一 `item_index` 和拒绝原因。

所有条目都有持久结论后，`settle_batch` 在一个事务里核对
「账本条目数 = 登记条目数」「拒绝行 index/原因 = 账本拒绝项」「每个 accepted 条目都有结构化行」，
通过后才写入 `status='settled'` 以及 `processed/accepted/rejected/changed` 摘要。任一项对不上就拒绝结算，
批次停留在 `finalizing`，不会出现“异常却记为成功”的批次。

中断窗口与恢复语义：

| 中断位置 | 持久事实 | 状态 |
| --- | --- | --- |
| 原始清单之前 | 至多留下原始文件，没有批次行 | 孤儿证据（`--list-orphans` 可查） |
| 证据登记之后、首条写入之前 | 清单 + 快照 + 批次行，账本为空 | `never_started` |
| 首条写入之后 | 部分 `records` + 账本 | `retryable` |
| 拒绝记录之后 | 全部 `records`/拒绝 + 账本 | `ready_to_settle`（若拒绝不是最后一条则是 `retryable`） |
| 最终结算之前 | 账本完整，`status='finalizing'` | `ready_to_settle` |
| 结算之后（未返回结果） | 已结算摘要 + 完整账本 | `completed` |

恢复操作只按账本重放缺失条目，用 `item_index` 去重，从不删除任何批次、结构化行或拒绝行：

- 同一份原始材料再次提交时，若摘要相同且源相同，会自动接续唯一未完成批次（幂等）；
- 存在多个未完成批次时拒绝猜测，要求 `--recover-batch N` 显式恢复（`AmbiguousBatchError`）；
- 另一个进程仍持有租约时报告 `in_progress` 并拒绝恢复，`--force` 只用于确认原进程已死亡；
- 原始文件、清单、输入快照或摘要相互矛盾时报告 `unrecoverable`，不自动重试，保留已有记录供人工处理；
- 操作员已标记 `unrecoverable` 的批次不会被再次自动接续。

恢复程序本身再次中断时，账本仍是权威事实，重启后再次恢复即可收敛到同一结果。`attempt` 与
`recovery_count` 记录尝试次数。

## 运行

```bash
PYTHONPATH=src python3 -m water_quality.cli \
  --fixture fixtures/sample.json \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

HTTP 模式：

```bash
PYTHONPATH=src python3 -m water_quality.cli \
  --url https://example.invalid/api/records \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

接口需要返回 JSON 数组，或包含 `records`、`data`、`items`、`results` 的对象；其他嵌套结构使用 `--records-key`。认证变量 `DATA_SOURCE_BEARER_TOKEN` 和 `DATA_SOURCE_API_KEY` 只作为环境变量名称示例，不写入真实值。

### 核对与恢复

```bash
# 查看单个批次：状态、计数、证据摘要、拒绝原因、最后错误、租约
PYTHONPATH=src python3 -m water_quality.cli --verify-batch 1 \
  --database /tmp/water-quality-observatory.sqlite3 --raw-dir /tmp/water-quality-observatory-raw

# 查看所有批次或落地后未登记批次的原始证据
PYTHONPATH=src python3 -m water_quality.cli --list-batches ... --raw-dir ...
PYTHONPATH=src python3 -m water_quality.cli --list-orphans ... --raw-dir ...

# 显式恢复（按批次号或按已落地证据摘要）
PYTHONPATH=src python3 -m water_quality.cli --recover-batch 1 ... --raw-dir ...
PYTHONPATH=src python3 -m water_quality.cli --recover-digest <sha256> ... --raw-dir ...
```

状态取值：`never_started`、`in_progress`、`retryable`、`ready_to_settle`、`completed`、`unrecoverable`。
失败结果以 `{"error": ..., "report": ...}` 形式输出并以退出码 1 结束。

### 故障演练

`--fail-at` 可在下列持久化检查点注入一次性中断，`raise`（默认）抛出异常，`crash` 模拟进程硬中断
（租约保留，需要 `--force` 或等待租约过期）：

```bash
--fail-at after_evidence         # 证据登记之后
--fail-at after_first_write      # 首条结构化写入之后
--fail-at after_rejection        # 拒绝记录之后
--fail-at before_finalize        # 全部条目落盘、最终结算之前
--fail-at after_finalize:crash   # 结算之后、返回结果之前
```

`--legacy-batch` 强制把每次提交当作新批次（用于验证重复提交的幂等统计）。`--lease-seconds` 控制
租约时长，便于在测试中快速判定失联进程。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
- SQLite 3.x（本地结构化存储）
- HTTP(S) JSON 数据源（生产接入时按具体来源协议配置）

## 验证

```bash
PYTHONPATH=src python3 -m pytest --import-mode=importlib tests
PYTHONPATH=src python3 -m compileall -q src
```
