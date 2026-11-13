# 水环境监测与污染事件数据平台

采集水质自动站、实验室检测和污染事件数据，统一水质指标和单位，识别缺测、超标和多来源冲突，并对同一水样指标的自动站初报与实验室复检进行**可追溯的修订裁决**。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> evidence ledger -> revision adjudication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录、内容指纹和撤回指令模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验、稳定哈希与撤回信封解析。
- `identity.py`：领域主键、去重键和裁决主体键 `站点|采样标识|指标`。
- `quality.py`：领域质量规则和拒绝原因。
- `adjudication.py`：可配置的来源优先级与质量确认条件（支持 JSON 配置文件）。
- `decisions.py`：**纯函数、确定性**的修订裁决状态机（无 I/O、无时钟、无全局状态）。
- `storage.py`：SQLite 结构化表、只追加证据台账、幂等写入、批次、原始清单、拒绝记录、状态表与转移历史。
- `pipeline.py`：编排标准化、质量校验、证据落账与逐条原子裁决。
- `reporting.py`：面向运营人员的只读查询模型（结论、替代原因、关联证据，不含任何认证信息）。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：`ingest` / `summary` / `subjects` / `subject` 子命令，并兼容旧版扁平调用。

## 修订裁决模型

### 主体与证据

- **裁决主体**：`subject_key = 站点|采样标识|指标`。同一水样指标的自动站初报、实验室复检、撤回与重提都归并到同一主体。
- **只追加证据**：每条输入落为一行 `report_evidence`，保留来源、原始哈希（`report_hash`）、测量内容指纹（`content_hash`）、来源批次 `batch_id`、批内序号 `batch_sequence` 和全局到达序 `arrival_order`。后来记录只改变派生状态，从不删除或覆盖既有证据。
- **撤回**：`{"action": "withdraw", ...}` 信封解析为不可变的 `withdrawal_directives`；只能撤回本来源、且已存在的观测。
- **原始证据**：每批原始响应按内容哈希落盘到 raw landing，并在 `raw_manifests` 与批次中留痕。

### 可查询状态

报告级：`pending` 待裁决 / `confirmed` 已确认 / `superseded` 已替代 / `disputed` 争议 / `withdrawn` 撤回。
主体级：`pending` / `confirmed` / `disputed`。只有主体 `confirmed` 才进入最终监管视图（`--final-only`）；未解决争议主体被排除，不会被误作最终监管数据。

裁决规则（见 `decisions.py`）：

1. 撤回的观测进入 `withdrawn`，证据仍可追查。
2. 质量标记不在 `eligible_quality_flags` 内的报告保持 `pending`，不能发布结论。
3. 合格报告按可配置来源优先级取最高档；同档同内容取到达最早者为 `confirmed`，其余同内容证据为 `superseded`（重复/佐证），低优先级不同内容为 `superseded`（被更高优先级推翻）。
4. 同档优先级出现不同内容 → 相关报告 `disputed`，主体 `disputed`，必须人工处理。

### 原子写入与重放

- 每条输入在**一个 `BEGIN IMMEDIATE` 事务**内完成：证据插入（或幂等跳过）→ 单主体重算 → 状态表更新 → 转移历史追加。中途异常整体回滚，不会留下半条逻辑版本。
- 证据与撤回指令以内容哈希为主键，重复传送是无操作（no-op），不产生第二条逻辑版本。
- 裁决是纯函数：相同证据集 + 相同配置必得相同状态。重放相同材料到独立数据库产生逐行一致的状态与原因。
- 迟到复检只重算其所属主体，其他水样指标不受影响。
- 无法解析的记录在进入证据层之前被拒绝，写入 `rejected_records`；同批其他记录正常提交。

## 运行

摄取（支持 `--config` 指定来源优先级与质量条件；缺省使用内置默认配置）：

```bash
PYTHONPATH=src python3 -m water_quality.cli ingest \
  --fixture fixtures/adjudication/01_initial.json \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw \
  --config fixtures/adjudication.config.json
```

旧版扁平调用仍然兼容（等价于 `ingest` 子命令）：

```bash
PYTHONPATH=src python3 -m water_quality.cli \
  --fixture fixtures/sample.json \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

HTTP 模式：

```bash
PYTHONPATH=src python3 -m water_quality.cli ingest \
  --url https://example.invalid/api/records \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

接口需要返回 JSON 数组，或包含 `records`、`data`、`items`、`results` 的对象；其他嵌套结构使用 `--records-key`。认证变量 `DATA_SOURCE_BEARER_TOKEN` 和 `DATA_SOURCE_API_KEY` 只作为环境变量名称示例，不写入真实值，也不会出现在任何查询输出中。

## 运营查询（输出不含认证信息）

```bash
# 状态统计与可发布监管数据量
PYTHONPATH=src python3 -m water_quality.cli summary --database DB

# 列出主体当前结论；可按状态过滤或仅看最终监管数据
PYTHONPATH=src python3 -m water_quality.cli subjects --database DB --status disputed
PYTHONPATH=src python3 -m water_quality.cli subjects --database DB --final-only

# 单个水样指标的当前结论、被替代原因、全部关联证据、撤回记录与状态转移历史
PYTHONPATH=src python3 -m water_quality.cli subject --database DB --subject-key "W-11|S-101|AMMONIA_N"
```

## 裁决配置示例（`fixtures/adjudication.config.json`）

```json
{
  "source_priority": { "lab-a": 100, "lab-b": 100, "lab-c": 90, "station-network": 10 },
  "fallback_priority": 0,
  "eligible_quality_flags": ["good", "suspect"]
}
```

## 受控场景夹具（`fixtures/adjudication/`）

按顺序摄取可复现：初报后确认、复检推翻初报、相同内容重复传送、两份高优先级结果冲突、撤回后重新提交，以及处理过程中的异常/坏记录恢复。

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
