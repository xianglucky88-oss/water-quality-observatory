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
- `storage.py`：SQLite 结构化表、幂等写入、批次、原始清单和拒绝记录。
- `pipeline.py`：只负责编排标准化、质量校验和存储。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：fixture/HTTP 两种运行入口。
- `identity.py, metrics.py, quality.py`：领域事件、生命周期和后续扩展规则。
- `assessment.py`：窗口级证据完整度评估（只读），区分可用观测、质量隔离、重复冲突、证据空洞和迟到到达，并输出可回溯的结构化结论与命令行摘要。

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

## 窗口级证据完整度评估

评估只回答“证据是否足够支持污染判断”，**从不判定污染是否发生**。它以只读 SQLite 连接运行，不写入数据库，对既有摄取结果无副作用。

五类证据状态严格区分，缺失、无效与零值不会合并：

| 类别 | 含义 | 结论代码/计数 |
| --- | --- | --- |
| 可用观测 | 窗口内通过质量规则的记录（零值是合法数值观测） | `usable_observations` / `zero_value_observations` |
| 质量隔离 | 被标准化或领域规则拒绝的原始记录，附拒绝原因与原始摘要 | `rejected_records`（无法定位窗口的计入 `unattributed_rejections`） |
| 重复冲突 | 同一领域键被来源重复提交但内容不同（raw hash 不同），全部版本保留、未经裁决不采信 | `CONFLICTING_VERSIONS` |
| 证据空洞 | 窗口边缘或观测之间超过该指标最大可接受间隔；无观测时为整窗空洞 | `GAP_EXCEEDS_MAX_INTERVAL` |
| 迟到到达 | 窗口内实际采样、但在到达截止时刻之后才首次入库；仍属可用观测，仅标注 `late_arrival` | `late_arrivals` |

缺测（`MISSING_IN_WINDOW`）与“仅有隔离记录”（`ONLY_REJECTED_EVIDENCE`）是两种不同结论。窗口采用**起点包含、终点不包含** `[start, end)` 的半开区间。

```bash
PYTHONPATH=src python3 -m water_quality.cli assess \
  --database /tmp/water-quality-observatory.sqlite3 \
  --station W-01 \
  --start 2026-11-01T10:00:00Z --end 2026-11-01T11:00:00Z \
  --metrics ph,lead \
  --arrival-deadline 2026-11-01T12:00:00Z \
  --policy-file policy.json \
  --format summary
```

`--format json` 输出结构化文档：每个指标给出 `status`（`supports_pollution_assessment` / `insufficient_evidence`）、原因、策略、计数、观测/冲突/空洞/隔离明细，以及批次和原始材料 digest 的溯源注册表；每条证据都带 `record_key`、`batch_id` 和 `raw_hash`，可回溯到原始材料摘要。`assessment_fingerprint` 是对实质结论的稳定 SHA-256：多次导入、不同输入顺序、重复原始材料都不会使指纹漂移（批次号和入库时钟不参与哈希）。

策略文件按指标覆盖默认值，只允许非负整数 `min_observations` 和 `max_gap_seconds`：

```json
{
  "ph": {"min_observations": 5, "max_gap_seconds": 300},
  "lead": {"min_observations": 2, "max_gap_seconds": 7200}
}
```

程序入口同样可直接调用 `water_quality.assessment.assess_window(...)`。

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
