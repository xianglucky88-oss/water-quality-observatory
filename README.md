# 水环境监测与污染事件数据平台

采集水质自动站、实验室检测和污染事件数据，统一水质指标和单位，识别缺测、超标、低于检出限和多来源冲突。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> rule catalog (versioned) -> identity resolution -> quality control -> deduplication -> quarantine / batch state -> structured storage -> replay / controlled reprocessing`

## 报告值与比较语义

每条正式观测同时保留两层信息，合规判断可以追溯到实验室原始表达：

| 字段 | 含义 |
| --- | --- |
| `reported_value_text` / `reported_value` | 原始报告文本（如 `"< 0.01"`、`"ND"`）与解析出的报告数值 |
| `qualifier` | 规范限定符：`=`（精确值）、`<`（低于报告值）、`ND`（未检出） |
| `detection_limit` | 报告检出限；缺失、非正或超出指标合理上限的结果进入隔离 |
| `reported_unit` | 原始单位（大小写/空白/Unicode 变体已归一，但保留来源拼写） |
| `value` / `unit` | 仅按目录中**显式声明的因子**换算后的可比值与规范单位 |
| `comparison_semantics` | `exact` 或 `left_censored`；截尾结果的可比值取检出限，**绝不写零** |
| `conversion_factor` | 本次使用的换算因子，未换算为 `1.0` |
| `rule_version` / `rule_fingerprint` | 生成该记录的规则版本与指标规则指纹 |

无法确定的单位、范围不合理的检出限、限定符与数值互相矛盾、无法解析的表达等，均带 `reason_code` 写入 `quarantine_records`（同时镜像到兼容旧读取方的 `rejected_records`），不污染 `records` 正式观测。

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录，覆盖报告表达、检出限、原始单位与比较语义。
- `rules.py`：**版本化指标/单位规则目录**；换算只允许显式因子，单位别名与限定符词表集中管理，每条指标规则带稳定指纹。
- `normalize.py`：文本/数值/限定符解析、检出限一致性校验、显式单位换算和稳定哈希。
- `identity.py`：领域主键和去重键（保持历史口径不变）。
- `quality.py`：由规则目录驱动的领域质量规则。
- `metrics.py`：单位词表的兼容门面，内部委托规则目录。
- `storage.py`：SQLite 表、幂等写入、批次、原始清单、隔离记录、规则版本登记、重处理运行；自动迁移旧库。
- `pipeline.py`：标准化、质量校验、持久化编排；支持 `ingest` 与只读 `preview` 两种模式。
- `reprocess.py`：规则升级后的影响评估与受控重处理，严格区分预览与正式写入。
- `application.py`：组装依赖、保存原始响应、登记规则版本和管理资源生命周期。
- `cli.py`：fixture/HTTP、预览与重处理运行入口。

## 运行

正式导入：

```bash
PYTHONPATH=src python3 -m water_quality.cli \
  --fixture fixtures/lab_samples.json \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

导入预览（原始响应仍落盘，但不写任何正式观测或隔离行）：

```bash
PYTHONPATH=src python3 -m water_quality.cli --preview \
  --fixture fixtures/lab_samples.json \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

规则升级后的受控重处理（先预览、确认后再加 `--apply`；代码内通过传入新的 `RuleCatalog` 触发）：

```bash
# 预览：识别受影响记录，不写入
PYTHONPATH=src python3 -m water_quality.cli --reprocess \
  --database /tmp/water-quality-observatory.sqlite3
# 正式写入：仅更新派生口径确实变化的记录
PYTHONPATH=src python3 -m water_quality.cli --reprocess --apply \
  --database /tmp/water-quality-observatory.sqlite3
```

重处理规则：

- 仅当可比值、单位、限定符、检出限或换算因子等**派生口径真正变化**才计为受影响并写入；仅规则版本号变化而结果一致的记录标记 `unchanged`，不会被重算或改写。
- 新规则下不再成立的正式观测被移出 `records` 并写入带原因的隔离审计行；新规则下可以接受的历史隔离行以 `release_from_quarantine` 释放为正式观测。
- 每次运行记录在 `reprocess_runs` / `reprocess_run_items`，预览与应用分别留痕。

HTTP 模式：

```bash
PYTHONPATH=src python3 -m water_quality.cli \
  --url https://example.invalid/api/records \
  --database /tmp/water-quality-observatory.sqlite3 \
  --raw-dir /tmp/water-quality-observatory-raw
```

接口需要返回 JSON 数组，或包含 `records`、`data`、`items`、`results` 的对象；其他嵌套结构使用 `--records-key`。认证变量 `DATA_SOURCE_BEARER_TOKEN` 和 `DATA_SOURCE_API_KEY` 只作为环境变量名称示例，不写入真实值。

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

测试覆盖：混用单位与显式因子换算、大小写/空白/Unicode 变体、低于检出限（`<` 与 `ND`）、检出限缺失与不合理、同一水样的两种报告方式、限定符矛盾、规则升级（预览/应用/因子修正/隔离释放）、重复导入幂等、旧库迁移、查询追溯原始表达，以及基线正常记录不被重算。
