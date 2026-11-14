# 湖岸小驻 · Lakewatch

一个**纯本地、完全离线**的数据湖表维护工作台。它使用本地文件系统模拟对象存储路径，
使用轻量的 Iceberg 风格清单与快照语义管理 Parquet 表，解决长期运行后常见的小文件膨胀、
分区倾斜、孤儿/缺失文件、过期快照和清单漂移问题。

## 定位与边界

**是**

- 「扫描 → 诊断 → 计划 → 执行 → 复核」闭环的表维护工具。
- 面向 `table/分区列=值/part.parquet` 布局的 Parquet 表工作台。
- YAML 策略驱动的小文件合并、过期快照清理、清单修复与前后对比报告。
- 所有动作写入本地 SQLite 台账，计划可 dry-run、可哈希校验、可幂等重放。

**不是**

- 不是 Spark/Flink 作业、查询引擎、Catalog 服务或调度器。
- 不是 S3/OSS/HDFS 客户端；运行期不发起网络请求。
- 不做业务 ETL、不读取业务数据做统计查询、不管理凭据。
- 不兼容真实 Iceberg/Delta/Hudi 元数据；目录与清单只做语义对齐。

## 快速开始

需要 Python 3.10+、Flask、PyYAML、pyarrow、pytest。仓库内已带可运行的 fixture，
无需下载任何数据。

```bash
# 启动本地只读工作台（默认只监听 127.0.0.1:7311）
python -m lakewatch serve --config configs/default.yaml

# 另一个终端：本地健康检查
curl http://127.0.0.1:7311/healthz

# 全量测试
pytest -q
```

CLI 入口（均不访问网络）：

```bash
python -m lakewatch scan   --config configs/default.yaml
python -m lakewatch scan   --config configs/default.yaml --json --out reports/scan.json
python -m lakewatch plan   --config configs/default.yaml --out reports/plan.json
python -m lakewatch apply  --config configs/default.yaml --plan reports/plan.json
python -m lakewatch report --config configs/default.yaml --json
```

`apply --plan` 执行保存过的计划；同一个计划文件第二次执行会由 SQLite 台账识别为
`already_completed` 并跳过全部动作。`apply --dry-run` 只打印计划，不写计划、报告或台账。
`scan --strict` 在存在 `error` 级诊断时返回 1；正常扫描即使有诊断也返回 0。

## 目录结构

```text
lakewatch/                 Python 包
  catalog/                 布局解析、当前清单、快照清单
  scanners/                小文件/倾斜/一致性/过期快照扫描
  planner/                 策略校验与确定性计划
  executor/                合并、清理、修复与台账编排
  ledger/                  SQLite 台账与 schema 迁移
  reports/                 JSON / Markdown / HTML 片段报告
  web/                     本地 Flask 应用、模板与静态资源
  fixtures/lake/           离线样例湖（Parquet + 自定义清单）
  fixtures/configs/        独立策略样例
configs/                   default.yaml、aggressive.yaml
scripts/make_fixtures.py   从零重建 fixture
tests/                     pytest 测试
reports/                   运行时生成的计划与报告（git 忽略）
pyproject.toml             包元数据与 pytest 配置
```

## Fixture 与重建

```bash
python scripts/make_fixtures.py
python scripts/make_fixtures.py --out /path/to/fixture-lake
```

脚本会**删除并重建**目标目录，请勿把它指向真实数据湖。样例湖包含：

- `events`：21 个小文件，含 1 个清单缺失文件、2 个完全无引用孤儿、1 个仅历史快照引用的
  文件、1 个非法分区目录；5 个快照中有 3 个过期。
- `orders`：23 个小文件，`region=skew` 的 14 个文件触发分表例外下的倾斜。
- `customers`：无分区健康表，文件不小于小文件阈值。
- `stray_export`：缺少 `_metadata` 的异常表，用于验证“不得静默跳过”。

全部 Parquet 单文件远小于 200KB，样例湖总大小约 150KB。清单格式为
`lakewatch.manifest/1`，快照为 `lakewatch.snapshot/1`，不是真实 Iceberg 元数据。

## 策略 YAML 字段

```yaml
policy:
  small_file_bytes: 4096          # 文件字节数严格小于此值为小文件
  max_files_per_partition: 12     # 文件数严格大于此值为倾斜
  max_files_listed: 8             # 诊断/动作中最多列出的文件数
  max_actions: 24                 # 单次计划动作上限，截断稳定
  compact_target_bytes: 65536     # 合并批次软目标
  compact_max_inputs: 12          # 单批最多输入文件数（>= 2）
  compact_small_files: true       # 是否生成合并动作
  vacuum_expired: true            # 是否生成过期快照清理动作
  repair_missing: true            # 是否修剪清单中的缺失文件项
  retention:
    keep_last_snapshots: 2        # 至少保留最近 N 个快照
    keep_days: 30                 # 至少保留最近 M 天内的快照
    as_of: "2025-03-10T00:00:00Z" # 可选的确定性参照时间
  tables:                         # 分表例外
    orders:
      max_files_per_partition: 8
      compact_max_inputs: 8
      # cluster:                  # 可选：显式聚簇合并契约（默认关闭）
      #   columns:
      #     - {name: quantity, direction: desc, nulls: first}
      #     - {name: id, direction: asc, nulls: last}
    events:
      max_files_listed: 6
```

必需字段：`small_file_bytes`、`max_files_per_partition`、`max_files_listed`、
`max_actions`、`retention.keep_last_snapshots`、`retention.keep_days`。其余字段有
上述默认值。未知字段、未知分表字段、错误类型和越界值都会被拒绝。分表例外可覆盖阈值、
合并参数、三个开关和 `retention` 的计数/天数；`enabled: false` 会跳过该表的全部计划动作。
缺省 `as_of` 时使用当前 UTC 日期起点，保证一天内重复扫描的血缘稳定。

分表例外还可以声明 `cluster`：一个完全显式的聚簇合并契约，`columns` 中每列都必须
给出 `direction`（`asc`/`desc`）和 `nulls`（`first`/`last`），列名不得重复。契约
进入 `policy_hash`，并随动作写入计划（计入 `action_id` 与 `plan_hash`）；执行时只按
计划中保存的契约重放，不会重新推导。缺省不声明时计划与执行结果与声明前完全一致。

快照保留是“最近 N 个”和“最近 M 天”的**并集**；边界时刻（恰好等于截止时间）仍保留。
可释放字节等于过期快照 JSON 本身的字节，加上只被过期快照引用、且不在当前清单或保留快照中
的实际数据文件字节。

## 计划与哈希

计划包含动作列表、每个动作的输入文件、预期输出、理由、输入哈希和整体 `plan_hash`。
动作按 `(kind, table, partition, action_id)` 稳定排序，`max_actions` 在排序后截断；
`deferred_action_count` 记录被截断数量。计划不包含绝对 lake 路径。

`plan_hash` 对 `format`、`policy_hash`、`as_of`、`input_digest` 和排序后的动作做
规范 JSON 序列化（键排序、无多余空白），再计算 SHA-256。`input_digest` 覆盖文件路径、
字节数、行数、内容 SHA-256、清单代数、快照序列和诊断 ID。因此相同 fixture、相同策略
两次扫描得到相同计划；动作被篡改时 `validate_plan` 会拒绝。

`python -m lakewatch plan` 是只读 dry-run，不会创建台账、报告、清单或数据文件。

## 执行、字节守恒与幂等

- **compact**：同分区小文件按路径稳定分批，用 pyarrow 读取、合并、重写为一个 Parquet
  文件。由于多个 Parquet footer 合并后通常会变小，工具在保留原始数据页偏移的前提下，
  于最终 footer 之前插入未引用的零字节对齐区，使输出物理字节数**严格等于**输入总字节数，
  同时文件仍可正常读取。随后替换当前清单并归档新快照，再删除旧输入。若动作携带
  `cluster` 契约，合并后先按契约重排行再落盘：逐列方向与 NULL 位置各自独立，相同键的
  行全部保留，计划内按路径排序的输入顺序作为最终决胜键，保证输出完整顺序稳定可复现、
  不依赖磁盘枚举先后。排序列缺失、列类型不可比较或不可排序、重排后超出输入字节预算，
  都会在更新清单、写快照、删输入之前失败，重试不会留下被清单或快照引用的半成品。
- **vacuum**：删除计划中的过期快照；删除数据文件前再次检查当前清单和剩余快照引用，
  避免误删仍被保留的状态。
- **repair**：仅从当前清单删除“计划时不存在、执行时仍不存在”的缺失文件项，不自动删除
  孤儿文件，也不重写历史快照。

台账使用 `plans`、`tasks`、`reports` 三张表。动作 ID 和任务 ID 都由计划哈希与动作内容
稳定派生；任务记录动作、输入哈希、输出哈希、状态、耗时和结果 JSON。重复执行同一计划
直接返回 `already_completed`；计划部分失败后重放时，已完成任务仍会跳过。执行后重新扫描，
将 `report-<plan_hash前12位>.json` 与 `.md` 写入 `reports/`，并同步写入台账。JSON 报告
固定包含 `format`、`schema_version`、`report_id`、`plan_hash`、`as_of`、`before`、
`after`、`delta`、`outcomes`、`actions`。

## Web、CLI 与离线声明

CLI 子命令：`scan`、`plan`、`apply`、`report`、`serve`。Web 只读展示当前扫描诊断、
dry-run 计划、台账与历史报告，不提供策略编辑、审批或在线执行。主要端点：
`/healthz`、`/api/tables`、`/api/plan`、`/api/tasks`、`/api/reports`、
`/api/reports/<id>` 和 `/api/reports/<id>/download`。页面、CSS、JS 全部本地提供，无 CDN。

运行期不发起 DNS 查询或外连；`tests/test_offline.py` 在屏蔽非回环 DNS/连接的情况下启动
真实本地服务并访问 `/healthz`、`/api/tables` 与首页。除 `127.0.0.1`、`localhost`、
`::1` 外，仓库不包含真实域名、IP、公司名或任何凭据。

## 明确不实现

- 真实 S3/HDFS/OSS 适配与云端凭据管理。
- 真实 Iceberg/Delta/Hudi 元数据兼容。
- 分布式执行、并发合并、任务队列、调度器。
- 查询引擎、SQL 执行、业务数据统计。
- 在线策略编辑、审批流、多租户隔离。
- 表 schema 演进与演进冲突自动解决。

## 验收核对

```bash
python -m lakewatch serve --config configs/default.yaml   # /healthz -> 200
pytest -q                                                 # 离线全绿
python scripts/make_fixtures.py && pytest -q              # 重建后全绿
python -m lakewatch plan --config configs/default.yaml     # 两次哈希一致
python -m lakewatch apply --config configs/default.yaml --plan reports/plan.json
python -m lakewatch apply --config configs/default.yaml --plan reports/plan.json
```

最后一条命令应报告 `already_completed` 且不产生副作用。
