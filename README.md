# Flink State Lens

仓库名：`flink-debug-tool` · [MIT License](LICENSE)

离线 Flink 状态分析桌面工具。导入手动下载的 checkpoint/savepoint，像查看 dump 一样定位到算子、子任务、状态文件和状态数据，支持按条件读取和分页浏览。当前适配 **Flink 1.20 + RocksDB**，依赖锁定 1.20.5；使用 Java 17+、Maven 和 Swing。

## 构建与运行

```sh
mvn -B -ntp clean package
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar
```

启动时直接打开快照目录或 `_metadata` 文件：

```sh
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar --open /data/savepoint
```

也可以省略 `--open`，直接将快照路径作为唯一参数。

`--open` 可同时使用 `--map` 和 `--jar`，在导入时预先设置本地路径映射与原作业依赖：

```sh
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar \
  --open /data/job-id/chk-42 \
  --map 's3://bucket/checkpoints/job-id=/data/job-id' --jar /data/job.jar
```

首次构建需要下载依赖；打包后的桌面 JAR 包含依赖，可离线运行。无需连接生产 Flink 集群。

如果使用项目内依赖缓存：

```sh
mvn -B -ntp -Dmaven.repo.local=.maven-repo package
```

## 使用流程

1. 手动下载快照及其引用的全部状态文件。增量 RocksDB checkpoint 往往需要 job 目录下的 `shared` 文件，单独下载 `chk-N/_metadata` 不够。
2. 打开快照目录或 `_metadata` 文件。先查看文件完整性，再选择左侧算子、子任务。树中优先显示元数据中的算子名称，其次为 UID；缺失时显示“未命名算子”和短 hash。悬停可查看完整名称、UID 和 operator hash。
3. 如果原文件地址仍是 HDFS/S3 URI，设置“原 URI 前缀 → 本地目录”映射，再重新导入。工具不访问远端，不按文件名猜测映射。
4. 点击算子会展开子任务，并展示该算子的文件引用。**选择具体子任务，再点击“加载数据”**，同时加载“状态 Schema”和“状态数据”。可按状态名、Key、Value、Map key 和 timer 时间范围筛选；条件在读取状态文件时执行，不受当前页内容限制。使用上一页、下一页浏览其余记录。切换选择或修改筛选条件会使旧页失效，重复选择同一节点会保留已读结果。界面区分未读取、读取中、取消、失败、零结果和部分支持；警告可在“诊断”页查看。
5. “状态 Schema”显示状态类型和序列化器；“状态数据”展示 Key、Map key、Value、Namespace、TTL 最近访问时间、timer 时间和类型，同时保留原始 HEX 与逐条解码结果。双击单元格可查看和复制完整已返回内容；CSV 导出只针对当前页。“业务状态查询”使用 State Processor API / DataStream BATCH，填写状态名和原始类型后，在本机读取整个算子。
6. Flink `PojoSerializer` 的对象解码可添加原作业 JAR 及相关依赖，恢复快照所记录的类型。缺失类、Kryo 或自定义序列化器可能只能返回原始 HEX 或部分解码结果，请以每行的“解码结果”为准。标准业务查询仅支持界面列出的基本类型和默认 namespace。

例如，远端快照引用 `s3://bucket/checkpoints/job-id/shared/a.sst`，本地保留如下结构：

```text
/data/job-id/
  chk-42/_metadata
  shared/a.sst
  ...其他被引用的文件
```

映射 `s3://bucket/checkpoints/job-id` 到 `/data/job-id`，导入 `/data/job-id/chk-42`。

## 命令行

```sh
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar --help
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar \
  --inspect /data/job-id/chk-42 \
  --map 's3://bucket/checkpoints/job-id=/data/job-id'
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar \
  --preview /data/savepoint --operator 0123456789abcdef0123456789abcdef --subtask 0 --limit 200
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar \
  --preview /data/savepoint --operator 0123456789abcdef0123456789abcdef --subtask 0 \
  --state order-map --key-contains alice --map-key-contains pending --limit 200 --offset 0
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar \
  --preview /data/savepoint --operator 0123456789abcdef0123456789abcdef --subtask 0 \
  --timer-from 1700000000000 --timer-to 1800000000000 --limit 200
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar \
  --query /data/savepoint --operator 0123456789abcdef0123456789abcdef \
  --state order-count --kind VALUE --key-type STRING --value-type LONG --limit 200
```

`--operator` 使用快照实际的 32 位 operator ID hash；示例中的值是占位符。CLI 输出 JSON，`--jar`、`--map` 可重复。

分页条件在后台读取时执行：`--state` 精确匹配状态名，`--key-contains`、`--value-contains`、`--map-key-contains` 为区分大小写的子串匹配，多个条件使用 AND；`--timer-from`、`--timer-to` 为包含端点的时间戳范围，单位毫秒。Map key 条件只匹配 MapState，timer 范围只匹配 timer。

每页最多 1000 条，同时使用 32 MiB 的解码文本保留预算；大记录可能让每页条数减少，仍可继续翻页。使用响应的 `page.nextOffset` 继续请求，其他条件保持一致，不要假定下一页 offset 一定增加所配置的每页条数。`page.offset`/`nextOffset` 是符合条件记录的位置；`page.scanned` 是本次请求扫描的记录数，不是整个状态的总条数。只有 `page.hasMore=false` 且 `page.complete=true` 才表示该子任务在所选范围内已遍历完。存在不支持的布局、解码失败或无法判定的筛选字段时，会保留 RAW 或警告，并标记 `complete=false`，不会把不完整结果标为全量。分页会重新扫描本地只读副本，大状态后面的页可能需要更长时间；可选择具体状态缩小范围。

没有现成快照时，可用 Flink 生成一个真实的示例 savepoint，再在界面导入：

```sh
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar --create-example /tmp/state-lens-example
```

目录必须尚不存在。示例包含 `order-count`（ValueState LONG）、`order-values`（ListState LONG）、`order-map`（MapState STRING → LONG），key 类型为 STRING；`alice` 累计 12、`bob` 累计 3。这些数据用于验证工具，不是用户作业的状态。

## 当前能力和边界

| 功能 | 能力 |
|---|---|
| `_metadata` | checkpoint ID、格式版本、operator hash、并行度、max parallelism、subtask handle；元数据实际保存名称/UID 且运行时 API 可读取时显示名称/UID |
| 文件检查 | 本地路径映射、文件缺失、声明/实际大小、shared/private/meta 引用 |
| 状态数据 | Canonical keyed snapshot（含 Snappy）、RocksDB incremental handle，按状态及解码字段筛选、每页最多 1000 条且可连续翻页；支持可识别布局的 Value/Reducing/Aggregating、MapState、标准 scalar、Flink PojoSerializer（需要对应作业 JAR）、内置 TTL 包装和常用 timer；保留原始 HEX 和逐条解码结果 |
| Timer | Flink `TimerSerializer` 的 event-time / processing-time 队列，展示 Key、Namespace、存储的毫秒时间戳；支持基础 Key 及 Void、基础类型、TimeWindow、GlobalWindow namespace；类型仅按 Flink 保留队列前缀识别，未知队列类型留空 |
| 业务查询 | State Processor API + 本地 DataStream BATCH，Value/List/Map 的标准类型、默认 namespace |
| 原文件保护 | 文件只读；RocksDB 原始预览和业务查询在临时副本上读取/恢复，结束后清理 |
| 导出 | 元数据报告 JSON、当前状态页 CSV；包含 Map key、存储的 TTL 时间戳、timer 时间/类型、解码结果和原始 HEX，导出不代表全 checkpoint 的全部数据 |

业务查询对 native savepoint/aligned checkpoint 为实验性支持；不支持 unaligned checkpoint 的业务查询。窗口 namespace、TTL/custom serializer 的高层业务查询、changelog、状态修改、跨快照 diff、全量 SQL 和安装包尚未实现。原始预览对 Kryo、自定义序列化器、ListState 和未知 timer 序列化器等布局仍可能返回 `RAW`；失败或部分解码不会把未知字节解释为业务对象，原始 HEX 会继续保留。Operator/broadcast/raw state 和 channel buffers 尚未完整解码，分页不意味着支持任意状态格式。

样本中的 `ttlTimestamp` 是快照实际存储的 TTL 最近访问时间，单位为毫秒；无 TTL 包装时为空。工具展示该时间戳和可解码值，不自动过滤记录，也不推断是否过期：原作业的 TTL 时长、更新策略和状态可见性配置不在快照中。MapState 的 TTL 时间戳对应具体 Map entry。

算子名称和 UID 的显示取决于快照保存的信息。Flink 1.20 `_metadata` 不包含这两个字段，无法从 operator hash 逆推出名称或原始 UID；较新格式（如 Flink 2.2）实际保存这两个字段、且运行时 API 可读取时，界面会显示它们。添加作业 JAR 用于恢复状态类型，工具不会运行作业 main 或猜测 UID。默认项目依赖仍为 Flink 1.20.5，这一展示能力不代表正式支持 Flink 2.2 快照解析。

子任务是同一算子的并行实例，没有独立业务名称。“子任务 0/2”表示并行度为 2 的算子中的第 0 个实例，编号从 0 开始；完整 operator hash 仍用于筛选和导出定位。

Flink 1.20 `_metadata` 也不包含业务记录条数、checkpoint 耗时或生成时间。格式版本不等于 Flink 版本。引用状态大小可能重复计算 shared 文件，不代表去重磁盘占用。原始状态每页最多 1000 条；单条 Key/Value 最多解码 1 MiB，超出时标记 RAW，HEX 最多展示 2048 字节并注明截断。支持范围内的已解码文本不会在 4096 字符处截断，筛选可匹配长文本后面的内容。业务查询仍最多返回 10000 个 key，每个 key 的 List/Map 最多显示 1000 项，长文本会明确标记截断。分页大小不限制底层恢复所需的文件大小；大快照需足够的本地临时磁盘。

Java 17+ 的必要模块访问参数已写入 desktop JAR 的 manifest（`java.base/java.util`），直接 `java -jar` 启动即可。若直接用 IDE/classpath 启动包含 Flink 作业的功能，请设置 JVM 参数 `--add-opens=java.base/java.util=ALL-UNNAMED`。

依据：[State Processor API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/libs/state_processor_api/)、[Checkpoint/Savepoint 支持边界](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints_vs_savepoints/)、[Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/)。

## 验证

```sh
mvn -B -ntp test
```

自动化测试覆盖真实 metadata roundtrip、canonical 与 Snappy 内容、RocksDB 增量文件与 key-group 范围、本地及 S3 URI 映射后的 State Processor API 查询、缺失/截断/大小异常、取消、路径映射，以及源文件的 SHA-256/大小/mtime 保持一致。另已用打包后的 JAR 验证示例快照生成、CLI 导入和桌面导入/原始样本/业务查询。集成测试在本机启动临时 Flink MiniCluster，需要本地回环端口。

## 本地数据

运行时显示和导出的路径、状态值可能含作业数据。下载的快照、用户 JAR、报告和本地依赖缓存不属于源码；默认忽略 `snapshots/`、`checkpoints/`、`savepoints/`、`reports/`、`target/` 和 `.maven-repo/`，请将本地数据放在这些目录或仓库之外。示例中的名称、URI 和状态值均为人工测试数据。
