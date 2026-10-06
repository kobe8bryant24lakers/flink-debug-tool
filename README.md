# Flink State Lens

仓库名：`flink-debug-tool` · [MIT License](LICENSE)

离线 Flink 状态分析桌面工具。导入手动下载的 checkpoint/savepoint，像查看 dump 一样定位到算子、子任务、状态文件和状态样本。当前适配 **Flink 1.20 + RocksDB**，依赖锁定 1.20.5；使用 Java 17+、Maven 和 Swing。

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

首次构建需要下载依赖；打包后的桌面 JAR 包含依赖，可离线运行。无需连接生产 Flink 集群。

如果使用项目内依赖缓存：

```sh
mvn -B -ntp -Dmaven.repo.local=.maven-repo package
```

## 使用流程

1. 手动下载快照及其引用的全部状态文件。增量 RocksDB checkpoint 往往需要 job 目录下的 `shared` 文件，单独下载 `chk-N/_metadata` 不够。
2. 打开快照目录或 `_metadata` 文件。先查看文件完整性，再选择左侧算子、子任务。
3. 如果原文件地址仍是 HDFS/S3 URI，设置“原 URI 前缀 → 本地目录”映射，再重新导入。工具不访问远端，不按文件名猜测映射。
4. “状态样本”显示 schema、真实原始字节及支持的解码结果。“业务状态查询”使用 State Processor API / DataStream BATCH，填写状态名和原始类型后，在本机读取整个算子。
5. 自定义类型/序列化器可添加原作业 JAR 及相关依赖；缺失类型会显示解析错误。标准业务查询仅支持界面列出的基本类型和默认 namespace。

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
  --query /data/savepoint --operator 0123456789abcdef0123456789abcdef \
  --state order-count --kind VALUE --key-type STRING --value-type LONG --limit 200
```

`--operator` 使用快照实际的 32 位 operator ID hash；示例中的值是占位符。CLI 输出 JSON，`--jar`、`--map` 可重复。

没有现成快照时，可用 Flink 生成一个真实的示例 savepoint，再在界面导入：

```sh
java -jar target/flink-debug-tool-0.1.0-SNAPSHOT-desktop.jar --create-example /tmp/state-lens-example
```

目录必须尚不存在。示例包含 `order-count`（ValueState LONG）、`order-values`（ListState LONG）、`order-map`（MapState STRING → LONG），key 类型为 STRING；`alice` 累计 12、`bob` 累计 3。这些数据用于验证工具，不是用户作业的状态。

## 当前能力和边界

| 功能 | 能力 |
|---|---|
| `_metadata` | checkpoint ID、格式版本、operator hash、并行度、max parallelism、subtask handle |
| 文件检查 | 本地路径映射、文件缺失、声明/实际大小、shared/private/meta 引用 |
| 原始样本 | Canonical keyed snapshot（含 Snappy）、RocksDB incremental handle，每次最多 1000 条；标准 scalar 的 Value/Reducing/Aggregating 可解码，其余保留 HEX |
| 业务查询 | State Processor API + 本地 DataStream BATCH，Value/List/Map 的标准类型、默认 namespace |
| 原文件保护 | 文件只读；RocksDB 原始预览和业务查询在临时副本上读取/恢复，结束后清理 |
| 导出 | 元数据报告 JSON、当前状态样本 CSV |

业务查询对 native savepoint/aligned checkpoint 为实验性支持；不支持 unaligned checkpoint 的业务查询。窗口 namespace、TTL/custom serializer 的高层业务查询、changelog、状态修改、跨快照 diff、全量 SQL 和安装包尚未实现。原始预览不能将无法解码的字节解释为业务对象。

Flink 1.20 `_metadata` 不包含算子名称、原始 UID、业务记录条数、checkpoint 耗时或生成时间。格式版本不等于 Flink 版本。引用状态大小可能重复计算 shared 文件，不代表去重磁盘占用。业务查询最多返回 10000 个 key；每个 key 的 List/Map 最多显示 1000 项，长文本和 HEX 会明确标记截断。样本上限不是状态总记录数，也不限制底层恢复所需的文件大小；大快照需足够的本地临时磁盘。

Java 17+ 的必要模块访问参数已写入 desktop JAR 的 manifest（`java.base/java.util`），直接 `java -jar` 启动即可。若直接用 IDE/classpath 启动包含 Flink 作业的功能，请设置 JVM 参数 `--add-opens=java.base/java.util=ALL-UNNAMED`。

依据：[State Processor API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/libs/state_processor_api/)、[Checkpoint/Savepoint 支持边界](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints_vs_savepoints/)、[Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/)。

## 验证

```sh
mvn -B -ntp test
```

27 个测试覆盖真实 metadata roundtrip、canonical 与 Snappy 内容、RocksDB 增量文件与 key-group 范围、本地及 S3 URI 映射后的 State Processor API 查询、缺失/截断/大小异常、取消、路径映射，以及源文件的 SHA-256/大小/mtime 保持一致。另已用打包后的 JAR 验证示例快照生成、CLI 导入和桌面导入/原始样本/业务查询。集成测试在本机启动临时 Flink MiniCluster，需要本地回环端口。

## 本地数据

运行时显示和导出的路径、状态值可能含作业数据。下载的快照、用户 JAR、报告和本地依赖缓存不属于源码；默认忽略 `snapshots/`、`checkpoints/`、`savepoints/`、`reports/`、`target/` 和 `.maven-repo/`，请将本地数据放在这些目录或仓库之外。示例中的名称、URI 和状态值均为人工测试数据。
