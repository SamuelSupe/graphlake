# GraphLake 集群 S3 的 OpenDAL 后端设计

实现基于 `f7c55dd`。

GraphLake 在 `Objects::open` 下提供可选的 Apache OpenDAL 集群 S3 后端。集群适配代码在现有 `ObjectStore` 接口和 OpenDAL S3 之间传递原始 key、对象版本与条件写入结果。默认使用 `object_store` 的 S3 客户端。 [集群存储入口](../../src/admin/cluster/store.rs#L421) · [集群 S3 适配代码](../../src/admin/cluster/s3.rs)

## 接入范围

| 路径 | 实施安排 |
| --- | --- |
| 集群共享 S3 | 首批接入。覆盖目录和模型归档、托管本地表的写入与恢复、CSR、分布式中间文件、查询结果和 GC。备份 CLI 读取共享对象时使用相同的后端选择。 [集群对象接口](../../src/admin/cluster/store.rs) · [托管本地表对象](../../src/admin/local_tables/managed/files.rs) |
| 本地集群存储 | 继续由 `LocalFileSystem` 处理。 [集群存储入口](../../src/admin/cluster/store.rs#L421) |
| Parquet、CSV、JSONL、Delta 及对象浏览 | 保留现有源端客户端。托管 Parquet 上传和恢复经集群存储，查询读取 manifest 中的 `s3://` URI，仍经数据源客户端。这使试点从第一天起就能检验跨客户端兼容性。 [数据源连接](../../src/source/storage.rs) · [Parquet 读取](../../src/source/parquet.rs) · [Delta 读取](../../src/source/delta.rs) |
| Iceberg | REST Catalog 已通过 `iceberg-storage-opendal` 使用 OpenDAL，继续使用 Catalog 注入的存储配置和凭据。 [Iceberg 工厂](../../src/source/iceberg.rs#L68) |
| 查询导出 | 单独评估。当前小文件使用条件创建，大文件先上传临时对象，再条件复制到目标 key，具备 5 GiB 上限、内存预算和取消清理。接入时可验证 OpenDAL 对目标 key 直接做条件 multipart，成功后可移除临时复制。 [导出实现](../../src/engine/export/s3.rs) · [S3 条件写入](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) |

## 后端选择和配置

通过进程环境变量 `GRAPHLAKE_CLUSTER_S3_BACKEND` 选择。未设置或显式设为 `object_store` 时使用现有 S3 构建器，`opendal` 时构建 OpenDAL `Operator`，以 `Arc<dyn ObjectStore>` 包装后交给 `Objects`。S3 模式下非法值或构建失败会中止启动。启动事件 `cluster_s3_backend` 记录后端名称。环境变量可设在部署与 systemd 备份共用的环境文件中，集群 YAML 使用 `storage: s3://bucket/prefix`。 [集群配置](../../src/admin/cluster/config.rs) · [部署与备份](../../deploy/cluster.md)

```text
Coordinator / Worker / 备份恢复 CLI
  -> Objects
     -> object_store::AmazonS3（默认）
     -> OpenDalS3 -> OpenDAL S3 Operator（显式选择）

etcd -> 领导权、提交指针、任务状态和变更回执
```

- **依赖。** 集群后端使用 OpenDAL `0.59.5`，其中 S3 GET 支持返回版本 ID，条件冲突和前置条件失败有独立错误类型。同代的官方适配器依赖 `object_store 0.14`。集群内的适配代码接到仓库现有的 `object_store 0.13.2`。Iceberg 的传递依赖使用 OpenDAL `0.57.0`。 [锁文件](../../Cargo.lock) · [上游变更记录](https://apache.googlesource.com/opendal/+/HEAD/CHANGELOG.md)
- **命名空间。** OpenDAL 以桶根目录为起点，仍由 `Objects` 生成 `<prefix>/graphlake-v1/<cluster>/…`，保留已有 key 和 manifest 中的绝对 S3 URI。验证请求、list 返回值和删除目标的 key 逐字节一致，包括已有前缀里的空格、Unicode 和字面 `%xx`。 [现有 key 拼装](../../src/admin/cluster/store.rs#L421) · [适配器路径处理](https://docs.rs/crate/object_store_opendal/0.57.0/source/src/store.rs)
- **连接和凭据。** 复用现有 S3 配置解析，`AWS_ENDPOINT_URL_S3` 优先于通用端点，HTTP 必须显式允许。OpenDAL 使用 5 秒连接超时，普通对象请求使用 30 秒总超时。两个后端的 Arrow 下载均按数据进展计算空闲超时。静态凭据和会话令牌显式传给 OpenDAL，自动认证使用其 AWS 凭据链。已识别且无法等价转译的连接选项会在启动时报错。 [部署设置](../../deploy/cluster.md) · [当前 S3 构建器](https://docs.rs/object_store/0.13.2/object_store/aws/struct.AmazonS3Builder.html)

## 写入与提交

目录和托管表继续先完成对象传输，验证候选 manifest，再由持有领导权的 Coordinator 通过 etcd 事务提交指针。CSR 和查询结果也保持现有的代次和任务提交机制。进程失去领导权时，由 etcd 拒绝旧领导者发布。指针提交响应丢失时，沿用现有回执处理。GC 继续根据活跃引用、读者保护和宽限期判定对象是否可删，OpenDAL 只执行对象请求。 [领导权与目录提交](../../src/admin/cluster/control.rs) · [集群保留策略](../../src/admin/cluster/retention.rs) · [托管表 GC](../../src/admin/local_tables/managed/gc.rs)

对不可变对象，以单次带条件的写请求实现仅创建语义。对内容寻址对象的重复写，沿用当前幂等语义。条件冲突、权限失败、服务端错误和请求结果未知要保留各自的错误语义。网络重试应受上层取消和超时约束。继续按上传进展判断目录上传空闲超时，允许持续取得进展的慢速上传完成。大对象以有界分段上传，取消后尝试 abort，进程崩溃后由桶的未完成 multipart 生命周期规则回收。 [不可变写入及分段上传](../../src/admin/cluster/store.rs#L560) · [目录上传](../../src/admin/cluster/control.rs#L894) · [S3 分段上传](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)

## 兼容行为

| 差异 | 实现与验证 |
| --- | --- |
| 已有 key | 适配代码将 `ObjectStore` 的 key 原样交给 OpenDAL S3 服务。层级 list 仍以 `/` 区分前缀边界。托管表 manifest 继续保存字面 key，提交给查询引擎和分布式计划时转换为 URL。测试覆盖 `%2F`、`%25`、Unicode、空格和相邻前缀。 [托管表对象引用](../../src/source/parquet_snapshot.rs) · [对象互读测试](../../src/admin/cluster/s3/tests.rs) |
| 版本化桶 | HEAD 和 GET 都将 `x-amz-version-id` 传给 `ObjectMeta`。GET 使用同一次响应的元数据与响应体。托管表恢复继续校验版本、长度和 SHA-256。测试覆盖按版本和 `If-Match` 的 Range GET。 [OpenDAL S3 GET](https://docs.rs/crate/opendal-service-s3/0.59.5/source/src/reader.rs) · [托管表恢复校验](../../src/admin/local_tables/managed/files.rs#L238) |
| 409 和 412 | 仅创建请求发送 `If-None-Match: *`。S3 前置条件失败映射为 `AlreadyExists`。409 返回错误并由调用方处理，失败的上传候选无法通过该路径进入已提交状态。 [S3 错误语义](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) · [错误注入测试](../../src/admin/cluster/s3/tests.rs) |
| 大对象与 GC | 上传使用现有 5 MiB 分片，OpenDAL writer 配置单并发。取消沿用现有 abort，之后由生命周期规则处理崩溃遗留分片。OpenDAL 清理按项报告结果。吞吐、RSS 和 GC 请求量可用同一数据集与默认后端对照。 [上传与清理](../../src/admin/cluster/store.rs) |

## 验证与切换

1. **存储行为。** 在远程 Linux 复用 [RustFS 及 etcd 夹具](../../tests/production-compose.yml)。在一次性桶启用版本管理并设置 `GRAPHLAKE_TEST_VERSIONED_BUCKET`，运行 `cargo test --locked --features admin --lib admin::cluster::s3::tests -- --include-ignored`。该测试组覆盖对象互读、版本、原始对象键、分段上传、批量删除和 409/412 注入。OpenDAL 每批提交最多 128 个待删除对象，只有整批确认后才返回这一批的成功结果。
2. **集群行为。** 给 [托管表验收](../../tests/managed_local_tables.py) 设置 `GRAPHLAKE_CLUSTER_S3_BACKEND=object_store` 与 `--standby-s3-backend opendal`，再交换两者。传入 `--s3-prefix 'snap%2F2026+雪/space name'` 可覆盖现有转义 key 的查询及恢复。脚本验证旧领导者事务被拒绝、接管后读取旧代次、重新同步、Worker 丢失后重试同代次和异后端恢复。可另运行 [备份恢复](../../tests/local_backup_recovery.py) 覆盖 PostgreSQL 与外部 Parquet 的恢复入口。
3. **性能和互读。** 用同一数据、内存预算、并发、网络条件和查询结果对照两种后端。记录 Range 请求数与字节、查询和下载 P50/P95、上传吞吐、峰值 RSS、GC 请求数和错误率。验证旧写新读、新写旧读，包含托管 Parquet 直接查询、版本化桶和有条件的多段导出试验。导出试验结果单独用于判断导出链路是否可以迁移。
4. **试点与回滚。** 先用独立前缀完成验收。面向 AWS S3 和部署实际使用的自动凭据做一次演练后，再扩大启用范围。回滚时将变量切回 `object_store` 或移除，重启 Coordinator、Worker 和相关备份进程。沿用原 bucket、prefix、etcd 和备份文件，并用默认客户端读取试点期间提交的代次。
