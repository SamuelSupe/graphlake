# GraphLake 11110be 审查核对

核对基线为 `11110beca75f19c624c07405bc24769c3194b457`（v1.1.3），改动位于 `codex/v113-audit-fixes`。按用户确认，以修复聊天中粘贴的审查材料为准。高优先级沿用原编号。中、低优先级按可辨认的顺序补编号，并将同一行指向不同实现的问题分开。原文有缺字的条目标明了可确认的范围。

## 高优先级

| 项 | 问题 | 当前处理与证据 |
| --- | --- | --- |
| H1 | PostgreSQL 变长路径把 `IN` 列表视为单一起点 | [起点条件](../src/cypher.rs) 区分列表过滤与唯一键绑定。PostgreSQL 多起点用例通过。 |
| H2 | 单文件本地表首次增量读取尚未生成的分区 | [回收与拆分](../src/admin/local_tables/overlay/maintenance.rs) 在原文件、暂存文件和分区之间解析实际读取位置。首次增量、重试和读者固定旧代次的回归通过。 |
| H3 | CSR 溢写分段累积文件描述符 | [构建器](../src/admin/cluster/csr/build.rs) 逐级合并分段并切分属性排序输入，[systemd](../deploy/graphlake.service) 配置 `LimitNOFILE`。256 MiB、1,024 文件描述符上限下，两次构建 400 万条边，峰值 123，快照查询和源表一致。 |
| H4 | 驱动结果行预算遗漏可变宽度值 | [协议转换](../src/admin/wire.rs) 在复制字符串或二进制值之前扣除共享行预算。预算回归通过。 |
| H5 | `CASE`、`coalesce` 在检查前生成过量 SQL | [表达式生成](../src/cypher.rs) 预检分支数并在追加时限制大小。生成预算回归通过。 |
| H6 | 最短路径的关系谓词提前构造未过滤输入 | [遍历规划](../src/cypher/pattern.rs) 下推可识别的 `ALL` 与 `NONE`，并在 [路径子查询](../src/cypher/clauses.rs) 提取捕获值前应用模式和端点约束。`ANY` 中的相关 `EXISTS` 与并列路径回归通过。一般路径谓词仍需在限定跳数内对候选路径求值。 |
| H7 | 相关 `CALL` 每 16 行重新规划和扫描 | [批处理](../src/cypher/apply/batch.rs) 对适用的无上限调用走可溢写集合连接，包含嵌套调用。PostgreSQL 的 6,000 起点用例为 40 次请求，嵌套用例为 64 次。 |

## 中优先级

| 项 | 问题 | 当前处理与证据 |
| --- | --- | --- |
| M1 | 二进制业务键在扩展中被当成 UTF-8 | [图身份与扩展](../src/traversal/expand.rs) 使用二进制的十六进制表示，CSR 和图输出遵守同一约定。SQLite 二进制键、Parquet UUID 逻辑列、MongoDB BSON Binary 节点键与 ObjectId 边键的两跳查询通过。 |
| M2 | 全量对账失败已推进下次对账时间 | [本地表结果提交](../src/admin/local_tables/mod.rs) 成功对账才推进计划，失败保留到期时间并退避重试。物理删除对账和失败 CSR 刷新用例通过。 |
| M3 | Worker 领取任务的 etcd 写入结果不明 | [接单处理](../src/admin/cluster/worker.rs) 使用同一任务尝试和领取标识重读 etcd。真实 etcd 提交领取后丢弃响应，Worker 复查并返回完整查询结果。 |
| M4 | 导出在异步线程同步编码、写盘和 `fsync` | [导出](../src/engine/export.rs) 将同步工作移交阻塞执行，保留发布与持久化顺序。导出结果回归通过。 |
| M5 | MCP 与模型元数据访问在全局锁里读文件或计算 | [MCP](../src/admin/mcp/tools.rs) 在有预算的阻塞任务中读取目录，验证已有草稿也使用同一路径。[模型文件](../src/admin/models.rs) 为读者取得文件视图后释放目录锁，发布和回滚的本地写操作移交阻塞执行。 |
| M6 | 单机新鲜度观察等待全局锁并同步读清单 | [新鲜度观察](../src/admin/freshness.rs) 把本地表目录访问移交阻塞执行，在锁外异步获取版本。[清单读取](../src/source/parquet.rs) 在线程池运行。 |
| M7 | Bolt 会话锁内执行审计 I/O | [Bolt 事务](../src/admin/wire/bolt.rs) 先完成会话状态修改并释放锁，再执行 begin、commit、rollback 审计。 |
| M8 | Oracle 建连等待耗尽阻塞线程 | [Oracle 驱动](../src/source/direct/oracle.rs) 对请求等待设期限，将不可取消的 OCI 建连限制在 4 个槽。Oracle Database Free 23.6 实连的整数、定点数、时间戳和空值通过。8 个握手停滞请求在约 0.91 秒内返回 504，只有 4 条 OCI 连接在途，释放后可再次查询真实实例。底层 Oracle Net 超时由连接配置设置。 |
| M9 | Spanner 持缓存锁建连缺少超时 | [Spanner 驱动](../src/source/warehouse/spanner.rs) 对等待缓存锁和初始化客户端使用同一超时。官方模拟器的 gRPC 快照、下推和纳秒精度用例通过。接受 TCP 连接后停住 gRPC 握手，800 毫秒预算在约 0.97 秒内返回超时。 |
| M10 | 分布式 507 预算错误触发多次重试 | [阶段调度](../src/admin/cluster/distributed.rs) 直接返回 507，保留其它临时错误的重试。 |
| M11 | Worker 高频读写任务元数据 | [Worker](../src/admin/cluster/worker.rs) 将归属和取消轮询从 150 毫秒放宽至 500 毫秒，心跳写间隔为 2 秒。[部署](../deploy/cluster.md) 明确 etcd 自动压缩及配额监控。未做长期数据库增长压测。 |
| M12 | 托管快照回收重复下载保留代次清单 | [GC](../src/admin/local_tables/managed/gc.rs) 复用不可变清单引用缓存。真实 etcd 和 S3 上的推进及过期代次回收用例通过。 |
| M13 | PostgreSQL 高扇出扩展回退过晚，末层漏判断 | [远程扩展](../src/traversal/remote.rs) 在各层累计检查读取和回退阈值，包括末层。PostgreSQL 前沿读取路径通过。 |
| M14 | Gremlin `repeat…until` 的无 `emit` 路径不下推 `limit` | [循环控制](../src/gremlin/control.rs) 将相邻可下推的偏移量和限制交给分轮执行；保留轮次内顺序。Gremlin 循环和限制回归通过。 |
| M15 | 投影 K 条路径的 `k` 缺少上限 | [投影路径](../src/traversal/projected/mod.rs) 在 CLI 和表函数入口统一限制 `k` 为 1–100。摘要中关于单次子路径搜索的后半句缺失。 |
| M16 | 直连远程 CSV/JSONL 使用 64 KiB 串行区间请求 | [文本读取](../src/source/text.rs) 正式读取按预算使用 64 KiB 至 1 MiB 区间，探测仍用小区间。S3 文本读回归通过。 |
| M17 | Bolt 临时结果暂存缺少进程级额度 | [Bolt 暂存](../src/admin/wire/bolt.rs) 每结果至多 64 MiB，按写入字节占用进程共享的 1 GiB 额度，随结果生命周期释放。 |
| M18 | 活跃读者固定旧代次时本地副本持续堆积 | [旧分区检查](../src/admin/local_tables/overlay.rs) 在加载前清理可释放的文件，超过预算时停止接收新加载。固定旧代次和回收回归通过。 |

## 低优先级

| 项 | 问题 | 当前处理与证据 |
| --- | --- | --- |
| L1 | 集群模型提交的 lease 在失败时未回收，原句缺字 | [提交回执](../src/admin/cluster/control.rs) 已知未提交时撤销租约。结果不明时保留租约，防止删除可能已提交的回执。 |
| L2 | OpenDAL 后端逐对象操作，原句缺字 | [删除适配器](../src/admin/cluster/s3.rs) 使用原始对象键执行批量删除，每批最多 128 个。在启用版本管理的 S3 桶中验证了含空格、Unicode 和编码斜杠的键，邻近对象保留。 |
| L3 | 本地表加载 panic 后一直显示运行中 | [单机](../src/admin/local_tables/mod.rs) 与 [托管](../src/admin/local_tables/managed/sync.rs) 将 panic 记录为失败，释放任务状态。 |
| L4 | 结果快照相关问题，原句缺字 | [查询快照](../src/admin/query/download.rs) 在快照锁外打开文件、删除过期 Arrow 文件和处理同步写入。已核对到该范围。 |
| L5 | 托管表每秒读取全量 etcd 记录 | [调度](../src/admin/local_tables/managed/sync.rs) 平时只返回修订后变动的记录，每分钟全量校正删除。超过 512 张表也保持这一刷新频率。 |
| L6 | 模型历史反复全量解析，超过文件上限无法发布 | [版本历史](../src/admin/models/versions.rs) 使用不可变分段并缓存生效指针。保留既有历史与回滚。接近 8 MiB 的旧清单发布和备份恢复用例通过。 |
| L7 | ClickHouse 标识符反斜杠转义不全 | [ClickHouse](../src/source/warehouse/clickhouse.rs) 转义反斜杠与标识符引号；只读账号的连接夹具通过。 |
| L8 | PostgreSQL 每次查询结束额外建连，原句缺字 | [PostgreSQL 快照](../src/source/postgres.rs) 仅在请求中断时发送取消，正常结束关闭已有连接。 |
| L9 | HTTPS Parquet 每文件重复创建客户端 | [Parquet HTTP](../src/source/parquet.rs) 按运行时和源站复用客户端，保留 16 项缓存上限。 |
| L10 | PostgreSQL 非 UTF-8 数据库读取失败 | [PostgreSQL](../src/source/postgres.rs) 在启动及会话中选择 UTF-8，先前用 LATIN1 实例验证读取。 |
| L11 | MongoDB 池上限固定为 2 | [MongoDB](../src/source/direct/mongodb.rs) 默认 16，保留连接串指定的 `maxPoolSize`。MongoDB 8.0.18 实连，嵌套字段、大整数、二进制键、ObjectId 与类型漂移用例通过。 |
| L12 | HA 会话续期争用 etcd 写锁，原句缺字 | [会话续期](../src/admin/auth.rs) 允许尚未临近过期的普通刷新在锁竞争时延后。到期边界及不明确写入仍按共享状态处理。 |
| L13 | 溢写收尾集中到两条运行时线程，原句缺字 | [`collect(DISTINCT)` 溢写](../src/engine/query_functions/cypher/collect/spill.rs) 多线程环境直接使用调用方运行时，单线程环境保留回退。大结果溢写回归通过。 |
| L14 | `CALL (a)` 内 `WITH` 后导入变量仍可见 | [CALL 作用域](../src/cypher/clauses/batch_call.rs) 使可见变量遵守 `WITH` 投影，执行仍保留相关键。作用域与 `UNION` 用例通过。 |

## 本轮运行验证

在远程 Linux 建立了独立目录并同步源码。远程构建停在 crates.io 索引阶段，后续 SSH 握手超时。以下运行验证在 OrbStack ARM64 完成。

- 140 个常规测试通过。另跑通了带版本的 S3、两个托管 GC、Coordinator/Worker 故障恢复和本地表对账共 5 个集成用例。
- Oracle Database Free 23.6、MongoDB 8.0.18 和 Spanner Emulator 1.5.6 的 3 个连接器用例通过。Spanner 验证使用官方模拟器和实际 gRPC 连接，生产 Google Cloud 身份认证未纳入环境。[Spanner Emulator](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator)
- 单独验证了 etcd 领取响应丢失、Oracle 建连并发与恢复，以及 Spanner 握手超时。相关测量和条件见 M3、M8、M9。
