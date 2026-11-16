# GraphLake S2 和 S3 实施与验证记录

测试在 OrbStack ARM64 Linux 上运行。SQLite 和 MySQL 以同一 worktree 的 S1 实现为基线，使用同一工具链和调试构建配置。ClickHouse 以同一版本的完整物化路径为对照。结果记录本地测试环境的成本。

## 来源读取

SQLite 和 MySQL 分别使用同一张 200,000 行表，设定 `--batch-size 256`，预热后交替运行两个版本各五次。完整扫描校验全部结果的摘要。包含本地残余条件 `length(name) > 0` 的无序 LIMIT 校验行数及返回值是否满足条件。首批延迟包括 CLI 启动，计到首行 JSONL 到达。ClickHouse 在真实只读账户上对同一条 Arrow 查询预热并重复五次，首批延迟包含描述来源和首批解码。完整结果对照了全部 ID 和属性值。

| 场景 | 首批 P50 | 读取行数 | 已读取字节 |
| --- | ---: | ---: | ---: |
| SQLite 全读 | 767.2 → 10.8 ms | 200,000 → 200,000 | 69,062,752 → 69,062,752 Arrow 字节 |
| SQLite 残余过滤 + `LIMIT 1` | 1,021.8 → 20.0 ms | 200,000 → 256–768 | 69,062,752 → 88,400–265,200 Arrow 字节 |
| MySQL 全读 | 575.9 → 16.6 ms | 200,000 → 200,000 | 53,772,040 → 53,772,040 Arrow 字节 |
| MySQL 残余过滤 + `LIMIT 1` | 541.1 → 14.7 ms | 200,000 → 512–768 | 53,772,040 → 137,656–206,484 Arrow 字节 |
| ClickHouse 到首批为止 | 81 → 12 ms | 200,000 → 8,192 | 2,652,480 → 108,864–108,992 HTTP 响应体字节 |

SQLite 和 MySQL 全读的查询总耗时 P50 分别从 1,490 ms 和 1,003 ms 降到 731 ms 和 479 ms。SQLite 残余过滤 LIMIT 的数据库全表扫描步数从 199,999 降到 255–767。ClickHouse 丢弃首批后的流，客户端消费停在 8,192 行和约 109 KB 响应体。ClickHouse 服务器内部扫描量和 MySQL 线上的字节数本轮未采集。ClickHouse 的 MergeTree 排序键不证明业务键唯一，需要多次消费或完整校验的查询继续使用可重扫的快照缓冲。

百万行的排序校验曾发现 SQLite 读取与本地排序同时占用查询内存时，下一批可能无法取得预算。需要本地排序、分组或去重的 SQL 保留可溢写快照。低预算、大输入的 SQLite 排序回归同时校验输出顺序和溢写。

## 小批增量写入与整理

复合键夹具包含 100,000 行，使用 `(tenant, UUID)`，每轮分散更新 100 行并保留含边界水位语义。两个版本交替消费同一轮变化。100 轮结束后分别将两份副本与来源按完整字段核对，均一致。索引从第一次全量建立，下面只统计各次增量，包含整理。

| 100 轮复合键增量 | S1 | S3 |
| --- | ---: | ---: |
| 来源实际接收行数，含重读边界 | 19,901 | 19,901 |
| 同步 P50 / 最慢一轮 | 386.5 / 630 ms | 65 / 168 ms |
| 进程读字节 `rchar` | 4,261,205,061 | 2,272,672,724 |
| 进程写字节 `wchar` | 2,064,867,607 | 149,691,579 |

第 16、31、46、61、76、91 轮触发整理。新路径累计写入是 S1 的 7.25%，未整理轮次的写入中位数约 0.94 MB。GraphLake 记录的新路径暂存与整理 Parquet 共 3,026,683 字节。`/proc/<pid>/io` 的 `rchar` 和 `wchar` 覆盖实际通过服务进程读写的源文件、Parquet、SQLite 索引/WAL 和任务状态。OrbStack 缓存文件系统上的 `write_bytes` 两侧均为 0，因此此表使用进程读写量衡量成本。

百万行的两种键夹具都对水位列建索引。每个夹具执行五轮，每轮修改 100 行，两边各接收 901 行（含重读边界），按顺序核对了全部 1,000,000 行。复合键缓存占 248,481,792 字节，可从已提交 Parquet 重建。

| 百万行夹具 | 同步 P50 | 进程读字节 `rchar` | 进程写字节 `wchar` |
| --- | ---: | ---: | ---: |
| 单列 UUID | 2,563 → 30 ms | 1,034,827,204 → 13,859,606 | 1,025,133,222 → 3,671,592 |
| `(tenant, UUID)` | 3,376 → 47 ms | 1,035,063,942 → 14,489,856 | 1,026,577,284 → 3,819,588 |

千万行复合键窄表在默认同步预算下执行一轮 100 行更新。两边各接收 101 行。同步耗时从 17,474 ms 降到 105 ms，进程读字节从 663,085,889 降到 2,039,099，写字节从 664,930,013 降到 551,211。首次全量加载从 50,757 ms 增至 160,275 ms，S3 完成后索引文件为 2,479,248,384 字节。按完整字段排序后，来源与两份副本的 1,000 万行具有相同的类型化 SHA-256 摘要 `71ffd9de03aee74dfe63a8c62e80c28c755f86cb29e69efd51030fe4f42d6486`。

初始使用宽字段的千万行夹具在 S1 全量加载失败，千万行指标使用初始属性宽度为 8 字符的同一份窄表。该规模的摘要核对采用 Arrow 批次和单独的 2 GiB 排序预算。256 MiB 下的全量排序遇到 DataFusion 的合并预算，JSON 导出的核对也曾超时。同步阶段两边仍使用默认内存和溢写预算。

## 取消与恢复

- SQLite 客户端取消后可继续查询同一来源。MySQL 取消后，另一连接可完成对同一表的 `ALTER TABLE`，证明之前的元数据锁已释放。ClickHouse 验证了首批即丢弃的客户端响应流。
- UUID 和复合键都通过重新打开、同水位无改动、整理和旧快照保护回归。整理过程中启动的读者可继续读取其固定代次。注入取消发生在索引提交、清单发布前时，旧清单保留，重试重建错代索引并清理未引用文件。
- 在 Linux 用 `strace` 延迟 `rename` 系统调用并发送 `SIGKILL`。索引提交后、清单发布前终止，重启仍读取旧代，任务标记为中断，重试发布正确结果。清单发布后、历史写入前终止，重启识别对应的 `committed_generation`，读取新代，相同水位重试不再发布。
- 最终版通过 99 项 Rust 核心测试和 10 项显式运行的真实服务测试。GitHub Actions 的远端运行状态待推送后生成。

## 复跑

```bash
docker compose -f tests/production-compose.yml up -d --wait
bash tests/production-services.sh
cargo test --locked --features admin --lib --bins --test engine --test admin
S1=/path/to/s1/graphlake
S3=/path/to/s3/graphlake
python3 tests/local_optimization_benchmark.py stream "$S1" "$S3"
python3 tests/local_optimization_benchmark.py sync "$S1" "$S3" --rows 100000 --rounds 100
python3 tests/local_optimization_benchmark.py sync "$S1" "$S3" --indexed-watermark --rows 1000000 --rounds 5 --key uuid
python3 tests/local_optimization_benchmark.py sync "$S1" "$S3" --indexed-watermark --rows 1000000 --rounds 5
```

同步基准从每个 CLI 的同一目录启动对应的 `graphlake-admin`。千万行夹具可加 `--indexed-watermark --narrow --rows 10000000 --rounds 1 --verify-memory-mib 2048`，给 JSON 核对设置更长的 `--sync-timeout`。MySQL 可为 `stream` 传入 `--catalog` 使用已有夹具。ClickHouse 对照可单独运行真实服务门禁中的 `single_scan_yields_before_eof_and_preserves_arrow`，用 `--nocapture` 保留每次测量。
