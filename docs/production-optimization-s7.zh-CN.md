# S7 生产监控与发布验收

GraphLake 提供运行指标和查询探针。部署侧的独立监控进程采集状态，Prometheus 与 Alertmanager 负责告警规则和通知。

## 发布门禁

PR 和 main 推送跑核心 Rust 回归、前端测试和构建、Prometheus 规则测试及真实服务回归。服务回归使用固定镜像摘要，并校验每个必测的 Rust 用例确实执行了一次。Snowflake、BigQuery、Trino 和 Databricks 使用本机协议夹具。发布候选额外加入 Delta 夹具、C/Python 绑定及存量迁移演练。

发布工作流可手动触发，也每周运行一次。手动选择要验收的版本和已经发布的回退版本。工作流先校验源码提交、锁文件、夹具和回归回执，再构建 Debian 12 的 ARM64 与 AMD64 归档。每个架构在同架构环境解包，校验 `BUILD-INFO` 文件列表与两个可执行文件的构建标识，实际加载随包网页资源并对 SQLite 本地表执行首次同步、强制终止、重启和重试。

发布候选还要通过滚动升级、S3 切换、100 轮并发同步与整理、故障恢复、存量回退、备份固定代次和绑定验证。候选阶段保留 `candidate-linux-*` 供诊断。只有所有门禁通过且两个架构的源归档摘要一致，手动运行才生成 `linux-*` 最终产物。随包 `BUILD-INFO` 记录编译目标、源包、Cargo 锁文件、夹具和回归回执摘要。Actions 产物 `graphlake-regression` 与 `graphlake-release-acceptance` 保存日志。仓库的 main 分支保护为独立设置。

## 存量迁移与回退

1. 停止旧本地表同步和调度，等待在途任务结束。保留旧版安装包、发布版本、控制备份以及匹配的 `.graphlake-local` 快照。对原发布模型保存排序查询的结果摘要。HA 存量文件仍需满足原 Worker 的路径约定。
2. 滚动更换 Worker，每次用同一发布版本比对查询结果。启动新 Coordinator 候选并确认其处于选主等待状态，再交接主权。检查新主能查询旧格式、读取旧表定义和历史。升级窗口保持本地表写入暂停。
3. 全部读节点升级后，启用托管模式，从相同来源和列定义创建一个新表并完成全量加载。确认行、主键、删除策略与时间戳预算，再发布 `local-table://` 引用。用独立 Worker 数据目录验证查询，并重新构建 CSR、检验严格查询。
4. 启用新写入前，可用旧二进制和原目录回退。启用新写入后，旧版回退使用升级前的控制备份和匹配的旧文件，经隔离命名空间验收后切流。回退点之后的变更需按源记录重放或重建。演练保留一份只读旧本地表目录并在新命名空间用旧程序读取，验证的是这份表的恢复点。

健康表在候选版启动时保留原有状态文件字节。遇到需要恢复的中断历史，服务仍会持久化恢复结果。需要回退时记录使用的二进制、发布模型版本、数据快照、结果摘要以及业务切流时间。

## OrbStack 复跑命令

从这个 worktree 进入 OrbStack Linux。以下命令需要 Docker Compose、PostgreSQL 16 客户端、Rust、Python 3.13、Node.js 24 和 promtool。使用专用测试主机或文件目录。需复用一台 OrbStack VM 上的其他服务时，可为本次夹具设置如下独立端口。

```bash
export GRAPHLAKE_FIXTURE_POSTGRES_PORT=45432 GRAPHLAKE_FIXTURE_MYSQL_PORT=43306
export GRAPHLAKE_FIXTURE_CLICKHOUSE_PORT=48123 GRAPHLAKE_FIXTURE_ETCD_PORT=42379
export GRAPHLAKE_FIXTURE_S3_PORT=49000 PYTHONDONTWRITEBYTECODE=1
docker compose -p graphlake-production-s7 -f tests/production-compose.yml up -d --wait
source tests/production-fixtures.sh
prepare_graphlake_bucket

cargo test --locked --features admin --lib --bins --test engine --test admin
bash tests/production-services.sh
npm --prefix web ci
npm --prefix web test
npm --prefix web run build
python3 tests/monitoring_test.py
promtool check rules deploy/prometheus-rules.yaml
promtool test rules tests/prometheus-rules-test.yml
```

使用旧归档及其校验文件。以下例子里的 `OLD_DIR` 与 `CANDIDATE_DIR` 是分别解包后的根目录。OrbStack 的源代码验证也可把 `CANDIDATE_DIR/bin/graphlake-admin` 换成同一 worktree 的 `target/debug/graphlake-admin`，并在回执中标明实际编译类型。

```bash
sha256sum -c ./*-linux-*.tar.gz.sha256
python3 tests/package_smoke.py "$CANDIDATE_DIR"
work=$(mktemp -d)
python3 tests/release_lifecycle.py "$CANDIDATE_DIR/bin/graphlake-admin" \
  --previous "$OLD_DIR/bin/graphlake-admin" --inject-faults --work-dir "$work/lifecycle" \
  | tee "$work/lifecycle.log"
mkdir "$work/managed"
python3 tests/managed_local_tables.py "$CANDIDATE_DIR/bin/graphlake-admin" \
  --isolate-worker --work-dir "$work/managed" | tee "$work/managed.log"
python3 tests/sqlite_read_benchmark.py "$OLD_DIR/bin/graphlake" "$CANDIDATE_DIR/bin/graphlake" \
  --require-read-budget | tee "$work/sqlite.log"
```

`--inject-faults` 通过当前测试 Coordinator 的专用代理暂时断开 S3，并在 16 MiB 的独立 tmpfs 触发 ENOSPC。恢复后用直连 S3 运行稳态阶段。演练对只读操作的主权确认 503 最多重试 5 秒，记录次数和恢复耗时。同步提交使用原始返回状态。临时挂载和 Worker 挂载隔离需要可用的 `sudo -n`。服务回归与演练使用随机隔离的数据库表、etcd 命名空间和对象前缀。工作目录应保留到日志采集完毕。用 `set -o pipefail` 执行带 `tee` 的命令，保留测试失败的退出状态。

绑定与 Delta 复跑使用 Python 3.13 的独立环境。

```bash
python3 -m venv "$work/python"
"$work/python/bin/pip" install pyarrow==26.0.1 deltalake==1.6.6 pyroaring==1.2.0
"$work/python/bin/python" tests/delta_fixture.py "$work/delta" "s3://$GRAPHLAKE_TEST_BUCKET/delta"
GRAPHLAKE_TEST_DELTA_FIXTURE="$work/delta/fixture.json" bash tests/production-services.sh
PYTHON="$work/python/bin/python" bash tests/bindings-smoke.sh

docker compose -p graphlake-production-s7 -f tests/production-compose.yml down -v
```

## 2026-11-16 验收记录

- 旧版 ARM64 构建为 `v1.1.0+src.5bceeceedc8e`，归档 SHA-256 为 `05241efa90e0872cfcff6e5e06232ebc5484290fa463266bbb6a9953d43737f1`。 [GraphLake v1.1.0](https://github.com/SamuelSupe/graphlake/releases/tag/v1.1.0)
- 候选 ARM64 构建为 `v1.1.0+src.eac1a0c93929`，部署归档 SHA-256 为 `d1b32daf65f466115623eb4d5f6a69bd8626c1e46ef52e1ee68a8e749c6d3d10`。使用 Debian 12 与 Rust 1.97.1 构建，可执行文件需要 glibc 2.34。源归档固定验收前的快照，本节的实测记录随后补入 worktree。
- 本次源码基线为 `8189ac391ea3ef81e79a7864ba845e53a1bb7a2a` 加当前 worktree 的 S0–S7 变更。使用 PostgreSQL、MySQL、ClickHouse、etcd 及 RustFS 的固定镜像，夹具 Compose SHA-256 为 `034b43cdff5fadae68ef1dfdb85a23d3a3eb8f230932e568290d678003d79efe`。
- 运行日志位于 OrbStack Ubuntu 的 `~/graphlake-s7/evidence`。迁移和长稳统一使用种子 7 的写入顺序，在同一 48 行来源上校验排序全量结果。100,000 行 SQLite 另作读取量回归。

| 验收项 | 实测结果 |
| --- | --- |
| 核心 Rust 与真实服务 | 核心 99 通过、0 失败，32 个需夹具的用例标为忽略。另实际执行 25 个专用夹具用例与 2 个 Delta 用例，全通过。独立执行了本地目录和 S3 恢复脚本。 |
| 前端与绑定 | OrbStack Linux 上前端 34 通过，生产静态资源构建成功。C Arrow 流、Python Arrow 流、取消与缓存图操作均通过。 |
| 告警 | promtool 3.5.0 校验 22 条规则，新鲜度与磁盘规则都验证了触发与恢复。真实 Coordinator 上用查询角色运行监控，CSR 缺失时报告 `csr_missing`，构建追平后恢复。 |
| 升级与回退 | 用发布版创建存量表，两次 Worker 更换、候选主接管和启用托管前回退均逐行匹配。候选启动前后的旧 `state.json` 逐字节匹配。启用托管后用独立命名空间中的旧程序读到保存的原始 48 行。 |
| 长稳 | 905.819 秒完成 100 轮同步及 6 次整理，576 次并发查询全部匹配。同步耗时中位数 149.5 毫秒、最长 509 毫秒，查询 P95 为 1.365 秒。Coordinator 峰值 RSS 185,880 KiB。同步共接收 298 行，记录的本地 Parquet 写量为 124,881 字节，包含整理写量。最终 CSR 返回 48 行，备份校验通过。 |
| 故障注入 | 同一来源上测试 PostgreSQL 阻塞时强制终止主节点、重启 Worker、S3 断连与 16 MiB 临时卷 ENOSPC。每种写入故障保留已提交代次，恢复后重试按预期更新。Coordinator 从强制终止到同步成功重试为 7.491 秒。 |
| 固定代次 | Worker 在独立挂载命名空间内查询；旧主提交被拒；查询和备份在 GC 同时运行时读到相同固定代次。恢复站离线查询与归档结果匹配。 |
| 读取量 | 旧发行版与同源候选使用同一个 100,000 行 SQLite 文件，每种查询各保留 12 个样本。业务键点查均收 1 行，普通属性、无条件 LIMIT、按主键倒序 LIMIT 均从 100,000 行降到 1 行。普通属性无索引，SQLite 内部仍有 99,998 次扫描步数。两份优化版 ARM64 归档在同一 Debian 12 环境对照，无条件 LIMIT 的 P50 从 9.0 毫秒降到 1.0 毫秒，普通属性从 15.5 毫秒降到 3.5 毫秒。 |
| 安装包运行 | Debian 12 ARM64 上校验并启动解包产物，双 CLI、随包网页资源、强制终止、重启和同步重试通过。该候选包在同一服务夹具又跑通迁移、故障注入、回退和 20 轮同步。 |

两次早期长稳尝试在并行构建期间遇到主权确认 503。最终演练保留了只读重试计数，直连 S3 阶段命中 0 次。主要记录为 `lifecycle-retry.log`、`core-final.log`、`services-final.log`、`delta-final.log`、`isolated-recovery.log` 和 `monitor.log`。归档和解包证据见 `debian-arm64-package.log`、`debian-arm64-smoke.log` 和 `debian-arm64-lifecycle.log`。

范围边界包括各云服务真实账号和业务网络、真实告警接收端，以及需独立环境的 Iceberg REST、Spanner、MongoDB、其他 MySQL 兼容方言、SQL Server 和 Oracle。GitHub 的只读核对显示 main 当前没有 required status checks。发布工作流的依赖关系与配置已通过 actionlint，正式放行仍待同一提交的 GitHub Actions 运行记录及 AMD64 候选归档验收。
