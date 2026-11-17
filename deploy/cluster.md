# GraphLake 多 Worker 查询服务

集群模式通过现有 HTTP 管理 API 将只读 Cypher、SQL、Gremlin 查询和 Arrow 导出分发到多个 Worker。配置 `distributed_partitions` 后，符合条件的单条查询还可以跨 Worker 执行 Parquet 扫描、跨源 Join 和局部聚合，由接单 Worker 完成全局归并。浏览器、服务账号、模型发布回退和单机 CLI 沿用现有入口。

已提交的查询预览、图续页与 Arrow 快照保留 30 分钟。重新执行导出产生的下载票据有效 60 秒，可领取一次。新 Coordinator 使用同一份集群配置启动后，可继续读取这些结果。Bolt 和 Gremlin 驱动会话、在线探索、模型校验与发布由 Coordinator 执行。配置分布式分区后，HTTP 管理 API 也可发布和读取分区 CSR。

## 启动

使用现有 `admin` 构建和前端构建。测试环境可在一台 Linux 主机使用多个进程。跨主机部署使用独立 etcd 和 S3 兼容对象存储，所有主机使用同步时钟。

```yaml
name: finance
metadata: http://127.0.0.1:2379
storage: /srv/graphlake/cluster-data
token_env: GRAPHLAKE_CLUSTER_TOKEN
distributed_partitions: 4
memory_mib: 256
workers:
  - http://127.0.0.1:7441
  - http://127.0.0.1:7442
  - http://127.0.0.1:7443
```

将配置保存为 `cluster.yaml`。启动可用的 etcd v3 HTTP/JSON 网关，创建私有存储目录，向各进程注入同一个随机集群令牌。跨主机时将 `storage` 改为 `s3://bucket/prefix`，通过各主机环境配置 AWS 凭据与区域。S3 的 `graphlake-v1/<name>/results/` 前缀应配置对象保留策略和未完成分段上传回收。应用每五分钟清理过期结果对象。归档模型位于独立的 `models/` 前缀，按模型保留策略管理。

集群 S3 默认使用 `object_store`。部署试点可在 Coordinator、Worker 和备份恢复命令共用的环境文件中设置 `GRAPHLAKE_CLUSTER_S3_BACKEND=opendal`。启动事件 `cluster_s3_backend` 记录选用的后端。两种后端使用同一 bucket、prefix 和 etcd 状态，切回时移除该变量或设为 `object_store`，然后重启相关进程。查询源和 S3 导出由各自的数据源或导出配置管理。

OpenDAL 使用 `AWS_ENDPOINT_URL_S3` 或通用的 `AWS_ENDPOINT` / `AWS_ENDPOINT_URL`，区域缺省为 `us-east-1`。HTTP 端点需要 `AWS_ALLOW_HTTP=true`。静态密钥可附带 `AWS_SESSION_TOKEN`，自动凭据由 OpenDAL 的 AWS 凭据链加载。使用自定义 STS 或 IMDS 地址、SSE-C、S3 Express 或 `AWS_*` HTTP 客户端覆盖项的部署应使用默认后端。 [S3 后端设计与验收](../docs/design/opendal-s3.md)

生产环境在 `metadata` 配置同一 etcd 集群的最多三个成员入口，例如 `metadata: [https://etcd-a.example:2379, https://etcd-b.example:2379, https://etcd-c.example:2379]`。每个节点使用同一份成员清单，地址不可跨独立集群混用。读取、申请空租约及续租在可用入口间切换，每个入口的响应预算为 210 毫秒。绑定主权和业务状态写入只在建连失败时换入口重试，可能已提交的写入使用原操作回执确认。原单地址配置继续可用。

通过 [OpenTelemetry 配置](README.md#opentelemetry-上报) 为每台 Coordinator 和 Worker 指定 Collector 和实例标识。请求 Trace 可从 Coordinator 延续到执行 Worker。备份单元加载相同的环境文件，可将 `backup_exported` 事件接入备份新鲜度告警。

```sh
graphlake-admin --catalog-dir /srv/graphlake/catalog auth init

graphlake-admin --cluster-config /srv/graphlake/cluster.yaml --port 7441 \
  worker --source-dir /srv/graphlake/catalog
graphlake-admin --cluster-config /srv/graphlake/cluster.yaml --port 7442 \
  worker --source-dir /srv/graphlake/catalog
graphlake-admin --cluster-config /srv/graphlake/cluster.yaml --port 7443 \
  worker --source-dir /srv/graphlake/catalog

graphlake-admin --catalog-dir /srv/graphlake/catalog --assets-dir /srv/graphlake/web \
  --cluster-config /srv/graphlake/cluster.yaml --query-slots 3
```

Worker 不需要管理员凭据。它根据内容版本从共享存储取出发布的 YAML，并在 `--source-dir` 下解析相对路径。Coordinator 和 Worker 需要读取相同来源；跨主机时将 Parquet 放到共同的对象存储，或者为相对路径挂载同一份数据并保持可比较的来源版本。模型目录和服务账号策略随 Coordinator 备份恢复。

升级集群时，应先升级所有 Worker，再启用新版 Coordinator 的模型探针。旧 Worker 缺少探针接口，检查会将其标记为无法完成。新版检查也会验证每个 Worker 到 etcd、模型归档与来源的访问。

Worker 固定监听 IPv4 回环地址。跨主机时在 Worker 前部署只开放 `/internal/v1/jobs`、`/internal/v1/stages`、`/internal/v1/csr`、`/internal/v1/identity` 和 `/internal/v1/readiness/{model}/{revision}` 的 mTLS 反向代理，将 `workers` 改为 HTTPS 地址，并为 Coordinator 和 Worker 配置 `ca_file` 和 `identity_file`。客户端身份文件为 PEM 格式，包含证书和私钥。代理应验证客户端证书。etcd 网关和 S3 也部署在受控网络内，存储权限限定到集群的前缀。GraphLake 管理后台继续只监听回环地址。

## 单条查询跨节点执行

`distributed_partitions` 省略或设为 `0` 时，每条查询交给一台 Worker。启用时可设置 `2` 至 `8`。一台 Worker 也可执行同一分区计划和 CSR，分区全部在本机执行。所有 Worker 使用同一构建版本。按压缩字节估算，将 Parquet 文件和行组分配到扫描分区。接单 Worker 在原 PostgreSQL 只读事务中读取数据，并将 Join 需要的分区提交到共享存储。远端 Worker 可以读取已提交的 Shuffle 分区来执行 Join、局部聚合和排序，最终分页和全局归并由接单 Worker 执行。可下推的业务键校验和多表映射也使用同一路径。没有可拆分扫描的查询，以及含预执行路径扩展、循环、过程或相关 `CALL` 的查询，继续由接单 Worker 处理。

Shuffle 使用压缩 Arrow IPC，先写 Worker 本机临时文件，再发布到共享存储。查询结束清理中间对象，异常终止遗留的对象由现有结果保留策略清理。`memory_mib` 为单次 Worker 执行或 CSR 构建的内存预算，默认 `256`。Worker 的 `process_memory_mib` 默认 `2048`，支持 `16` 至 `2048`。分区执行开启时会为至少一个远端任务保留 `memory_mib`，其余空间决定可同时接受的查询数上限。内存预算统计引擎申请，会话、网络、文件缓存和运行时也会占用进程内存。分布式阶段按分区构建 Hash Join，共享构建侧状态的 Join 留在单个 Worker 上执行。`64 MiB` 及以下优先规划可溢写的排序归并 Join。单条查询的累计已提交 Shuffle 上限沿用引擎的 1 GiB 溢写预算。

`distributed_min_rows` 是可选的本机执行门槛，默认 `0` 表示保持分区配置。非零时，CSR 以清单中的顶点加边数判定，小于门槛的分区直接在接单 Worker 运行。查询计划多输入的统计已知且尚未进行分区暂存时，用输入行数估计作相同判断。统计缺失或已有已提交分区的路径沿用分区计划。设置 `0` 可复现强制分区对照。链接数、端点倾斜和来源 I/O 环境会改变成本。

每次分区尝试使用独立对象前缀。下游只读取已确认完成的输出。远端 Worker 断连后，未提交的分区可由其他 Worker 或接单 Worker 重试。Parquet 重试核对原对象版本，Shuffle 重试读取原已提交分区。对象已经变化时返回 `graph_sources_changed`。接单 Worker 丢失 PostgreSQL 事务时，沿用 `worker_lost` 语义。

实际使用分布式阶段的查询预览会返回 `distributed`，包括参与 Worker、阶段数、远端 Join 和局部聚合分区数、重试数、Shuffle 字节数以及 Worker 算子溢写字节数。指标统计提交的分区输出。

## 分区图执行

管理员用 `POST /api/v1/models/{id}/csr` 构建并发布当前生效模型的完整 CSR。请求必填 `model_revision`，可选 `graph_options` 和 `timeout_ms`，默认 60 秒，最大 600 秒。构建复用已有映射、逻辑分区、SCD2、业务键和端点语义。写入并校验清单后，etcd 使用比较并交换更新模型版本对应的代次指针。节点按标签和类型化复合键分片，正反邻接按所在顶点分片，平行边引用独立的代次内属性记录，单顶点的高扇出邻接可跨块。 [etcd 事务契约](https://etcd.io/docs/v3.6/learning/api/#transaction)

`GET /api/v1/models/{id}/csr` 返回最新代次、来源版本、边和节点数量、对象字节数和构建指标。查询使用原管理 API 和原结果读取、取消、导出入口。示例请求体如下。

```json
{
  "request_id": "84ce0e43-d6a0-4e55-a5ca-95074b227365",
  "model_revision": "<active_revision>",
  "csr": {
    "generation": "<generation>",
    "direction": "outgoing",
    "edge_types": ["Payment"],
    "operation": {
      "kind": "shortest",
      "start": { "label": "CustomerAccount", "key": ["9007199254740993", "1"] },
      "target": { "label": "Merchant", "key": ["1", "7"] },
      "max_hops": 3
    }
  }
}
```

- `reachable` 接受 `start` 和 `max_hops`，包括起点及各顶点最小跳数。最短路用 `shortest` 或 `all_shortest`。后者还要求 `max_paths`，范围 1 至 10,000，超出时返回错误。路径返回按顺序排列的顶点和边属性 JSON，键值和属性文本保留显示精度。入边和无向遍历分别使用 `incoming` 和 `undirected`。省略 `edge_types` 使用全部边类型。
- `pagerank` 接受 `damping`、`tolerance`、`max_iterations`。每轮合并各分区的贡献，并重新分配悬挂顶点的排名。 `wcc` 接受 `max_iterations`，使用无向邻接，返回分量内最低的类型化业务键。未收敛的 WCC 返回错误。 [分区迭代执行模型](https://dsf.berkeley.edu/cs286/papers/pregel-sigmod2010.pdf)
- 请求固定清单中的 `graph_options` 和来源版本，省略外层 `query`、`language`、`parameters_json` 和 `graph_options`。刷新期间已经接单的查询继续读取旧代次，新查询读取最新代次。使用过期代次开始新查询返回 `409 csr_generation_conflict`。
- PageRank 和 WCC 在各 Worker 上加载一个分区的顶点状态，并把下一轮状态和按目标分片分组的消息提交到共享存储。接单 Worker 归并每轮的标量收敛信息，并分批收集最终 Arrow 结果。分区重试使用前一轮的已提交状态。每个分片的状态仍受执行该分片的 Worker 内存预算约束。
- 每个 Worker 可为最近两个已发布代次保留校验过的邻接块，上限为 Worker 单查询预算的八分之一且最多 8 MiB。变长路径可复用同一起点遍历中的小型邻接列表。
- CSR 清单按模型保留最近使用的两个代次，每个进程最多保留 256 条，按清单 JSON 大小合计最多 128 MiB。同代次的并发加载合并为一次读取和解码。
- 响应的 `csr_stats` 提供远端分区数、Worker、重试、对象读取、分区控制消息字节、状态交换文件字节和内存预留峰值。已发布 CSR 对象保存在独立的 `csr/` 前缀，Coordinator 每五分钟检查无人引用的代次。当前生效模型版本的 CSR 指针及尚有租约任务引用的代次受保护。Coordinator 比较 CSR 指针版本及生效模型版本后清除退役指针。CSR 回收先列出代次目录，跳过仍有引用的代次下的块文件。同轮引用检查固定在一个 etcd 修订并分页读取。模型回滚后可重建已退役的 CSR。启用 HA 时由当前 Coordinator 主节点回收。所有对象写入满 24 小时后，首次确认无人引用时写入回收标记，再过 24 小时并复查引用后删除整代对象。

普通 Cypher 查询可在请求体中提交顶层 `csr_generation`，同时保留原 `language: "cypher"`、`query`、参数和结果选项。变长关系、`shortestPath()` 和 `allShortestPaths()` 使用该代次的正反邻接。刷新会生成第 5 版快照，按实体 ID 组织类型化属性块。列表表达式、`UNWIND` 和图预览按命中的节点或边 ID 批量读取。Worker 对当前邻接候选执行关系模式内联属性过滤，`csr_stats.filtered_arcs` 记录被过滤的邻接项。请求省略 `csr_generation` 时读取实时来源。第 3 版快照支持原有执行路径，刷新后启用索引和 Worker 过滤。第 2 版快照需要刷新才能用于普通 Cypher 和分布式 WCC。

查询结果的属性查看、邻居展开和搜索可向 `POST /api/v1/models/{id}/explore` 提交结果中的 `model_revision`、`csr_generation` 和图范围，使用原代次的实体标识和数据。发布新代次或新模型版本后，只要原代次对象和模型归档仍在，已有结果可继续探索。快照被清理后返回 `409 csr_snapshot_expired`，可用当前已发布代次重新运行查询。直接从来源探索带时间区间的 SCD2 映射时，每次新探索使用当前时间，该探索签发的游标保留开始时的时间。

第 4 版及更新的快照支持整数和 Decimal 关系属性的块级范围与独立列页，以及浮点和字符串关系属性的独立列页。Worker 可用整数和 Decimal 的范围排除无匹配的块，再通过范围读取取回剩余块的谓词列。浮点和字符串在列页上执行原 Cypher 比较规则。包含未索引列的过滤继续读取类型化属性块。`csr_stats.skipped_property_blocks` 和 `csr_stats.filter_column_pages` 分别记录跳过的属性块和读取的列页。

普通 Cypher 路径使用紧凑的关系身份块。对 `UNWIND relationships(p) AS e` 后由 `WITH` 或 `RETURN` 排序分页的查询，可按多个 ID、属性或别名排序键先选页，再回填页内属性。选页前按需读取过滤、排序、去重和聚合依赖的属性。`DISTINCT` 或聚合按关系身份保留边时，依赖该身份的返回属性可在分页后回填。选页前需读取完整关系属性的表达式走完整计划。刷新后的第 5 版使用 Zstandard 压缩 Arrow 属性块与列页，并为关系键生成类型支持的列页。既有快照和缺少所需列页时使用原属性块。回填优先选择所需列页与整块中读取字节较少的方案。`csr_stats.sort_column_bytes` 和 `hydration_column_bytes` 分别记录选页及回填阶段实际从对象存储读取的独立列页字节。

路径展开、表达式回填和图预览在同一次查询中共享已校验的块。内存淘汰的身份块及属性块可压缩后写入有上限的查询临时文件。`csr_stats.lookup_spilled_bytes` 记录该文件的实际写入量。

第 5 版快照压缩节点身份、关系身份和关系记录块。读取时按解压后的大小保留内存并校验内容。新版 Coordinator 和 Worker 可读取既有快照。升级所有 Worker 后再刷新，旧 Worker 无法读取第 5 版。

```json
{
  "request_id": "a26b1282-31b2-4745-93d7-82279435391f",
  "model_revision": "<已发布模型版本>",
  "csr_generation": "<已发布 CSR 代次 UUID>",
  "language": "cypher",
  "query": "MATCH p=allShortestPaths((a:CustomerAccount {account_id: 9007199254740993, tenant_id: 1})-[:Payment*1..3]->(m:Merchant {tenant_id: 1, merchant_id: 7})) RETURN length(p), [e IN relationships(p) | e.amount]"
}
```

## 请求和故障恢复

现有 `POST /api/v1/models/{id}/query` 返回相同的表格、来源版本和图结果，集群模式增加 `execution_worker`。客户端用原始 `request_id` 请求 `GET /api/v1/queries/{request_id}` 查看状态或取回结果。HTTP 202 表示任务仍在运行，HTTP 200 返回查询预览或导出票据。查询、取消和结果读取继续校验请求所有者和模型授权。

调度前保存请求与发布版本，接单 Worker 通过 etcd 原子领取任务，再写结果并原子提交。Coordinator 断开后，已领取任务可继续运行。Coordinator 在派发前退出时，客户端重新连接后请求原 ID 可恢复派发。未领取任务会尝试其他可用 Worker。

接单 Worker 每秒更新任务心跳。Coordinator 按本机单调时钟观察 etcd 中的任务版本变化。持续 6 秒未见心跳时会确认接单进程的身份。原进程不可达且查询固定了 CSR 代次时，协调器可在原超时内用同一请求 ID 和快照重新派发一次。重试使用独立的输出路径，只有当前尝试可以提交结果。实时来源查询在接单进程丢失后返回 `worker_lost`。远端分区可在同一快照代次交给其他 Worker 重试。CSR 派发为剩余远端尝试和本地回退预留时间，远端 Worker 按本次尝试的期限执行。结果写入共享存储后，如果 etcd 暂时无法确认终态，接单 Worker 保留请求和结果并继续确认，原请求可供 Coordinator 重派。

浏览器从原模型的查询页选择「找回原查询」时，按原请求 ID 查看状态或取回结果。结果恢复后，仍可下载本次保存的 Arrow 结果和读取后续图批次。跨页面刷新后，重新执行需从编辑器输入语句。取回结果和取消任务仍要求会话及模型访问权限。

每个 Worker 用 `--query-slots` 限制接单查询数。启用分布式执行后，每个 Worker 至少为远端分区预留一个槽，接单查询分配完后的额外内存最多可供四个远端分区同时执行。Worker 以本地单调时钟执行分发方下达的剩余时长；旧发送方未发送预算时才按兼容路径使用绝对截止时间。Coordinator 的同名选项控制入口查询并发和本地引擎总槽数。本地查询、数据校验、CSR 构建和本地表加载共用引擎槽及总资源预算，并分别保留各自的入口节流。Coordinator 为邻居探索预留受配置约束的独立预算，为仅结构校验和模型来源探针预留小额预算。数据源上的等待不会占住另一类请求的执行槽。Worker 的查询和远端任务共用配置的进程资源预算。结果使用现有 Arrow 与图分页格式。集群状态使用 etcd 的事务和租约。同一 Coordinator 在五秒窗口内复用查询元数据租约，每个查询仍具有独立的 etcd 状态键。Worker 控制接口使用版本化 HTTP JSON。

在 OrbStack 的 PostgreSQL、Parquet 百万边夹具和 16 MiB 查询预算下，终止已领取查询的 Worker 后，另一台 Worker 使用原请求 ID 和 CSR 代次返回相同顺序的 500 行及相同来源版本。Chrome 验证了旧代次展开、快照缺失后使用当前代次重建，以及刷新后取回原请求结果。

同一夹具的实时 Cypher 在 16 MiB 下对照单机结果，覆盖 `WHERE EXISTS`、投影 `EXISTS`、带排序和分页的 `EXISTS`、`COUNT {}` 去重及空组聚合、模式推导和列表内关联。原先构建哈希表失败的查询使用可溢写的排序归并连接。开启溢写时内存预算维持 16 MiB。

`EXISTS` 和 `COUNT {}` 内的关联 `CALL` 由接单 Worker 执行，复用当前查询的 PostgreSQL 事务和已固定的 Parquet 对象版本。中间输入与结果共用查询的内存和溢写预算。带有列表推导和行子查询的语句关闭 DataFusion 的物理投影下推，以保留列表变量与外层列的绑定。

用同一套 484 个 PostgreSQL 节点和 1,000,903 条 Parquet 边跑了 13 组端到端查询。三 Worker 集群的 16 MiB HTTP 结果与单机 16 MiB、384 MiB 结果逐项一致，覆盖变量导入、嵌套调用、聚合后分页、去重、`UNION ALL`、可选匹配和列表推导。带中间聚合与两层分页的用例返回计数 2，集群请求耗时约 5.3 秒，单机四分区复测的峰值内存预留为 10.3 MiB。

## 第三阶段运行验证

在 OrbStack ARM64 上用三台 Worker、真实 PostgreSQL 节点、Parquet 边、etcd 和 RustFS S3 验证。32 MiB 内存配置下，四分区构建耗时约 18 秒，发布 484 个顶点、1,000,903 条边和约 173 MiB CSR 对象。构建读取约 31.5 MiB Parquet，快照缓冲溢写约 54 MiB。同一夹具的多表映射在 32 MiB 下刷新约 17.7 秒，保留精确复合键与 Decimal 边属性。

| 查询 | HTTP 耗时 | 对象读取 | 分区交换 |
| --- | ---: | ---: | ---: |
| 一跳无权最短路 | 0.034 秒 | 0.45 MiB | 261 B |
| 高扇出两跳可达性 | 0.379 秒 | 37.2 MiB | 12.8 KiB |
| PageRank，4 轮 | 3.44 秒 | 366.8 MiB | 190.8 KiB |
| 弱连通分量，4 轮 | 2.52 秒 | 183.4 MiB | 289.9 KiB |

可达集合和 WCC 分量代表与单机对照一致。PageRank 最大绝对误差为 `4.26e-16`。刷新与查询重叠时，原查询按原代次完成。注入远端 Worker 进程退出后，PageRank 在同代次重试并返回。集成测试覆盖无业务键平行边、已发布代次跨 Coordinator 重启和旧代次冲突。记录来自同一虚拟机，跨独立主机的扩容收益尚未测量。

### Cypher 和分区顶点状态基准

在同一 OrbStack 环境和 32 MiB 预算下，带属性快照的 CSR 刷新约 20.6 秒，生成约 205.7 MiB 对象。沿用 PostgreSQL 复合业务键和 Parquet Decimal 边属性，六种路径查询在较小的原夹具切片上与单机逐行一致。百万边夹具用单机固定一跳 Join 校验已知的一跳结果与并列最短路径。只返回路径长度的查询约 0.12 秒，运行中终止远端 Worker 后用同一代次重试一次并返回相同结果。

| 百万边 Cypher 查询，返回边属性 | 三次 HTTP 请求中位数 |
| --- | ---: |
| 有界无向变长路径 | 8.36 秒 |
| `shortestPath()` | 8.36 秒 |
| `allShortestPaths()` | 8.33 秒 |
| 无类型 `allShortestPaths()` | 8.34 秒 |

上述边属性查询仍需扫描发布代次中的属性表，读取约 16 MiB 对象。百万边单机固定一跳 Join 实测约 1.0–1.3 秒。它用于对照结果，执行计划与分区路径不同。

原夹具上的 PageRank 四轮耗时 2.38 秒，最大绝对误差为 `4.26e-16`。WCC 耗时 3.45 秒，分量代表逐行匹配单机。两项算法均在远端 Worker 被终止后重试并返回。加入 180,000 个隔离的 PostgreSQL 顶点后，两项算法在 32 MiB 预算内完成。解码下载的 Arrow 结果确认各包含 180,484 个唯一顶点，PageRank 总和为 1，WCC 中新增隔离顶点各自成分量。下表为该规模的一次 HTTP 查询，内存为引擎预留峰值。

| 算法 | 耗时 | 接单 Worker 峰值 | 分区 Worker 峰值 | 状态交换写入 |
| --- | ---: | ---: | ---: | ---: |
| PageRank，3 轮 | 9.77 秒 | 7.88 MiB | 6.58 MiB | 25.35 MiB |
| 弱连通分量 | 17.24 秒 | 7.52 MiB | 6.58 MiB | 39.41 MiB |

### 路径属性索引与 Worker 过滤

在同一台 OrbStack 虚拟机、三个 Worker 和 32 MiB 预算下，重新发布 484 个 PostgreSQL 顶点和 1,000,903 条 Parquet 边。第 4 版刷新耗时 42.22 秒，生成约 221.5 MiB 对象，快照和邻接溢写约 130.7 MiB。上一次第 3 版刷新为 20.6 秒、205.7 MiB。

| 返回 Decimal 边属性的 Cypher 查询 | 第 3 版中位数 | 第 4 版中位数 |
| --- | ---: | ---: |
| 有界无向变长路径 | 8.36 秒 | 0.381 秒 |
| `shortestPath()` | 8.36 秒 | 0.383 秒 |
| `allShortestPaths()` | 8.33 秒 | 0.387 秒 |
| 无类型 `allShortestPaths()` | 8.34 秒 | 0.385 秒 |

每项执行三次。对象读取由约 16 MiB 降为约 1.9 MiB，覆盖邻接、属性和身份。单机固定一跳 Join 对照确认复合键、并列边和 Decimal 金额。32 MiB 与 16 MiB 均通过指定边类型、无类型、无匹配和数字与字符串比较用例。终止正在处理过滤的远端 Worker 后，同代次重试一次，约 0.56 秒返回原结果。16 MiB 用例读取已发布快照，刷新验证用 32 MiB。

高扇出探针中，Worker 过滤了 2,899 条邻接，返回与单机一致的两条边。过滤只解码谓词引用的列，但候选散布时仍需读取它们所在的属性块。这次读取约 31.3 MiB，共耗时 1.79 秒。

### 第 4 版属性块裁剪与刷新

沿用同一套 484 个 PostgreSQL 顶点和 1,000,903 条 Parquet 边，在 OrbStack 的三个 Worker 上复测。32 MiB 下刷新从 42.22 秒降至 25.95 秒，计入快照与邻接构建的溢写从 130.7 MiB 降至 64.2 MiB。属性、边身份索引和邻接共用一次排序输出，端到端构建对象读取从 62.1 MiB 降至 31.5 MiB。包含谓词列页的新代次约 225.3 MiB，原第 4 版约 221.5 MiB。

| 2,901 条候选邻接的等值过滤 | 返回条数 | 优化前对象读取 | 优化后对象读取 | 优化后三次 HTTP 中位数 |
| --- | ---: | ---: | ---: | ---: |
| 稀有金额 `12.25` | 2 | 31.3 MiB | 0.76 MiB | 0.115 秒 |
| 常见金额 `15.0` | 127 | 60.1 MiB | 32.9 MiB | 0.691 秒 |
| 范围外金额 `-1` | 0 | 未测 | 0.18 MiB | 0.090 秒 |

对象读取包含邻接、过滤及返回路径所需的身份与属性。稀有金额跳过了 123 个候选属性块中的 121 个。金额、字符串和 `null` 的等值探针、变长路径、并列最短路、可达集和弱连通分量与单机对照一致。PageRank 四轮的最大绝对误差为 `4.26e-16`。16 MiB 下刷新耗时 25.62 秒，六组高扇出过滤在新发布的代次上均返回相同结果。32 MiB 下，多表映射刷新耗时 26.23 秒，六组过滤也通过单机对照。新 Worker 读取原第 4 版快照时，两组数值过滤仍正确。在远端 Worker 执行过滤时终止进程，原请求在同代次重试一次，约 0.22 秒返回相同结果。

### 浮点、字符串列页和路径结果回填

在 OrbStack 的三个 Worker 上沿用原百万边夹具，并保留同一组 ID、端点和 Decimal 值，派生一份带 `Float64` 与 UTF-8 关系属性的 Parquet。常见金额查询逐行校验 127 个关系 ID、Decimal 金额和时间戳，32 MiB 下对象读取从 58.9 MiB 降至 30.3 MiB，三次 HTTP 请求的中位数从 5.75 秒降至 2.99 秒，查询缓存写入约 24.2 MiB 临时文件。原夹具启用紧凑身份块后，快照体积从 225.3 MiB 增至 253.9 MiB。新 Worker 可直接读取原第 4 版快照。

另一组查询展开全部 2,901 条候选边，按关系 ID 排序并返回前 500 行。排序前仍需回填全部候选。32 MiB 下，临时文件复用将对象读取从约 330 MiB 降至约 61.5 MiB，写入约 59 MiB 临时文件，最终版本在 40.6 秒返回 500 行。原版本在同一条查询上触发 60 秒超时。最终版本在 16 MiB 下耗时 56.1 秒，两个预算均逐行核对了 ID、Decimal 数值和时间戳。

16 MiB 下带浮点和字符串的新快照刷新成功，六组查询覆盖浮点、UTF-8、两列组合、负零、数字与字符串混比以及 `null`。单机对照逐行核对关系 ID 和投影结果。执行两列组合过滤期间终止远端 Worker，同一快照代次重试一次，返回 127 行。接单 Worker 独立维护心跳，避免长时间处理路径回填时误报掉线。对象读取统计包含邻接、过滤、关系身份与属性回填。

### 路径按关系 ID 选页与第 5 版压缩

同一套 OrbStack 夹具包含 484 个 PostgreSQL 节点和 1,000,903 条 Parquet 边。查询展开 2,901 条候选边，按关系 ID 选出前 500 行，再读取金额和时间戳。在 16 MiB 下，原版本耗时 56.1 秒。启用先选页后，原第 4 版快照同一查询耗时 3.22 秒。第 5 版快照上三次 HTTP 请求中位数为 2.87 秒，单次对象读取降至 7.7 MiB。两版新查询都与单机逐行核对了 ID、Decimal 数值和时间戳。

第 5 版压缩身份块后，快照从 253.9 MiB 降至 88.6 MiB。16 MiB 下刷新耗时 29.19 秒。32 MiB 下执行路径分页时终止远端 Worker，原请求在同一快照代次重试一次，仍返回 500 行。多表映射上的无类型路径按关系 ID 倒序，跳过 250 行后取 100 行，结果与单机逐行一致。集成回归也覆盖直接按 ID 排序和 `LIMIT 0`。百万边快照通过可达集、最短路、PageRank 和弱连通分量的单机对照。

### 路径属性排序与深分页

沿用同一套 OrbStack 夹具和三个 Worker。16 MiB 下，`WITH` 从 2,901 条候选边按 Decimal 金额降序、关系 ID 升序跳过 1,900 行并返回 500 行，再回填时间戳。原版本单次请求耗时 57.36 秒，优化后三次顺序请求的中位数为 18.15 秒。查询缓存溢写从 59.3 MiB 降到 17.2 MiB，对象读取从 33.3 MiB 增到 42.0 MiB，其中选页列读取 11.5 MiB。新增列页使快照从 88.6 MiB 增到 96.3 MiB，16 MiB 下刷新耗时 34.92 秒。三次返回的 ID、金额和时间戳及其顺序均与单机逐行一致。

关系别名传入 `RETURN`、`WITH` 过滤后分页、时间戳与金额和 ID 三键排序也通过单机逐行对照。原第 5 版快照走属性块回退，过滤分页返回相同的 90 行。直接按 ID 排序的三次请求中位数为 2.68 秒。16 MiB 和 32 MiB 下均在远端分区执行期间终止 Worker，原请求在同一代次重试一次，返回相同的 500 行。

### 压缩属性读取与去重、聚合分页

在同一套 OrbStack PostgreSQL、Parquet 百万边夹具和 16 MiB 预算下，重测上述跳过 1,900 行、返回 500 行的请求。三次顺序请求的中位数从 18.15 秒降到 3.76 秒。选页列读取从 11.5 MiB 降到 5.0 MiB，回填读取了 1.5 MiB 的列页。包含身份与邻接在内的对象读取从 42.0 MiB 降到 9.2 MiB，查询缓存溢写从 17.2 MiB 降到 9.0 MiB。快照从 96.3 MiB 降到 77.0 MiB，刷新耗时 34.64 秒。纯 ID 选页三次请求的中位数从 2.68 秒降到 1.62 秒。

验证覆盖 `RETURN DISTINCT`、`WITH DISTINCT`、按关系身份分组求和、聚合后过滤、去重后再分组、按金额分组并计算 `count(DISTINCT e)`，以及 `LIMIT 0` 跳过对象读取。返回的关系 ID、属性、聚合值和行顺序均与单机逐行一致。部分完整关系分组的单机对照使用 768 MiB 和一个执行分区。原第 5 版快照的属性块回退也通过对照。16 MiB 下对含浮点、UTF-8 和时间戳返回列的聚合分页终止执行中的远端 Worker，同一快照代次重试一次，3.76 秒返回与单机一致的 500 行。

### 路径计数与身份解码

CSR 展开按物理计划请求的列生成 Arrow 批次。没有身份输出要求的扫描直接返回行数或跳数。对于需要关系或节点列表的查询，每批按身份块归并请求，每个去重后的身份只解码一次，再按原路径与元素顺序生成列表。首批最多 128 条，后续最多 512 条，并按路径 ID 的内存占用提前分批。

沿用同一代百万边快照和 16 MiB 预算。对同一起点的 2,901 条路径计数，三次 HTTP 请求中位数从 1.62 秒降到 0.12 秒，对象读取从 2.69 MiB 降到 0.19 MiB，查询缓存溢写从 2.50 MiB 降到零。按路径长度分组的中位数从 1.62 秒降到 1.00 秒，该计划仍构造路径结构，读取 2.69 MiB。两组结果均与单机一致。

带属性的深分页继续跳过 1,900 行、返回 500 行。完成构建和测试后，三次顺序请求中位数为 3.00 秒，上轮记录为 3.76 秒。两轮对象读取均为 9.2 MiB，查询缓存溢写均为 9.0 MiB。返回值和顺序逐行对照单机。

16 MiB 下实际终止一台正在执行的远端 Worker，带浮点、UTF-8 和时间戳输出的聚合分页在同一快照代次重试一次，返回与单机一致的 500 行。实库回归同时覆盖零跳的空关系列表、完整节点列表、变长路径与两种最短路。

## 第二阶段运行验证

在同一台 OrbStack ARM64 虚拟机启动 PostgreSQL、etcd、RustFS S3 和三台 Worker，沿用原 PostgreSQL 节点与 1,000,903 行有效边的 Parquet 夹具。对照单 Worker，SQL 和 Cypher 的计数、Decimal 求和、分组、排序分页、混合类型去重与空结果一致。

下表为 256 MiB、每组五次顺序 HTTP 请求的端到端中位数和 P95。20 次请求全部成功。同一虚拟机上的阶段调度与对象存储往返增加了延迟。独立主机扩容性能尚未测量。

| 查询 | 单 Worker 中位 / P95 | 三 Worker 中位 / P95 |
| --- | ---: | ---: |
| SQL 计数与求和 | 2.22 / 3.02 秒 | 9.70 / 9.86 秒 |
| Cypher 跨源 Join 与分组 | 2.07 / 2.23 秒 | 11.08 / 12.45 秒 |

32 MiB 下，跨源分组返回相同结果，累计写入约 37.2 MiB Shuffle 文件，Worker 算子溢写约 85.6 MiB。测试在远端分区执行中终止一台 Worker，重试一个分区后返回相同的行和 Parquet 版本。执行中替换 Parquet 返回 HTTP 409。另验证查询取消、1 毫秒超时、Gremlin 计数、图结果和 Arrow 下载。大查询运行时，接单槽仍可处理并发的标量请求。

## 第一阶段运行验证

OrbStack ARM64 使用 etcd 3.6、RustFS S3、三个 Worker 和原 PostgreSQL、Parquet 夹具。三路并发提交 60 次跨源点查，60 次成功，P95 101.55 毫秒，P99 101.96 毫秒，拒绝率 0%。这组数字反映同一虚拟机中的测试负载。独立主机的扩容曲线需要另测。

验证覆盖三种查询语言、500 行预览后的图续载、Arrow 领取、取消、1 毫秒超时、终止正在读取 PostgreSQL 的 Worker，以及继续向其他 Worker 派发。强制停止 Coordinator 后，已领取的查询继续执行。重启后取回原执行的预览、图批次和 Arrow。mTLS 代理拒绝无证书请求，Worker 拒绝无令牌请求。Chrome 登录后验证结果表、图续载、执行节点显示和 320 像素视口。

用本机 etcd v3 网关复跑集群进程回归。

```sh
GRAPHLAKE_TEST_ETCD=http://127.0.0.1:2379 \
  cargo test --locked --features admin --test admin cluster_queries_survive -- --ignored
```

## Coordinator 故障接管

两台 Coordinator 使用相同构建、相同集群名、同一套 etcd 和 S3 兼容存储。生产环境使用具备多数派容错的 etcd 部署。模型目录提供首次导入的 YAML、管理员密码哈希以及相对路径的数据文件。多个 Coordinator 可挂载同一 NFS 来源目录，源文件与 Worker 的读取路径必须可用。主权由 etcd 租约和随机任期标识决定。S3 保存不可变的模型目录快照，etcd 原子提交当前快照指针、账号授权、会话和写入回执。每次提交比较当前任期。旧进程失权后退出，HTTP 停止接单，已有 Bolt 与 Gremlin 连接关闭。

首次从旧版升级时，停止所有旧 Coordinator，保留模型目录和权限后再启动一台新版，等待导入完成，再启动备用。全量导入重试时会复用已经上传的内容对象。持续有对象上传完成时，导入可以继续，连续 60 秒没有对象完成时会超时。普通模型写入仍受请求期限约束。新建的草稿、发布、下线与授权此后从集群存储读取。宿主机改动旧 YAML 或旧账号文件只影响独立单机部署。灾备需一起保留 etcd 快照、对象存储的 `control/catalog/` 和 `control/catalog-blobs/` 前缀，以及原路径的密钥和数据源。回退到旧版前先从新版 API 取回需要的状态并停用新版入口，旧目录只保存迁移前的版本。集群模式的旧版磁盘会话在此次迁移后需重新登录。后续同一集群内的接管保留已提交的登录与退出状态。

```sh
graphlake-admin --catalog-dir /srv/graphlake/catalog --assets-dir /srv/graphlake/web \
  --cluster-config /srv/graphlake/cluster.yaml --public-url https://graphlake.example \
  --coordinator-failover --driver-model payments --bolt-port 7687
```

etcd 请求超时或暂不可达时会暂停接单，并在最近一次成功续租确立的本地安全窗口内重试。复核当前主权和续租成功后恢复接单。主权被更改、租约失效或本地安全窗口耗尽时关闭服务与驱动连接。共享配置目录不可用时也会退出。用 systemd 的 `Restart=on-failure` 重启为候选实例。相对路径使用共享挂载时设置 `RequiresMountsFor=/srv/graphlake/catalog`，并在各主机保留相同的管理员密码哈希。重置管理员密码前停掉所有 Coordinator，在各主机更新同一份密码哈希后再启动。 `/coordinator/active` 校验主权、Worker 可用性和配置对象存储，适用于入口选路。 `/readyz` 和模型 `/readiness` 提供进一步的诊断。

HA 模式的服务账号变更使用管理员会话调用 `PUT /api/v1/service-accounts`。正文为与 `service-accounts.json` 相同的完整 JSON，浏览器同源调用或管理脚本需带正确的 `Origin`。旧版 JSON 仅在首次启动时导入。轮换和撤销后已有旧凭据连接会被关闭。请确保 JSON 使用精确的账号和模型授权，空 JSON 对象会撤销全部服务账号。HA 操作审计由服务进程输出到系统日志，按 `graphlake-audit` 前缀收集。部署时为两台 Coordinator 配置持久化日志和集中收集，保留迁移前的磁盘审计。人工用户与角色使用同一个 etcd 仲裁独立提交，登出和轮换按账号版本校验。

### 冗余入口

在同一二层网络的两个入口节点部署安装包的 [HAProxy 配置](haproxy.cfg) 和 [Keepalived 配置](keepalived.conf)。两台入口安装同一证书，配置相同的回环转发端口。每台入口上的端口转发都应独立连接两台 Coordinator。

```sh
ssh -NT -o ExitOnForwardFailure=yes -o ServerAliveInterval=10 -o ServerAliveCountMax=2 \
  -L 127.0.0.1:17337:127.0.0.1:7337 -L 127.0.0.1:17687:127.0.0.1:7687 coordinator-a
ssh -NT -o ExitOnForwardFailure=yes -o ServerAliveInterval=10 -o ServerAliveCountMax=2 \
  -L 127.0.0.1:27337:127.0.0.1:7337 -L 127.0.0.1:27687:127.0.0.1:7687 coordinator-b
```

通过服务管理器监督端口转发并校验 SSH 主机密钥。将安装包的 `graphlake-ingress-ready` 安装到 `/usr/local/libexec`。在每台入口创建 root 所有、权限 `0600` 的 `/etc/graphlake/ingress.env`，设定唯一的 `GRAPHLAKE_INGRESS_NODE`、`GRAPHLAKE_CLUSTER_CONFIG` 和可选的 `GRAPHLAKE_ADMIN_BIN`。探针使用集群配置中的 etcd 连接和 TLS 文件。保持该文件及集群配置目录仅可由可信管理员修改。

Keepalived 探针要求本地 HTTPS、Bolt 转发均健康，并通过共享 etcd 为该入口取得或续期 6 秒租约。无法连通 etcd 时，健康探针失败并撤销 VIP。两个入口彼此隔离且都能访问 etcd 时，只允许一个入口通过探针。应使用默认 1 秒探测、连续两次失败撤销 VIP 的设置，避免旧入口的撤销时间超过租约。修改 VRRP 源地址、对端、网卡和虚拟 IP，让站点 DNS 指向 VIP。启用双向 IP 协议 112 并允许 ARP 更新。跨网段部署可使用具备健康检查和故障转移的托管负载均衡。若未启用 Bolt，同时移除 Bolt 前后端、入口健康检查中的 `/bolt` 路由及脚本中的 Bolt 检查。

Bolt 客户端使用 `bolt+s://graphlake.example:7687`，Gremlin 使用 `wss://graphlake.example/gremlin`。已提交的分布式 HTTP 查询可按查询 ID 恢复，Bolt 事务与 Gremlin 遍历需要重新建立连接。

### 管理写入与重试

创建、保存、发布、回退、下线和归档修复支持 `Idempotency-Key: <UUID>`。请求前生成并保留 UUID。普通响应提供 `Idempotency-Key` 和 `Location` 响应头；查询 `GET /api/v1/mutations/{id}` 可确认是否已提交及对应的草稿、生效版本。相同请求标识和内容会返回原结果，不同请求内容返回 `409 idempotency_conflict`。回执保留 24 小时。 `404 uncommitted` 表示查询时还未观察到提交，遇连接中断或超时后应使用原 UUID 重试。验证出错或失权时不会提交候选状态。对象存储不可用返回 `503` 或 `504`。HAProxy 不会自行重放请求。未提供 UUID 时由服务生成，若连接中断前未收到响应头则无法补取标识。管理页面在发送前保存原请求及标识，连接恢复后可查询回执或重试原请求。

S3 的 `control/catalog/` 保留不可变目录索引，`control/catalog-blobs/` 按内容摘要保存目录文件。目录提交只上传上一代没有引用的内容，写齐后由 etcd 更新生效指针。目录文件总量上限为 512 MiB，单文件上限为 8 MiB，索引上限为 32 MiB。Coordinator 按同一规则回收无人引用的目录索引，包括旧版内联清单。生效目录指针和未过期回执引用的索引受保护。内容摘要对象可被多个索引复用，仍由集群保留。新版本可读取旧的内联目录，旧版本无法读取新索引。切换前先升级备用 Coordinator 和备份工具，并保留切换前的便携检查点供回退。

S3 按前缀授权时，Coordinator 的回收任务需要列举并删除 `csr/` 和 `control/catalog/` 下的对象，以及列举、创建和删除 `retired/` 下的标记。上述前缀都位于集群命名空间下。

### S3 托管本地表

在启用 Coordinator 故障接管、已有 etcd 与 S3 兼容集群存储的部署中，可在集群配置启用托管本地表。候选 Coordinator 与 Worker 使用同一集群名和对象存储前缀，并可访问同一 etcd。Worker 的 `--source-dir` 仍用于普通文件源。只查询托管副本时，可以为它传一个节点私有的空目录。

```yaml
managed_local_tables: true
local_snapshot_retention_seconds: 86400
local_snapshot_gc_grace_seconds: 300
```

新建本地表返回稳定的 `local-table://<表 UUID>`，编辑器将它加入 Parquet 映射。旧文件路径可继续读取。迁移时用原始来源创建新的托管表，加载并检查后改用托管引用发布模型。切换期间，新旧版本各用自己的来源。

同步 Parquet 文件和派生 SQLite 键索引均使用 Coordinator 本机临时盘。可通过服务的 `TMPDIR` 指定本节点可写、容量充足的磁盘目录。共享 catalog 可继续使用 NFS。候选代次的清单和新增数据先上传，未变化的数据文件可被多代引用。上传完成后，单个 etcd 事务核对活动租约持有者、表记录版本和任务归属，并同时发布代次、水位和任务结果。新主可以读原代，并按已提交水位重试中断的同步。切主后的首次写入可能需要下载上一代并重建派生键索引。托管键索引随本次启动的同步工作区清理。

退役代次在独立的 etcd 键中保留，发布事务同时切换当前代次和登记旧代次。GC 按有界事务分批标记和回收。新版 Coordinator 会分批迁移旧记录中的内联历史。启用新格式前应升级所有 Coordinator 和备份工具。旧版拒绝读取已迁移的表状态，以防回收仍被历史代次引用的对象。需要回退时使用升级前的检查点。

Coordinator 在派发前固定输入并登记 etcd 租约。Worker 校验租约剩余时间、清单摘要和 Parquet 对象版本，按这些引用读取。仅依赖托管表的查询在 Worker 丢失时可用原代次重试一次。CSR 构建过程固定代次，完成后使用自身的 CSR 数据块，来源代次仍用于新鲜度校验。备份也登记独立租约，读取固定代次并验证文件 SHA-256。恢复到托管模式时，在提交控制状态前将副本重新上传到目标站的 S3 前缀，保留表 ID 和代次。

保留期从代次退出生效状态起算。达到保留期、且没有查询或备份持有租约的代次先标记，再等待回收宽限期。仍被其他代次引用的数据文件保留。失败上传遗留的完整对象在宽限期后回收。etcd 或必要清单不可读时，本轮回收停止。对象存储若启用版本管理，非当前对象版本和未完成的分段上传由桶的生命周期策略处理。不要对 GraphLake 活跃对象前缀设置按对象年龄删除的规则。

### 入口协议探测

HAProxy 对 HTTPS 后端检查 `/coordinator/active`，对 Bolt 后端通过实际转发端口发送 Bolt 5.4 握手。Coordinator 在回复版本前校验主权。仅 Bolt 转发中断时，入口的 `/bolt` 返回 503，Keepalived 可将 VIP 移至健康入口。Gremlin WebSocket 与 HTTPS 共用转发路径。验收时应分别中断 HTTPS 和 Bolt 转发，并使用真实 WebSocket 客户端校验入口的升级配置。

### 在线控制状态备份与恢复

手动备份目录最多接收 16 份备份。达到上限后，移走已有备份即可继续生成。

管理员可在「运行配置 → 备份与恢复」选择「立即备份」。在每台启用故障接管的 Coordinator 上设置 `GRAPHLAKE_MANUAL_BACKUP_DIR`，指向服务用户可写、权限为 `0700` 的现有私有绝对目录，例如 systemd 配置下的 `/var/lib/graphlake/manual-backups`。配置后重启服务。文件采用与命令行导出相同的格式和校验，页面报告服务器文件名和检查点时间。手动结果记录在同目录的 `manual-export-status.json`，与定时任务独立。将该目录纳入保留和异地同步策略。

导出时 Coordinator 可持续接单。命令在同一 etcd 事务读取模型指针、用户角色和服务账号，随后下载该代次的不可变归档。输出为经过校验的 `0600` 文件，包含草稿、历史、授权以及密码哈希，按凭据备份保管。安装随包提供的 `graphlake-backup.service` 与 `graphlake-backup.timer` 可每小时导出一次。配置独立目录同步与保留策略，将备份副本迁出 etcd 与对象存储所在故障域。监控进程根据导出和异地校验状态触发故障与过期告警。[生产监控与恢复检查](monitoring.md)

```sh
graphlake-admin --catalog-dir /var/lib/graphlake/catalog \
  --cluster-config /etc/graphlake/cluster.yaml backup export \
  --file /srv/graphlake-backups/control.json
```

有本地表时，`control.json` 包含 `state.local_tables`，其中记录备份标识、定义、刷新策略、历史及每个文件的 SHA-256。将控制 JSON 和其同级的 `local-<state.local_tables.id>` 目录作为一组同步和保留。复制期间已发布代次持续可读，定时、手动和 CLI 导出均读取已提交版本。派生键索引在恢复后的第一次需要时自动重建，可能读取完整键列。定时单元默认从 `/var/lib/graphlake/catalog` 读取，定制部署可用 `GRAPHLAKE_BACKUP_CATALOG_DIR` 指定与活动 Coordinator 一致的目录。导出已引用本地表的模型需要提供该目录。

恢复时为配置选择全新的集群名和 S3 前缀，准备 etcd、对象存储、Worker 和源路径。控制备份包含凭据哈希。异地传输后先对副本执行 `graphlake-admin backup verify --file control.json` 验证档案。

#### 绑定恢复点并开放入口

目标站可在 HTTPS 的 `/recovery` 从异地副本恢复。准备与主站隔离的目标集群名、S3 前缀、可写的共享来源目录。清单包含 PostgreSQL 时还需准备一个空数据库。所有候选 Coordinator 使用相同恢复配置，目标站的管理员密码单独初始化。安装能读取对应 PostgreSQL 自定义归档的 `pg_restore`。可通过 `GRAPHLAKE_RESTORE_PG_RESTORE` 指定客户端的绝对路径，恢复前会检查归档版本兼容性。

- `GRAPHLAKE_RESTORE_INBOX` 指向可信、只读的异地副本目录，可由异地对象存储同步或挂载。每个检查点用一个以字母或数字开头的子目录，目录名可含连字符和下划线。子目录根下放置 `control.json`、`manifest.json`，有本地表时放入同级 `local-<id>` 目录。外部 Parquet 沿用清单的相对路径，含 PostgreSQL 时放入 `postgres.dump`。各站点需保留这些只读副本供验收重验。
- 清单含 PostgreSQL 时，`GRAPHLAKE_RESTORE_POSTGRES_DSN_ENV` 填目标站 DSN 的环境变量名，须与清单的 `postgres.dsn_env` 一致。该变量在目标站解析到专用空库，使用单一 TCP 地址并明确给出数据库和用户。开启 TLS 时会验证主机名与 CA，可为 `pg_restore` 通过 `GRAPHLAKE_RESTORE_PG_CA_FILE` 指定 CA。
- `GRAPHLAKE_RECOVERY_TOKEN` 使用 `openssl rand -hex 32` 生成并在所有候选 Coordinator 上配置。部署随包 HAProxy 配置并启动 Worker、Coordinator。保持源数据冻结，在管理后台选择「运行配置 → 备份与恢复 → 恢复备份」，输入目标站 HTTPS 地址。也可以直接访问目标站 `/recovery`。在目标站输入恢复令牌，选择并检查恢复点。检查会读取控制备份和清单，显示检查点、表和文件数量。确认后，服务会再次核对清单摘要及数据归档，然后校验并写入外部 Parquet 和本地副本，有 PostgreSQL 时在单事务中恢复数据库，再提交控制状态。已安装的文件仅在摘要一致时复用。开始写入后重试须使用同一检查点。普通业务入口会保持关闭，直到验收通过。验收重查本地文件和状态摘要，并执行清单中的业务查询。恢复保留原刷新策略，定时同步和定时对账暂停，验收后在本地表页面保存刷新配置即可恢复。CLI 恢复中断后，目标命名空间只能用同一清单重试。页面还会检查同一 HTTPS 地址的业务入口是否就绪。

以下步骤适用于手工准备检查点和使用现有 CLI 恢复。UI 恢复使用步骤 1 生成的同格式检查点。

1. 在同一个业务检查点暂停控制写入、PostgreSQL 写入和 Parquet 替换。导出控制备份和 PostgreSQL 自定义格式归档 `pg_dump -Fc`，把归档命名为 `postgres.dump`。部署使用的 `pg_dump`、`pg_restore` 和目标数据库应选用相互兼容的版本。用下列命令取得源库每张恢复表的内容摘要。把控制备份中的 `sha256`、PostgreSQL 归档的 SHA-256、表摘要和每个 Parquet 文件的 SHA-256 写入 `manifest.json`。用生产业务查询记录期望结果和实际生效的模型版本。多行结果须使用确定性排序。归档的内容摘要标识这份逻辑恢复点。
2. 在异地使用 `pg_restore --single-transaction --no-owner --no-acl` 恢复到空数据库，将清单中的 Parquet 文件恢复到目录对应的相对路径。Coordinator 和 Worker 使用相同的恢复数据，优先使用共用的只读快照。恢复期间保持业务来源冻结，直到验收和切流完成。模型的来源环境变量应指向目标站。管理员凭据单独恢复或重新初始化。
3. 全部目标 Coordinator 停止时运行下方恢复命令。目标已有控制状态时拒绝导入。新实例统一用本版软件并设 `GRAPHLAKE_RECOVERY_DIR` 为下方清单目录。此时正常 HTTPS、Gremlin 和 Bolt 入口保持关闭，内部状态端口可用于监控。
4. 使用随包 HAProxy 配置。从验证客户端打开目标站 HTTPS 地址的 `/recovery`，输入此次恢复专用令牌。页面先校验清单、备份与 Parquet 摘要，之后可确认执行恢复验收。CLI 也可通过同一 HTTPS 入口发送 `POST /recovery/verify`。VIP 尚未启用时，可把生产域名临时解析到目标 HAProxy 的可达地址，保留生产 Host 和 TLS 校验。只有清单、数据摘要、生效模型和查询行逐项通过时，Coordinator 才会提交开放标记。同一 etcd 命名空间内接管的节点遵守同一门槛。失败时检查响应原因，修正目标数据后重试。清单本身错误时从经复核的新备份和新命名空间重做演练。页面通过同一个 HTTPS 地址检查业务入口的 `/readyz`。显示「允许切流」时，验收已通过且业务入口已就绪。可重新读取状态核对，之后清除操作端保存的专用令牌。

```sh
graphlake-admin backup fingerprint-postgres --dsn-env LEDGER_DSN \
  --relation ledger.accounts --relation ledger.merchants
# 在备份目录中保存 manifest.json 和 postgres.dump
export GRAPHLAKE_RECOVERY_TOKEN="$(openssl rand -hex 32)"
graphlake-admin --catalog-dir /var/lib/graphlake/catalog \
  --cluster-config /etc/graphlake/recovery.yaml backup restore \
  --file /srv/graphlake-restore/control.json --recovery-dir /srv/graphlake-restore
# curl 从 stdin 读令牌，避免把它放到进程参数
printf 'X-GraphLake-Recovery-Token: %s\n' "$GRAPHLAKE_RECOVERY_TOKEN" | \
  curl --fail-with-body --max-time 270 --connect-to graphlake.example:443:RECOVERY_HOST:443 \
    -X POST -H @- https://graphlake.example/recovery/verify
```

`manifest.json` 示例中的摘要应替换为 64 位小写十六进制值，模型版本直接使用管理 API 返回的 `active_revision`。清单包含至少一条实际业务查询及其完整结果。按需提供 `postgres` 和外部 `parquet`。只使用本地副本的模型可省略这两项，本地副本文件清单已经在控制 JSON 内。结果单元格填写 API 返回的文本或 `null`。

```json
{
  "format": 1,
  "control_sha256": "<control.json 中的 sha256>",
  "postgres": {
    "dump_sha256": "<postgres.dump 的 sha256>",
    "dsn_env": "LEDGER_DSN",
    "relations": {
      "ledger.accounts": "<accounts 摘要>",
      "ledger.merchants": "<merchants 摘要>"
    }
  },
  "parquet": { "payments.parquet": "<payments.parquet 的 sha256>" },
  "canaries": [{
    "model": "ledger",
    "revision": "<生效版本>",
    "language": "cypher",
    "query": "MATCH (:Account)-[:Payment]->(:Merchant) RETURN count(*) AS edges",
    "expected_rows": [["1000903"]]
  }]
}
```

每次恢复会停用用户及角色内服务账号，移除角色成员，并清空旧服务账号策略和会话。通过恢复验收后，用当前身份目录和授权记录重新分配角色和签发服务令牌。应用客户端切流前，再验证旧账号、旧 Cookie 被拒绝，以及新账号实际可用。目标入口运行遥测探针，分别记录控制版本、源恢复点以及验收通过时间。TLS 材料、源凭据和集群基础设施由部署侧分别恢复。

升级前在线导出并保存原安装包。回退时使用升级前的备份与对应版本，保持流量隔离，复核源数据和权限后再开放。支持恢复清单的目标实例须统一使用当前发行包。

验收页在目标站业务门禁关闭时也可访问。令牌只上传至目标站，同源页在内存中保留令牌直至关闭或刷新。校验恢复清单只读取文件与摘要，执行验收才复核已恢复的 PostgreSQL 数据、运行真实业务查询并提交切流门禁。目标站的恢复目录、挂载、凭据与空 PostgreSQL 数据库由部署侧准备。操作员可通过恢复页导入控制备份并恢复 PostgreSQL 和 Parquet 数据。

### 混合版本维护窗口

先逐台升级 Worker，并检查已有 Coordinator 的查询及恢复入口。旧 Worker 会拒绝包含新租户范围或禁用 Arrow 捕获字段的请求，因此启用这些权限前须完成 Worker 升级。随后从备用 Coordinator 开始升级，停止旧主后等待新主取得主权和通过三种协议检查，再升级另一台。维护窗口内暂停新角色字段及企业登录配置的变更。

需要退回旧版时，先切出流量并冻结权限写入。旧版不理解租户范围、独立导出权限和企业代理身份，必须使用权限升级前的归档、全新集群名及存储前缀。核对授权后再切入口；已有 Bolt 和 Gremlin 会话重新连接。
