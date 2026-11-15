# GraphLake 技术参考

GraphLake 是单进程、只读的 Rust 图分析引擎。Rust API、CLI、Python 绑定和 C ABI 共享同一套引擎。引擎支持关系库、文档库、文件、数据湖与数仓的直接读取，包括 SQL Server、Oracle、MongoDB、DuckDB、Doris、StarRocks 和 Databricks 等扩展来源；也可通过 Trino Catalog 读取 SQL Server、Oracle、Hive、Hudi、MongoDB、Elasticsearch 和 Redshift。各连接器的配置、运行依赖和读取边界见下文。Cypher 和只读 Gremlin 通过 DataFusion 执行关联、过滤和聚合。有界遍历可以从索引批量查边、扫描构建 CSR，或读取已发布的 CSR 代次。图投影支持多种节点与边标签，可运行 PageRank、弱连通分量、加权路径、Louvain、Leiden 和 Node2Vec。

## 本地运行

使用 Rust 1.95 或更新版本。示例在新目录生成两份 Parquet 和一份图映射。

```sh
cargo run --locked --example demo -- /tmp/graphlake-demo

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml --diagnostics query \
  'MATCH (a:Account)-[:Transfer]->(b:Account)-[t:Transfer]->(c:Account)
   WHERE a.tenant_id = 1 AND a.account_id = 10
   RETURN c.account_id AS account, count(*) AS paths, sum(t.amount) AS amount'

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml query --language sql \
  'SELECT tenant_id, count(*) AS transfers FROM "Transfer" GROUP BY tenant_id'

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml traverse \
  --edge Transfer --start 1 --start 10 --max-hops 3 shortest --target 1 --target 30
```

两跳查询返回 `{"account":30,"paths":2,"amount":600}`。最短路径返回终点、跳数和有序的边业务键。默认输出 JSON Lines，`--format arrow` 输出 Arrow IPC 流。浮点 `Infinity`、`-Infinity` 和 `NaN` 在 JSON Lines 中以字符串输出，在 CSV 中以对应文本输出。Arrow 保留原浮点值和空值。`--diagnostics` 将物理计划、各来源版本和运行统计写入 stderr。

## 本机管理后台

后台按目录管理图模型。使用 Node.js 24 构建页面，再启动可选的 Rust 服务。可将目录指向已有模型，也可以先生成本地 Parquet 示例。

```sh
npm --prefix web ci
npm --prefix web run build
cargo run --locked --example demo -- /tmp/graphlake-admin-demo
cargo run --locked --features admin --bin graphlake-admin -- \
  --catalog-dir /tmp/graphlake-admin-demo auth init
cargo run --locked --features admin --bin graphlake-admin -- \
  --catalog-dir /tmp/graphlake-admin-demo --assets-dir web/dist
```

管理后台支持自定义角色及成员管理。角色按模型授予查询、导出、建模、发布和只读审计权限。参见 [用户与角色](../deploy/README.md#用户与角色) 和 [集群灾备](../deploy/cluster.md#在线控制状态备份与恢复)。

图模型列表支持按名称或标识搜索，以及「全部」「已发布」「未发布」「需处理」筛选。「发布状态」与「草稿校验」分别显示；草稿校验失败不代表生效版本已下线。「需处理」包含草稿校验失败和版本存储异常的模型，可与发布状态重叠。具有查询权限的已发布模型提供直接「查询」入口；版本存储异常时不显示该入口。无匹配结果时可「清除筛选」返回完整列表，列表旁可刷新模型。窄屏下列表切换为保留状态、统计及操作的卡片，导航在空间不足时可横向滚动。 数据源、节点和边数量合并在「模型结构」列；完整发布版本仍可在模型及版本历史中查看。界面统一采用浅色导航与低对比度分隔线，主要操作使用蓝色强调。

在当前页面会话内，从列表打开模型后返回，会恢复该工作区的搜索、筛选和滚动位置；图模型、数据源、查询列表各自保留状态。新建或尚未发布的模型显示「添加数据源 → 定义节点与边 → 保存草稿 → 校验并发布」指引，缺少来源表、节点业务键或边端点时提示下一步，并可直接打开对应配置。边是可选项，只有节点的模型也可以发布和查询；结构有效的离线来源草稿仍可保存。

在建模画布可以拖动节点，已有边和标签会随节点更新。聚焦节点后也可用方向键移动，或选择「移动选中节点」后点击画布空白处。手动位置保留在当前模型编辑会话中，可用「自动排列节点」恢复当前布局。点击「连线建边」后依次选择起点和终点，或点击节点旁的「＋」直接从该节点开始。画布预览方向，允许连接到同一节点；Esc 可取消。右侧面板确认边名和端点后将边加入草稿，再配置来源表与端点字段并保存。

管理员可从主导航的「Overview」查看平台概览。页面显示当前服务入口的就绪状态和版本，以及当前进程的运行时间、查询并发与排队、HTTP 5xx 累计次数、遥测上报及备份记录。「需关注」汇总已知的运行异常和待生效配置，并可跳到对应详情。概览可见时每 30 秒采集一次，也可手动刷新。

「运行趋势」支持查看最近 5 或 15 分钟的查询负载与 HTTP 请求速率，也可从「查看数据」打开右侧面板读取采样表。速率根据相邻两次采集的计数差与实际间隔计算，包含管理 API 请求。两次采集超过 60 秒、计数重置或采集失败时，趋势图显示缺口。切换到其他工作区时暂停采集，返回后保留近期样本和已完成的模型检查。模型列表短暂不可用时会显示读取失败，恢复后继续展示生效版本相同的检查结果。刷新浏览器页面后重新采集。备份状态取自最近任务和数据检查点，长期趋势与生产告警由部署侧监控提供。

在 Overview、模型目录的「运行健康」或「运行配置 → 健康检查」中，可逐个或依次检查全部已发布模型、停止检查和只看需处理。每个结果显示检查时间、来源可用数量、服务返回的 Worker 状态及异常位置。来源异常可直接进入数据源配置，映射异常可打开模型修复。结果是按需检查的快照，生效版本变化后旧结果不再显示。未检查、检查失败与运行正常分别呈现。来源明细可展开查看，运行检查与草稿校验状态分开。

版本历史的「查看并对照」先列出数据源、节点、边及模型字段的新增、删除和修改摘要，再显示带行号与增删标记的 YAML 差异。默认只展示修改行，可展开未修改行或完整 YAML 对照；仅格式和注释变化会单独说明。差异过大时保留摘要和完整 YAML，不阻塞编辑器。

查询编辑器提供 Cypher、SQL、Gremlin 语法高亮、行号和括号匹配，按生效模型补全节点标签、边类型、业务键和属性。点击「补全」或按 Ctrl+Space 打开候选，方向键选择、Enter 插入、Esc 关闭；⌘/Ctrl+Enter 运行，Tab 移到下一个控件。三种语言各自保留语句，切换工作区不会丢失查询和结果。

新建与重命名、节点/边列表、来源浏览、查询范围、模型 YAML、版本历史以及成员/角色编辑使用右侧滑出面板。标题和底部操作区固定，中间内容独立滚动；短表单、范围配置和宽内容分别使用适合的宽度，手机上按单列显示。来源浏览按「选择来源 → 字段预览 → 使用此表」组织，并展示字段类型和可空性。版本历史左侧选择发布记录、右侧显示变更，检查来源与下线操作收纳在「生效版本操作」中。健康页优先显示模型状态，上报状态作为辅助区域。删除、下线、退出和灾备执行等简短确认继续居中，并保留安全操作的默认焦点。侧滑打开时使用短动画，遵循系统减少动态效果偏好；关闭后焦点回到入口。

侧边栏独立的「AI」页面默认展示 Agent 接入指南，用户可生成、轮换或撤销个人 AI 令牌，查看自身可用模型并测试连接。AI 继承令牌所属用户当前的查询、建模、发布权限及租户范围，审计记录归属实际用户。查询与建模各有一个 MCP 地址（`/mcp/query` 和 `/mcp/modeling`），管理员在「运行配置 → MCP 服务」中独立启停。详见 [MCP 接入与能力边界](mcp.md)。

后台和 Worker 提供 Prometheus 指标。「运行配置」包括日志与遥测、健康检查、备份与恢复及 MCP 服务。管理员可编辑日志和 OpenTelemetry 配置、检查发布模型的数据源、查看最近备份状态和配置 MCP 服务。从「备份与恢复 → 恢复备份」进入目标站 HTTPS 恢复页，可选择并检查已同步的恢复点，确认恢复后查看各阶段进度、执行验收并核对切流状态。[恢复操作](../deploy/cluster.md#绑定恢复点并开放入口) · [OpenTelemetry 配置](../deploy/README.md#opentelemetry-上报)

启用 Coordinator 故障接管并配置私有目录后，管理员可在「备份与恢复」选择「立即备份」，在服务器生成已校验的控制状态文件。PostgreSQL 与 Parquet 源数据另行备份。[配置手动备份](../deploy/cluster.md#在线控制状态备份与恢复)

在浏览器打开 [本机管理后台](http://127.0.0.1:7337/)。使用 `--port` 可指定端口，`--timeout-ms` 可将默认 60 秒的执行期限调短。服务绑定 IPv4 回环地址，页面和 API 共用同一个来源。模型文件命名为 `<标识>.yaml`，标识以 ASCII 字母或数字开头，可包含下划线和连字符，长度最多 64。模型目录中的文件继续使用原有 YAML 格式和相对路径规则。启动服务前，在同一进程环境中设置 Catalog 引用的凭据变量。

认证默认开启。已有模型目录升级时，先停止后台，用该目录执行 `auth init`，再启动并以 `admin` 登录。初次设置和重置均在终端隐藏输入并确认 15 至 128 个字符的密码。重置时先停止后台，再执行 `graphlake-admin --catalog-dir <目录> auth reset-password`。密码哈希保存在该目录的 `.graphlake-auth/`，使用随机盐和 Argon2id。Unix 上目录权限为 `0700`，凭据和锁文件为 `0600`。同一模型目录同时只允许一个后台或凭据命令访问。`--insecure-dev` 仅供无管理员凭据的回环测试目录使用。图查询 CLI 和语言绑定仍可直接使用模型 YAML。

后台用于受信任主机上的回环 HTTP。后台和凭据命令应在同一操作系统环境运行。使用 OrbStack 共享目录时，请在同一个 Linux 实例里启动和管理凭据，macOS 和虚拟机的文件锁各自生效。会话 Cookie 使用 `HttpOnly`、`SameSite=Strict`，名称包含监听端口。会话闲置 30 分钟、建立满 8 小时、退出或服务重启后失效。会话过期会锁定当前页面，重新登录后可继续编辑留在页面中的草稿，并手动重试失败的操作。退出登录时会提示确认放弃未保存的草稿，并清除页面中的模型和结果。构建产物放在独立的 `--assets-dir` 中，服务只读取入口页和 `assets/` 下的构建资源。

服务提供 `/healthz`、`/readyz` 和 Prometheus `/metrics`。用 `--driver-model <已发布模型>` 可为现有 TinkerPop 客户端开启只读 Gremlin WebSocket；另加 `--bolt-port 7687` 可为 Neo4j 驱动开启只读 Bolt。HTTP、Bolt 和 Gremlin 共用模型授权，独立服务账号通过 `.graphlake-auth/service-accounts.json` 配置，操作审计写入同目录。驱动与 HTTP 查询共享 `--query-slots`（默认 1），过载时进入有界队列。Bolt 支持托管只读事务，两种驱动均提供原生图对象和分批结果。单机结果快照每个身份最多保留 4 个，服务合计最多 32 个、1 GiB。Arrow 下载从开始发送起最多占用传输槽 60 秒。连接示例、协议范围、Linux 打包、systemd 单元和升级恢复步骤见 [单机部署手册](../deploy/README.md)。

桌面从左侧、窄屏从顶部导航切换图模型、数据源和查询。当前模型可在工作区标题栏切换。模型列表在较窄容器中按条目排布，数据源卡片随可用宽度自动换列。图模型列表支持搜索、新建和从 YAML 文件导入草稿。可在编辑器下载当前草稿，也可打开版本历史并排查看归档 YAML 和草稿、下载归档 YAML。导入文件里的相对路径以后台的模型目录解析。打开模型后可在画布选择节点或边。配置面板在桌面位于画布右侧，在窄屏位于画布下方。画布上方保留添加、保存、发布及带数量的节点和边入口。可直接从画布连接端点建边，手动校验、版本历史和 YAML 位于「更多工具」。数据源工作区以卡片展示当前模型已配置的数据源；点击卡片的「配置」打开独立弹窗，可就地保存草稿并查看保存结果；关闭弹窗会保留未保存的编辑，点击「浏览表与字段」读取来源结构，选表后可将来源带入映射。已配置的数据源支持按名称或连接器类型搜索。下方的连接器卡片支持按数据库、文件与数据湖、数仓与分析、通过 Trino 分类筛选，并可组合关键词搜索，显示匹配数量；无匹配时可清除筛选。点击「配置」即可在弹窗填写数据源名称和连接参数，或添加文件表映射与列类型，完成后「添加到草稿」。新建连接的必填项会在提交时校验并聚焦缺失字段；文件路径和列名等已有输入也参与必填校验。取消新建不会修改模型。随后保存草稿、校验并发布，配置才会进入生效版本。PostgreSQL、MySQL、ClickHouse、Trino 及其连接器和 Iceberg 支持逐级浏览，Parquet、CSV、JSON Lines 和 Delta 浏览已配置的表，SQLite、Spanner 可列出数据库内的表。Snowflake、BigQuery 和 SAP HANA 可按已映射或手工输入的限定模式列出表。也可输入完整表名直接读取字段。节点和边可配置多个来源表、逻辑字段到物理列的映射、类型转换、行过滤、优先顺序与重复键策略。当前连接器均可从数据源页配置，凭据填写服务进程的环境变量名。可配置 Delta 表版本、Iceberg 表快照及连接器的高级字段。重命名数据源会更新单表和多表引用，删除前需解除引用。节点和边可在「时态与分区」设置 SCD2 和局部跳过逻辑分区。模型 YAML 仍可查看、导入和导出。

数据源卡片显示连接器的 SVG 标识。已配置的对象存储卡片与存储服务选择器会显示当前供应商的标识。

选择来源表后，单表映射的业务键、属性和边端点可从带类型的来源列中选择。多表映射保留公共逻辑字段，每个来源可独立选择物理列和过滤列。模型工作区在切换标签或页签时保留最近读取的来源列，修改来源配置后不使用旧列提示。重新选表时保留已有字段，未在新表中找到的列会标明。可在配置面板删除边类型；删除节点标签时，确认框列出将同时移除的关联边。图模型须保留至少一个节点标签，删除先更新草稿，发布后才影响查询。

「保存草稿」检测磁盘版本并原子替换 `<标识>.yaml`。数据源离线时仍可保存结构有效的草稿。「校验结构」检查映射列、业务键类型和端点类型。「校验数据」读取当前逻辑图，检查空业务键、违反映射规则的重复键和孤立边。对 SCD2 检查当前生效记录。对于能定位的失败，点击「定位」可打开对应标签、展开来源映射并聚焦出错字段；修改草稿后需重新校验。「校验并发布」及回退都通过数据校验后才切换生效版本，失败时保留当前生效版本。既有有效 YAML 在启动时归档为初始版本；运行期间新放入的有效 YAML 会在首次执行或保存时归档。发布和回退独立于工作草稿。备份模型时应一同保留目录下的 `.graphlake-*` 版本文件。

查询页支持 Cypher、SQL 和 Gremlin。每次执行绑定已发布的内容版本，结果显示实际版本、耗时与来源版本；页面持有过期版本时，服务要求重新加载。查询可取消，可为单次请求覆盖超时（1–60000 毫秒）；普通查询、邻居探索与模型校验使用独立执行槽。浏览器按模型保留最近 20 条查询的语句、版本、范围和状态。当前页面会话内可从历史恢复 Cypher 参数，刷新后提示重新填写。查询可另存为模板，参数值仅在显式勾选时存入浏览器。载入历史或模板后，焦点返回查询编辑区；需要补填参数时自动展开选项并聚焦参数输入。三种语言均可在「查询范围」用表单填写业务时间、逻辑分区和一致性要求，也可编辑等效的 JSON。Cypher 参数、查询超时、完整结果保留和 CSR 选择集中在「查询选项」中，默认收起；已填写选项时入口显示「已设置」，参数缺失或超时无效时自动展开。执行详情包含完整版本标识、来源版本和可用的执行节点信息。预览上限为 500 行、1 MiB；结果区包含图、表格、图结构和原始响应四个页签。Cypher 和 Gremlin 返回的节点、边和路径可汇到同一张图，按标签和完整业务键合并。可点击查看属性，从节点展开一跳入边和出边，拖动画布或节点，滚轮或按钮缩放，并切换径向、分层和力导向布局。展开可按方向、边类型和映射属性筛选。有业务键的边按标签和完整业务键排序，每页最多 50 条。无业务键的边在本次展开的分页结果中具有稳定临时标识，可以逐页浏览，包括属性相同的平行边。画布最多显示 3,000 个图元素，支持连续加载剩余邻居并随时停止。可按业务键、标签、属性值或 `属性=值` 搜索画布。展开邻居后可搜索最近展开的邻域及其全部批次，点击未加载的匹配项会跳到对应批次并定位，包括平行边。可返回原画布继续探索。「运行并追加到画布」合并同一模型版本、查询范围和兼容来源版本的查询结果。匹配列表按可见区域渲染。节点超过 400 个时，力导向布局改用径向布局。空间不足时暂存当前页中尚未加入的边及端点。移除节点会同时移除它的关联边，聚焦子图会保留所选节点及已显示的一跳邻域。腾出空间后可以继续加入暂存的元素，也可以切换到当前批次，仅保留起点和该批邻域，随后前后翻页。Gremlin 路径中未返回边的相邻节点以虚线相连。属性补读和邻居展开沿用原查询的生效模型版本和图过滤条件。读取到同一来源的可比较版本发生变化时，页面暂停探索，可用原语句、参数、超时和模型版本重跑并重建画布。图结构显示生效版本的节点、边和端点映射。原始响应显示管理 API 返回的 JSON 预览。

普通运行完成后，表格、原始响应和图均切换到该次执行；没有图元素时图页为空。「运行并追加到画布」会保留画布及其起始执行标识，表格和原始响应仍显示最近一次执行。打开属性面板或切换结果页签时保留当前缩放与平移；从邻居批次返回原画布时同时恢复原搜索结果。「图例与标签」可为每种节点和边设置显示属性，默认从常见名称属性取值，为空时显示类型名。可按类型隐藏节点和边；隐藏节点也会隐藏其相连的边。当前画布的类型显隐也用于跨批搜索，翻回已浏览的邻居批次会恢复上次选择。放大大图时，边标签按视口空间显示，选中的边独立高亮。浏览器在同一页面会话内保留最近五个模型的未执行语句、查询结果和画布状态。有未保存模型草稿的工作区继续保留。

「下载结果 Arrow」读取执行时保存的类型化结果。截断时默认可下载已显示的预览行，运行前勾选「保留本次执行的完整结果」可下载同次执行的全部结果。服务最多保留 4 份结果、合计 1 GiB，最多 30 分钟。「重新执行并导出」使用结果对应的语句、参数、范围和模型版本重新读取当前数据源。导出受查询超时、内存与溢写预算约束。未领取的导出文件最多保留 4 份、合计 1 GiB，票据最多有效 60 秒。网页不能指定服务端导出位置。Cypher 和 Gremlin 的图预览截断后可加载同一次执行的后续图批次，读取时沿用执行时的模型版本和图属性。达到 3,000 元素的画布容量时可切换到下一批。图快照最多采集 12,000 个元素，采集字节上限为 8 MiB 和查询内存预算八分之一中的较小值，并受结果保留期限约束。

本地表可从外部源表加载选定字段，支持按主键和增量字段更新、定时刷新、后台任务与加载历史。入口为「图模型 → 本地表」或「数据源 → 本地表与同步」，加载成功后可将节点和边映射切换到副本。参见[本地表与数据同步](local-tables.zh-CN.md)。

管理 API 位于 `/api/v1`。`GET /auth/session` 返回 `authenticated`、`enabled` 和 `username`。`POST /auth/login` 接收 JSON `username` 和 `password` 并设置会话 Cookie。登录失败返回 `401 invalid_credentials`，一分钟内累计五次失败后返回 `429` 和 `Retry-After`。`POST /auth/logout` 幂等撤销当前会话。其他管理请求（包括 `HEAD` 和结果下载）携带会话 Cookie，未登录或会话失效返回 `401 auth_required`。管理响应使用 `Cache-Control: no-store`。`Host` 必须匹配启动地址，写入和执行请求（包括登录、退出）需要同源 `Origin`。

模型使用 `GET/POST /models` 和 `GET/PUT /models/{id}`；更新草稿时提交原草稿的 `ETag` 作为 `If-Match`。模型响应分别返回草稿 `revision` 与 `active_revision`。草稿校验用 `POST /models/validate`，传 `check_data: true` 执行数据校验，表结构浏览用 `POST /models/{id}/browse`（草稿 `yaml`、`source` 和可选 `schema`、`table`）。可定位的校验失败带可选的 `location`：`table` 给出来源表及可选物理列，`field` 给出标签、字段及从零开始的可选序号，`input_column` 给出标签、从零开始的来源序号和逻辑字段。查询页使用 `GET /models/{id}/schema/{revision}` 读取当前生效版本的节点和边结构，无需读取来源数据。读取版本历史用 `GET /models/{id}/history`，读取某个历史版本的 YAML 用 `GET /models/{id}/history/{revision}`。发布和回退用 `POST /models/{id}/publish`、`POST /models/{id}/rollback`，提交目标 `revision` 和页面看到的 `active_revision`。

`POST /models/{id}/query` 和 `POST /models/{id}/export` 均要求 UUID `request_id`、`model_revision`、`language`、`query`，接受可选 `timeout_ms`、`graph_options`（JSON 对象或 JSON 文本）与仅供 Cypher 使用的 `parameters_json`。查询传 `capture_result: true` 可保留完整结果。响应的 `snapshot` 可用于 `GET /models/{id}/results/{snapshot}` 下载，`snapshot_complete` 表示是否包含本次执行的全部行。查询预览的 `graph` 包含图元素及仅用于显示遍历的 `steps`。传 `capture_graph: true` 可为图预览保留后续批次。响应包含 `graph_snapshot` 和从零开始的 `graph_next` 时，执行者可通过 `GET /models/{id}/results/{graph_snapshot}/graph/{graph_next}` 读取一批，返回 `graph`、`next`、`incomplete` 和 `model_revision`。超出保留期限或被淘汰时返回 `404 result_expired`。通过 `POST /models/{id}/explore` 可读取属性或展开一跳邻居，提交 `model_revision`、元素的 `kind`、`label` 和字符串数组 `key`，以及 `neighbors`、原查询的 `graph_options` 与可选的 `timeout_ms`。邻居展开还接受 `filter`，包含 `direction`（`both`、`in`、`out`）、`edge_label`，以及最多八条使用所选边类型映射属性的 `properties` 比较条件。比较值按映射列类型解析，精确数值可传字符串。按原请求回传 `next_cursor`、`previous_cursor` 或 `page_cursor` 作为 `cursor`，分别读取下一页、上一页或当前页。邻域按页写入临时文件，首批写完即可返回，其余页面在查询预算内继续生成，后台可同时生成两个新邻域；冷游标恢复使用独立的两个执行槽。每个邻域最多占用 64 MiB 临时文件。服务保留 8 个活跃邻域，文件创建 15 分钟后或被淘汰时，已完成的分页文件可进入磁盘缓存。磁盘缓存最多保留 32 个邻域、合计 256 MiB，最多保留到文件创建后 1 小时。缓存命中时验证来源版本后直接读取目标页。缓存未命中时，只要服务未重启且来源版本可验证并且未变，签名游标记录页首位置，可跳过更早的边类型，并在可按键定位时先按键过滤再做多表去重。无业务键或不能按键定位时按稳定排序和序号跳到目标页，不生成之前的页面；平行边保留原临时标识。游标保留最近的若干页锚点供后退，锚点超出大小预算或较早的页未记录时，按可用位置恢复。两级缓存都未命中时，读取量仍取决于来源排序、索引和 Parquet 行组统计。回传同一邻域的任意有效游标并附带 `search: {query, offset, hidden_nodes, hidden_edges}` 可以搜索全部批次，`offset` 和隐藏类型数组可省略。过滤端点或边类型后，每组最多返回 100 个去重结果，并包含 `total` 与每项所在页及其游标。独立的有界搜索索引可在分页缓存淘汰后复用，并在复用前校验来源版本。不支持版本校验的来源无法安全重建时返回 `neighbor_session_expired`，可重跑原查询。可发送 `source_versions`（来源标识到版本的映射）拒绝已变更的可比较来源版本，游标也会校验这些版本。版本不一致时返回 HTTP 409 和 `graph_sources_changed`。版本检查覆盖本次读取与历史记录重合的 PostgreSQL、Parquet、Delta 和 Iceberg 来源。PostgreSQL 在只读快照内计算关系内容指纹。主库普通表、分区表和物化视图在事务可见性不变且无行级安全策略时，续页跳过内容扫描；快照变化时重新计算指纹，无关表写入不会使游标失效。视图、外部表、启用行级安全的表和备库保留内容校验。需要在持续写入的主库跳过整表校验时，由管理员运行 [表版本安装脚本](../examples/postgres-relation-versions.sql)，对参与探索的永久独立表执行 `SELECT graphlake_tracking.watch('schema.table');`，并按脚本给 GraphLake 账号授予两个版本表的只读权限。旧安装重跑脚本即可迁移既有触发器，再为读取账号补授新表权限。触发器覆盖 INSERT、UPDATE、DELETE 和 TRUNCATE；每个事务为每张表追加独立记录，已提交记录在同一只读快照内参与版本计数，回滚不计入。对繁忙的表定期在独立事务中运行 `SELECT graphlake_tracking.compact('schema.table');`，将已提交记录折入基准计数并控制日志大小，游标版本不变。对表做绕过触发器的维护后，应重新执行 `watch`。GraphLake 不会自行修改源库。Parquet 续页只检查对象版本。导出完成后返回短期单次下载票据，通过 `GET /downloads/{ticket}` 下载。取消执行使用 `DELETE /queries/{request_id}`。所有管理请求校验 Host，修改与执行类请求还需匹配 Origin。

版本历史提供「下线模型」「检查生效版本数据源」和归档恢复。管理员可通过 `POST /models/{id}/unpublish` 提交当前 `active_revision` 下线，保留草稿与历史，可回退或重新发布。下线模型拒绝新的查询、导出和探索。活动归档损坏时可回退到健康版本，或用哈希一致的原始 YAML 恢复。授权客户端可用 `GET /models/{id}/readiness` 检查当前生效版本的映射、跨源端点类型和各 Worker 的来源可用性。参见[部署手册](../deploy/README.md#运行检查)。

参见[图模型列表稿](../web/design/model-list.png)、[图模型编辑器稿](../web/design/model-editor.png)和[实库与 Chrome 验证记录](../web/design/verification.md)。

集群模式可将管理 API 的查询和 Arrow 导出分发到多个 Worker，也可配置单条查询跨 Worker 扫描 Parquet、执行跨源 Join 和聚合。发布分区 CSR 后，普通 Cypher 查询可通过 `csr_generation` 运行变长路径和无权最短路。第 5 版快照压缩身份块和属性页。`UNWIND relationships(p)` 后可按关系 ID 和多个属性键选页，再批量读取页内边的属性。含 `DISTINCT` 或聚合时，先读取表达式依赖的属性列、完成去重或分组，再为页内仍可按身份定位的边回填属性。Worker 可按列读取关系属性来过滤邻接候选。PageRank 和弱连通分量的顶点状态按分区由 Worker 计算。查询结果可跨 Coordinator 重启取回。参见[多 Worker 部署手册](../deploy/cluster.md)。

集群查询页可在 Cypher 编辑器选择「使用已发布 CSR」。结果显示实际执行的代次，属性查看、邻居展开、翻页和搜索均读取同一代次。刷新页面或连接中断后，可用「找回原查询」读取原任务状态和结果。浏览器会话存储保留待找回的请求标识和模型版本，已执行语句及参数仍按现有编辑器和历史规则处理。

## 图映射与连接器

### 扩展数据库与对象存储

数据源卡片包含直连与 Trino 两种入口。旧的 `sqlserver`、`oracle`、`mongodb` 配置仍通过 Trino；新增直连类型分别为 `sqlserver_native`、`oracle_native`、`mongodb_native`，不会自动改变已有连接方式。完整配置示例见 [extended-sources.yaml](../examples/extended-sources.yaml)。

| 类型 | 配置与读取边界 |
| --- | --- |
| `doris` / `starrocks` | MySQL 协议；配置 `dsn_env`、`tls`、可选 `ca_file`。按语句读取，不执行 InnoDB 快照初始化。支持普通标量类型和不超过 38 位的 Decimal；复杂类型需要远端视图转换。 |
| `tidb` / `oceanbase` | 同样使用 MySQL 连接字段；OceanBase 仅支持 MySQL 模式租户。按语句读取，不声明跨表一致快照。 |
| `mariadb` | 使用 MySQL 连接字段；InnoDB 只读重复读事务，使用 MariaDB 的 `max_statement_time`。 |
| `sqlserver_native` | `host`、`port`、`database`、`user`、`password_env`，TLS 默认开启，可配置 `ca_file`。通过 TDS 直接执行生成的 SELECT；按语句读取，不声明跨表同一快照。读取账号应只授予 SELECT 和所需元数据权限。 |
| `oracle_native` | `connect_string`、`user`、`password_env`。使用 Easy Connect 或 TNS 名称；GraphLake 进程所在机器需安装 Oracle Instant Client，库搜索路径需能找到客户端库。使用只读事务，`NUMBER(p,s)` 在可表达时映射 Decimal128，无固定精度或负标度的 NUMBER 保留为文本以避免精度丢失。Oracle DATE 保留时分秒。 |
| `mongodb_native` | `dsn_env` 保存 `mongodb://` 或 `mongodb+srv://` 连接串。引用使用 `source.database.collection`。默认采样 1000 条推断结构，`schema_sample_size` 可设为 1–10000；空集合或采样内全为空的字段无法推断。嵌套对象展开为点分字段，数组保留为 Extended JSON 文本，ObjectId 为十六进制文本，Decimal128 保留为十进制文本。采样和后续读取发现类型不一致时失败，不做静默转换。普通游标不保证一致快照，`require_consistent_snapshot` 拒绝此来源。 |
| `duckdb` | `path` 指向服务端数据库文件，引用使用 `source.schema.table`（常见模式为 `main`）。只读打开并使用事务；禁止外部访问与自动扩展访问，关闭 DuckDB 临时落盘，查询结果通过 GraphLake 的磁盘配额溢写。DuckDB 执行引擎预留查询内存预算的一半；读取期间文件不能由其他进程写入。 |
| `databricks` | 工作区 `uri`、`token_env`、SQL Warehouse ID `warehouse`、`catalog`；引用使用 `source.schema.table`。通过 Statement Execution API 读取 JSON_ARRAY 分页，校验行数、游标和截断标记，超时取消语句。使用 INLINE 结果，受 Databricks 的 INLINE 大小限制；超过限制需缩小远端视图，不会返回部分结果。复杂类型应在远端视图转为标量。 |

Oracle 客户端是运行时依赖，不随 GraphLake 分发。CLI 会保留 Oracle 驱动的错误原因。遇到 `DPI-1047` 时检查客户端库搜索路径及其系统依赖，例如 Linux 的 `libaio`；遇到 `ORA-12541` 时检查连接地址和数据库监听器。SQL Server、Oracle 等服务端行为必须在目标数据库版本上进行实连验收；协议夹具不能替代这一验收。所有连接器的外部读取都只生成查询和会话设置，不提供写入 SQL。

`object_storage` 为每个数据源分别保存存储配置，不修改进程环境变量。配置 `storage.provider`、`storage.bucket`、`format`（`parquet`、`csv`、`jsonl`）和表映射 `tables`；每张表的 `files` 是桶内相对对象键，不是 URL。CSV/JSON Lines 同时声明 `columns`，格式与独立文件连接器一致。界面按完整路径段递归列出对象，例如 `exports/2026/`，并可将对象加入表映射。列表最多展示 2000 项，需要列出对象的权限。列出对象与按已知对象键读取文件分别授权。

| 存储服务 | 配置 |
| --- | --- |
| `s3` / `minio` | 可选 `region`、`endpoint`；MinIO 必须指定 endpoint。`access_key_env` 与 `secret_key_env` 成对配置，可另配 `session_token_env`。S3 可使用服务端默认身份。 |
| `oss` | 指定区域及 OSS endpoint，账号须启用 S3 兼容 V4 认证；使用相同的密钥环境变量字段。 |
| `tos` | 指定区域以及 `https://tos-s3-区域.volces.com` 兼容端点，使用相同的密钥环境变量字段。 |
| `cos` | 指定区域以及 COS endpoint，桶名包含 APPID，使用相同的密钥环境变量字段。 |
| `azure` | `bucket` 为容器名，`account` 为存储账号；`account_key_env` 可引用账号密钥。未指定时使用 Azure 默认凭据链，可指定自定义 endpoint。 |
| `gcs` | `bucket` 为桶名；`service_account_env` 引用包含服务账号 JSON 的环境变量，或使用默认身份。不接受自定义 endpoint。 |

OSS/TOS/COS 自动使用带桶名的虚拟主机访问；S3/MinIO 自定义 endpoint 使用 path-style。HTTP endpoint 仅允许回环测试地址，其余连接使用 HTTPS。对象读取固定版本或强 ETag，后续范围读取保持相同版本；多对象不声明为同一快照。凭据只在服务端读取，配置中填写环境变量名。云端权限、端点和凭据链仍需在实际账户环境验证。

数据源按映射列读取。DuckDB、SQL Server、Oracle、Databricks、Snowflake 和 BigQuery 会下推类型匹配的整数比较与空值判断；Iceberg 使用顶层 32/64 位整数条件在固定快照内裁剪。可下推条件来自物理映射过滤、逻辑分区，以及能证明只读取同一节点表的查询中的业务键。共享物理表只下推各用法共有的条件，列转换后的逻辑过滤仍在本地执行。GraphLake 保留查询过滤与业务键校验，字符串排序规则、隐式类型转换、复杂表达式和跨源关联由本地引擎处理。

通过 SQL API 读取数仓时，存在可下推条件才发出额外的零行查询确认列类型，数据语句仍使用原有的快照设置。MongoDB 在采样和读取时都投影映射字段，采样不受查询过滤影响。对象存储的同表文件共享客户端，CSV 和 JSON Lines 按内存预算合并范围读取，单次最多 1 MiB；对象版本固定和逐条解码校验保持有效。

普通 SQL 的单层 `SELECT` 及显式 `ON` 连接从源读取查询引用的列，同时保留业务键、关系端点和分区条件使用的列。通配符、CTE、子查询和隐式连接列仍使用完整投影。多输入和 SCD2 继续读取用于去重及冲突判断的完整映射。CSV 和 JSON Lines 仍扫描并校验完整文件。

DuckDB 和 PostgreSQL 可下推单节点表的有序 `LIMIT`（无 `OFFSET`）、标量 `COUNT(*)`、`COUNT(列)` 和整型 `MIN/MAX`。源端须有能够证明整数业务键唯一的约束，有序 LIMIT 须按完整业务键排序。所有 WHERE 条件都须能精确下推，目前覆盖以 AND 连接的整数比较和空值判断。下推仍在原读快照内执行。DuckDB 直接查验映射表的完整主键。多个 DuckDB 来源共享查询内存的一半，剩余预算用于缓冲、溢写和本地运算。PostgreSQL 聚合和有序 LIMIT 都排除空业务键。业务键无约束、排序存在并列、聚合带分组或 DISTINCT，以及无法证明条件等价的查询使用本地计划。

用 CLI 的 `--diagnostics` 对照 `metrics.reads` 和 `metrics.elapsed_ms`。其中 `warehouse_rows`、`postgres_rows` 与对应的 Arrow 字节数统计进入 GraphLake 的数据，源数据库内部扫描量需用其执行诊断另行测量。

DuckDB 可利用行组的列最小值和最大值裁剪有序 `LIMIT` 的扫描范围，裁剪效果取决于键在行组间的分布和查询过滤条件。无 `WHERE`、至多 50 行、按单列整数主键排序且只读主键和一个整数属性时，连接器使用单趟扫描，避免按行 ID 再读取同一批行组。带过滤条件或涉及宽列的查询沿用 DuckDB 的优化器策略。


[跨源示例](../examples/finance.yaml) 将 PostgreSQL 账户与 S3 转账表关联。PostgreSQL 连接串从 `dsn_env` 指定的环境变量读取，TLS 使用系统证书验证。测试实例可在连接串中显式设置 `sslmode=disable`。连接器支持布尔、整数、浮点、文本、UUID、二进制、日期、时间戳和声明了精度与小数位的 `NUMERIC(p,s)`。Decimal 转为 Arrow Decimal128，要求 `0 <= s <= p <= 38`。UUID 转为文本，带时区时间戳按 UTC 输出。

Parquet 表显式列出文件，YAML 中的相对路径以配置文件目录为准。同一表各文件的映射列须具有相同的 Arrow 类型。HTTP 服务须支持 HEAD、Range 和强 ETag。S3 使用 `AWS_REGION` 等标准 AWS 配置，也可通过 `AWS_ENDPOINT` 指定兼容服务。


通过 Snowflake、Spanner、Trino、SAP HANA 和 SQL Server／Oracle 直连读取的时间戳统一映射为 Arrow 微秒时间戳，可保留 9999-12-31 等源端长时间范围的日期。超出微秒的尾数被截去。TIME 列仍按连接器映射保留其时间精度。

PostgreSQL 无穷日期、无穷时间戳和定点 `NaN` 在读取时报告具体值和类型。需要这些值或无精度约束的 `NUMERIC` 时，可在源端创建只读视图，将相应列转成 `text`，例如 `valid_until::text AS valid_until`。有限日期与定点数继续使用 Arrow 类型。

**CSV / JSON Lines**

`type: csv` 和 `type: jsonl` 原生读取 UTF-8 文本文件，支持本地路径、HTTP(S) 和 S3。每张表的 `files` 必须显式列出文件；相对路径以 YAML 所在目录为准。HTTP 服务需要 HEAD、Range 和强 ETag，S3 使用与 Parquet 相同的 AWS 环境配置。读取按对象版本或 ETag 固定，检测到读取期间的对象变化则失败。查询内的重复扫描读取同一份受内存、磁盘预算约束的物化结果；不同文件之间没有共同提交点。

文本解码复用 [Apache Arrow CSV Reader](https://arrow.apache.org/rust/arrow_csv/reader/index.html) 与 JSON Reader。列采用显式 `columns` 列表，避免自动推断破坏前导零业务键、大整数或金额精度。`nullable` 默认 `true`。支持 Boolean、Int8/16/32/64、UInt8/16/32/64、Float32/64、Utf8、Date32、Timestamp 和 Decimal128；Decimal 要求 `0 <= scale <= precision <= 38`。CSV 必须为逗号分隔、带表头格式，所有列的名字与顺序必须匹配声明，支持引号和引号内换行，空字段读为 NULL。JSON Lines 每条记录为对象，按顶层字段名读取，未声明的字段忽略，缺失字段视为 NULL；声明列的类型不匹配会失败。Decimal 推荐在 JSON 中写为字符串。

```yaml
sources:
  files:
    type: csv
    tables:
      accounts:
        files: [accounts.csv]
        columns:
          - {name: id, data_type: Int64, nullable: false}
          - {name: name, data_type: Utf8}
```

节点引用写作 `files.accounts`。[完整可运行示例](../examples/text/graph.yaml) 将 CSV 节点与 JSON Lines 边关联。浏览器可添加表映射、文件路径、列名、类型及空值约束；「浏览表与字段」检查文件可读取并解码首条记录后返回声明的列结构。完整数据验证仍需执行模型验证或查询。

**SQLite**

```yaml
sources:
  local:
    type: sqlite
    path: ./business.db
nodes:
  Account:
    source: local.accounts
    key: [id]
    properties: [name]
```

SQLite 使用本地数据库文件（事务语义见 [SQLite 官方隔离说明](https://sqlite.org/isolation.html)），表引用为 `source.table`。相对路径以 YAML 所在目录为准；数据库必须已经存在。连接以只读标志打开并启用 `query_only`，不会创建数据库、写入业务表或执行用户 SQL。一次查询中同一来源的所有表共用一个读取事务；会话标识记录在 `sources` 中。WAL 模式下并发提交不会改变该事务已固定的视图。SQL 扫描在阻塞线程执行，取消会通过 SQLite progress handler 中断工作；锁等待最多 100 ms。

按列声明类型映射 INTEGER affinity → Int64、TEXT affinity → Utf8、REAL affinity → Float64、BLOB → Binary；BOOL/BOOLEAN 仅接受 0、1 或 NULL。NUMERIC/DECIMAL、无声明类型和其他无法稳定映射的类型返回错误，不自动猜测或转换。SQLite 的动态类型允许一列混入其他存储类型，GraphLake 会拒绝与声明不符的值。空值仍按源列约束及图键规则处理。映射列投影和安全的整数、布尔、NULL 过滤可下推，结果按查询预算物化，支持溢写。浏览器可发现数据库中的表、视图及支持类型的列。

SQLite、CSV 和 JSON Lines 当前不支持显式 Bolt 读取事务；SQLite 也不作为恢复向导的 PostgreSQL 恢复目标。多文件来源要求一致快照时会被拒绝；同一 SQLite 来源的多表可共享一致快照。

- 节点和有键的边要求来源提供唯一的业务键。复合键按 Catalog 中的列顺序和 Arrow 类型匹配。图查询过滤空键和找不到端点的边。
- 边可以省略 `key`。引擎在查询内物化一次并分配 `__graphlake_edge_id`，同一查询中的各次扫描共享这些 ID。
- 一个 PostgreSQL 来源中的表和扫描共用只读、可重复读事务。Parquet 每次范围读取携带版本或 ETag 条件。各来源分别固定版本，结果会记录来源各自的版本和一致性保证。
- PostgreSQL 下推投影及整数、布尔、NULL 语义明确的过滤条件。Parquet 下推列裁剪和行组剪枝，DataFusion 复核剩余行上的谓词。

Iceberg 使用 Apache Rust SDK 连接 REST Catalog，按表固定当前快照或显式指定的快照 ID，按需投影列。支持 v1 和 v2，v2 扫描应用位置删除和等值删除。数据可位于本地文件系统或 S3 兼容存储。使用 `AWS_ENDPOINT` 指定 S3 兼容服务时，连接器使用 path-style 请求。

```yaml
sources:
  lake:
    type: iceberg
    uri: https://catalog.example.com
    warehouse: s3://finance-lake/warehouse
    token_env: ICEBERG_REST_TOKEN
    snapshots:
      finance.transfers: 123456789
```

边或节点的 `source` 写作 `lake.finance.transfers`。可省略 `warehouse`、`token_env` 和 `snapshots`。REST Catalog 可提供存储配置，客户端也读取 `AWS_REGION`、`AWS_ENDPOINT` 和 AWS 凭据环境变量。

MySQL 8.x 使用 `source.database.table` 映射 InnoDB 表。连接串从 `dsn_env` 读取，默认验证 TLS 证书及主机名。私有 CA 可通过 `ca_file` 添加 PEM 证书，相对路径以 YAML 所在目录为准。隔离测试实例可显式配置 `tls: false`。同一来源的表在一个只读可重复读快照里分批读取，按查询内存和磁盘预算物化映射列，之后再做本机关联。映射保留有符号和无符号整数，支持精度最高 38 的 DECIMAL，并按 UTC 读取 TIMESTAMP。查询报告中的 MySQL 版本标识对应本次读取会话。

```yaml
sources:
  crm:
    type: mysql
    dsn_env: GRAPHLAKE_MYSQL_DSN
```

Delta Lake 表使用 delta-rs 读取事务日志，可配置本地目录或 S3 URI。引擎固定当前提交版本，也可显式选择历史版本。扫描应用文件移除、分区值和删除向量。遇到不支持的读取协议特性时，查询会返回错误。

```yaml
sources:
  lake:
    type: delta
    tables:
      finance.transfers:
        path: s3://finance-lake/delta/transfers
        version: 42
```

节点或边的 `source` 可写作 `lake.finance.transfers`。省略 `version` 时选择最新提交。Delta 的 S3 配置沿用 Parquet 的 AWS 环境变量，本地相对路径以 YAML 所在目录为准。

Snowflake SQL API 和 BigQuery Jobs API 按来源固定 Time Travel 时间戳，分批读取映射列，物化为 Arrow 批次后执行本机关联。`as_of` 可指定保留期内的 RFC 3339 时间戳，省略时从该来源读取当前时间。凭据由调用方通过 `token_env` 注入。引擎使用 TLS 验证 API 证书，明文 HTTP 仅用于回环地址测试。请求带远端执行时限，取消或超时会在运行时仍可用时尝试取消远端作业。

```yaml
sources:
  snow:
    type: snowflake
    uri: https://my-org-my-account.snowflakecomputing.com
    token_env: SNOWFLAKE_TOKEN
    warehouse: GRAPH_WH
    role: GRAPH_READER
  bq:
    type: bigquery
    token_env: BIGQUERY_ACCESS_TOKEN
    billing_project: graph-jobs
    location: US
    maximum_bytes_billed: 1073741824
```

Snowflake 表引用形如 `snow.FINANCE.PUBLIC.TRANSFERS`，按映射中的精确大小写读取数据库、schema、表和列。`role` 可省略。Snowflake 将 `NUMBER(p,0)` 按精度映射为 Int16、Int32、Int64 或 Decimal128。BigQuery 表引用形如 `bq.data-project.finance.accounts`，`maximum_bytes_billed` 限制每个查询作业的计费字节。BigQuery `INT64` 映射为 Int64，`NUMERIC` 保留精度和小数位，嵌套或重复字段、`BIGNUMERIC` 会返回类型错误。单个 API 响应解码上限为 32 MiB，Arrow 批次占用查询内存预算。

ClickHouse 通过 HTTP 读取 ArrowStream，将 Date 和 Nullable(Date) 映射为 Arrow Date32，表引用使用 `source.database.table`。配置 `user` 和可选的 `password_env`。投影下推到 ClickHouse，默认以 Arrow Utf8 读取字符串并展开 LowCardinality 字符串。若只读账号禁止更改查询设置，引擎沿用服务器配置的 Arrow 格式和远端执行限制。查询结果按语句记录 ClickHouse 查询 ID。锁定只读配置需在服务端设置远端执行时限和断连取消，GraphLake 仍按请求超时停止等待。

Trino 通过协调器的 HTTP 查询协议读取 `source.catalog.schema.table`。配置 `user`，并按协调器认证方式配置 `token_env` 或 `password_env`。引擎读取分页结果，保留十进制精度，并启用 Trino 参数化时间类型以保留不超过纳秒的时间精度。带时区时间戳归一为 UTC。数组、Map、Row 等结构化 Trino 列需要先在远端视图中展开或转换。结果按表记录 Trino 查询 ID。

```yaml
sources:
  events:
    type: clickhouse
    uri: https://clickhouse.example.com:8443
    user: graph_reader
    password_env: GRAPHLAKE_CLICKHOUSE_PASSWORD
  federated:
    type: trino
    uri: https://trino.example.com
    user: graph_reader
    token_env: GRAPHLAKE_TRINO_TOKEN
```

SQL Server 和 Oracle 可使用 `type: sqlserver`、`type: oracle`，配置结构与 MongoDB 等命名 Trino 连接器相同（`uri`、`catalog`、`user` 及可选认证变量），节点引用为 `source.schema.table`。它们依赖已经部署的 Trino Catalog，不是原生数据库驱动。通用 Trino 入口仍然可用。

在 Trino 部署中配置 [SQL Server](https://trino.io/docs/current/connector/sqlserver.html)、[Oracle](https://trino.io/docs/current/connector/oracle.html)、[Hive](https://trino.io/docs/current/connector/hive.html) 和 [Hudi](https://trino.io/docs/current/connector/hudi.html) Catalog 后，可将节点或边的来源分别映射到 `federated.sqlserver.dbo.accounts`、`federated.oracle.finance.transfers`、`federated.hive.finance.accounts` 或 `federated.hudi.finance.transfers`。JDBC 凭据、Metastore 和文件存储由 Trino 管理。ClickHouse 和 Trino 都校验 TLS，明文 HTTP 限回环地址。两个连接器均按远端语句读取，查询跨表组合时各表可见版本由远端系统分别决定。

[MongoDB](https://trino.io/docs/current/connector/mongodb.html)、[Elasticsearch](https://trino.io/docs/current/connector/elasticsearch.html) 和 [Redshift](https://trino.io/docs/current/connector/redshift.html) 使用各自的 Trino 插件。先在协调器配置 Catalog，然后在 GraphLake 配置 `type: mongodb`、`type: elasticsearch` 或 `type: redshift`，并在 `catalog` 填入 Trino Catalog 名。三种类型共享 Trino 的 `uri`、`user`、`token_env` 和 `password_env`。节点或边引用分别使用 `docs.finance.accounts`（MongoDB 数据库和集合）、`search.default.accounts`（Elasticsearch schema 和索引）或 `warehouse.public.accounts`（Redshift schema 和表）。集合、索引或表名称可以包含点，例如 `search.default.transfers.2026.11`。嵌套字段通过 Trino 提供标量列后再映射。MongoDB ObjectId 读取为 12 字节的 Arrow Binary，可以直接作为业务键。MongoDB 的字段类型由 Trino 的 schema collection 管理。多个协调器并行访问新集合前，先写入该集合的字段定义，避免首次查询同时推断并写入定义。

[Spanner](https://docs.cloud.google.com/spanner/docs/) 通过官方 Rust 客户端读取 GoogleSQL 数据库。生产连接使用 Application Default Credentials。可为本机模拟器指定 `uri: http://127.0.0.1:9010` 或使用 SDK 的 `SPANNER_EMULATOR_HOST`。节点或边引用使用 `global.Accounts` 或 `global.schema.Accounts`。同一个来源内的多表读取共享 Spanner 的只读事务并记录读时间戳。同一引擎在同一 Tokio 运行时复用数据库会话。支持标量列，`NUMERIC` 映射为 Decimal128(38,9)，时间戳以 UTC 纳秒读取。数组、结构和超出 Arrow 纳秒范围的时间戳返回错误。

SAP HANA 直连租户 SQL 端口，使用 `hana.SCHEMA.TABLE` 引用表。schema、表及列按映射中的精确大小写读取。TLS 默认启用并校验证书，可以使用 `ca_file` 增加私有 CA。明文仅用于回环测试。支持常用标量和定点十进制列，LOB、数组、空间类型和超出 Arrow 纳秒范围的时间戳需要在远端视图转换。每张表按独立语句读取，GraphLake 为该读取生成诊断 ID。

```yaml
sources:
  docs:
    type: mongodb
    uri: https://trino.example.com
    catalog: mongo_finance
    user: graph_reader
    token_env: GRAPHLAKE_TRINO_TOKEN
  global:
    type: spanner
    database: projects/example-project/instances/finance/databases/ledger
  hana:
    type: sap_hana
    host: hana.example.com
    port: 443
    user: GRAPH_READER
    password_env: GRAPHLAKE_HANA_PASSWORD
```

### 多表图映射与业务时间

单表标签使用 `source`。多表标签使用 `inputs`，每个输入的 `columns` 以逻辑字段为键，省略的字段沿用同名源列。输入的 `where` 在重命名前按源列过滤，支持 `eq/ne/lt/le/gt/ge/is_null/is_not_null`。属性可通过 `casts` 指定 Arrow 类型，例如 `{name: Utf8}`。业务键和端点列使用一致的 Arrow 类型。

```yaml
nodes:
  Account:
    key: [tenant, id]
    properties: [name]
    scd2: {valid_from: effective, valid_to: expires}
    inputs:
      - table: crm.public.accounts
        columns: {tenant: tenant_id, id: account_id, name: display_name}
        where: [{column: deleted, op: eq, value: false}]
      - table: lake.finance.accounts
        columns: {tenant: tenant_id, id: account_id, name: account_name}
```

分区和 SCD2 条件在去重及端点校验前应用。普通单表节点和有键边会拒绝参与查询的重复业务键。整图分析和 CSR 刷新在构建拓扑时校验业务键，包含端点缺失的边。普通查询可复用满足键约束的 PostgreSQL 有效唯一索引。同一引擎会复用已通过完整校验的 Parquet 业务键证明，文件版本变化时重新校验。多表映射和 SCD2 折叠完整相同的记录，同键冲突默认报错。键点查和相关子查询可先筛选候选键，再校验各候选键的完整输入记录。候选键之外的冲突由扫描对应键的查询或拓扑构建发现。多表有键标签可配置 `on_conflict: prefer_first`，按输入顺序选择完整记录，优先输入内部仍须满足键唯一性。无键边保留独立的查询内身份。

`snapshot_time` 可指定与 SCD2 列同类型的日期、时间戳或整数，历史区间采用 `[valid_from, valid_to)`。空起点表示无下界，空结束值默认表示无上界，可用 `open_end` 配置结束值哨兵。默认读取当前记录，可用 `current_flag` 和 `current_value` 定义当前标记。业务时间和数据源快照版本分别指定。

```cypher
USING {logicalPartition: {tenant: 42}, SnapShotTime: '2026-06-01'}
MATCH (a:Account)
OPTIONAL MATCH (a)-[t:Transfer]->(b:Account)
WHERE t.amount > 0
WITH a, count(t) AS outgoing
RETURN a.id AS id, outgoing
```

分区字段必须在参与查询的标签上可见。共享标签可配置 `unpartitioned: true`。相同条件可通过 Rust `QueryOptions.graph`、Python 构造参数 `logical_partition` 和 `snapshot_time`、或 CLI 传入，适用于 SQL、Cypher、Gremlin、遍历、算法和刷新。CLI 和 Python 的 `logical_partition` 接收 JSON 谓词数组，例如 `[{"column":"tenant","op":"eq","value":42}]`。CSR 按图映射、权限范围、逻辑分区和业务时间选择代次。启用映射过滤、分区或 SCD2 的 PostgreSQL 遍历走逻辑图扫描，复用查询内快照。

默认按来源分别固定读取版本。需要统一快照时，在 Rust `QueryOptions.graph`、Python `Engine` 构造参数或 C 的图选项 JSON 中设置 `require_consistent_snapshot`，在 CLI 传入 `--require-consistent-snapshot`，或在 Cypher 开头写 `USING {requireConsistentSnapshot: true}`。该模式允许单个物理读取，如一个 Parquet 对象、一个 Iceberg 或 Delta 表快照、或一条 Trino、ClickHouse 或 SAP HANA 查询。同一个 PostgreSQL、MySQL 或 Spanner 来源可在单个事务中读取多表。同一个 Snowflake 或 BigQuery 来源可使用同一个时间旅行时间点。PostgreSQL 视图和外部表、多个独立快照以及跨来源组合会返回错误。严格模式使用独立的 CSR 代次。

## 查询语义与资源控制

Cypher 支持串接 `MATCH`、`OPTIONAL MATCH`、`WITH`、`UNWIND` 和 `RETURN`，以及 `UNION [ALL]` 和返回结果的只读 `CALL {}`。相关子查询支持 `CALL (a) { ... }` 和首条 `WITH` 两种导入方式。`CALL (a)` 导入的变量在整个子查询中可见。由 `MATCH`、`OPTIONAL MATCH`、`UNWIND` 和 `WITH` 组成的相关子查询，可按导入的实体身份和标量值分组批量执行。中间 `WITH` 的聚合、去重、排序和分页分别作用于每个关联组。符合这些条件的嵌套 `CALL` 和 `UNION [ALL]` 分支也可批量执行。`UNION` 在各自的关联组内去重。最终 `RETURN` 可投影或聚合，并可对每组独立排序和分页。重复的外层输入保留各自的结果。分批执行时，查询内缓存保留近期、符合预算的关联组，并跨批次复用已求值的键。其余子查询按行求值，并在查询预算内复用重复导入键的结果。两条路径共享来源快照，输出可按查询预算溢写。紧随 `CALL` 且无排序、去重或聚合的最终 `RETURN` 可以用 `SKIP` 和 `LIMIT` 提前停止求值。相关子查询保留已有的外层排序，跨分区读取时也用该顺序分页。批量路径分批读取外层输入。可提前停止的外层分页只读取每个新关联组所需的结果前缀，已缓存的组仍可成批复用。显式排序和聚合仍需处理满足语义的输入。对于固定方向的相关 MATCH，如果分页只依赖排序键和其他窄字段，Cypher 先选出页面，再用命中行的业务键批量读取仅用于返回的目标节点属性。支持外层和子查询内分页，优化可穿过保留目标身份的 WITH、按实体去重或分组、嵌套 CALL，以及各分支从同一标签读取同名属性的 UNION ALL。排序、过滤或聚合使用的属性在对应阶段读取。`LIMIT 0` 只准备结果模式，跳过数据扫描。SQL 提供映射后的图表，大小写敏感的表名使用双引号。三种查询入口都支持 `EXPLAIN` 和 `EXPLAIN ANALYZE`。

定长有向模式可用导入的业务键和前一段查询选出的节点键限制节点与边的读取。单节点 `MATCH` 也支持与导入标量的业务键等值条件。多表映射的键条件可穿过列重命名和去重，源端能执行的条件由连接器下推。普通标量投影裁剪未使用的属性列，完整实体及冲突校验仍保留所需字段。图级分区和 SCD2 条件照常应用。跨来源使用有界候选键，后续关系和目标表共享 PostgreSQL 快照时也可在该来源执行半连接。支持比较的候选键超出内存或数量预算时，先全局去重并按需溢写，再分批向连接器下推键过滤。Parquet 根据整数候选键覆盖的行组、预计读取行数和查询内存预算选择精确谓词或范围剪枝。范围剪枝后用批次键集合校验成员关系。

`WITH ... WHERE` 在分页前过滤。没有聚合或去重的 `WITH` 允许 `WHERE` 读取输入变量和投影别名。聚合或去重后的过滤作用于投影后的作用域。`WITH ... ORDER BY` 的排序用于紧随其后的 `collect()`，`collect(DISTINCT ...)` 按排序后的首次出现去重。`DISTINCT` 使用支持溢写的分组执行。`collect(DISTINCT ...)` 在聚合过程中去重，溢写后先按 Cypher 等价键归并，再将保留的首次出现值按查询顺序组装成完整列表。`collect()` 的中间状态可写入查询临时盘，最终物化的 Arrow 列表仍需满足内存预算。只读取列表长度时，支持的 `size(collect(...))` 和相邻 `WITH` 写法可直接计数。

多标签图中的 `MATCH (n)` 遍历所有映射的节点标签，包含孤立节点。`MATCH (n:Account|Device)` 只匹配指定标签。跨标签结果带有 `label`、按业务键列顺序排列的 `key` 和 `properties`。同名属性具有不同类型时，结果使用按 Arrow 类型命名的结构体字段保存原值，例如 `{"Int64":1,"Utf8View":null}` 和 `{"Int64":null,"Utf8View":"1"}`。同类型属性直接返回标量。跨标签返回实体的 `UNION [ALL]` 使用同样的实体格式，也可以与返回 `null` 的分支合并。

支持简单 `CASE value WHEN ...` 和条件 `CASE WHEN ...`。列表切片 `xs[start..end]` 从零计数，包含起点并排除终点。负下标从列表尾部计数，省略边界时取到相应端点，越界时截断。

`CASE`、`coalesce()`、`UNION` 和 `UNION ALL` 的标量分支使用同样的 Arrow 类型字段保留异构结果。同类型的结果直接返回标量。`coalesce()` 保留第一个非空值的类型。列表分支的元素类型可不同，元素在列表中保留各自的类型，可以继续求长度、展开或参与列表推导。`UNION`、`DISTINCT`、`count(DISTINCT ...)`、`collect(DISTINCT ...)` 和聚合分组共享等价规则。数值相等的整数和浮点数归为同一值，文本 `'1'` 与数值 `1` 属于不同组。`=`、`<>` 和 `IN` 按实际值类型求值，数值与字符串之间的大小比较返回 `null`。列表逐项比较，共同前缀相等时短列表在前。`ORDER BY`、`min()` 和 `max()` 按 Cypher 值顺序比较，极值保留选中值的类型。混合类型属性上的数值运算按当前行的实际操作数类型求值。`sum()` 和 `avg()` 可聚合混合整数、浮点和十进制数。整数及十进制计算使用对应的精确数值类型，参与计算的类型包含浮点时采用浮点计算。`SUM(DISTINCT ...)` 和 `AVG(DISTINCT ...)` 在数值转换之前按原值去重。

`xs[index]` 同样从零计数，支持负下标，越界返回 `null`。`range(start, end[, step])` 生成包含端点的整数序列，支持负步长。步长为零或序列超过查询内存预算时返回错误。列表推导 `[x IN xs WHERE x > 1 | x * 2]` 可筛选并投影列表，输入也可以是 `collect(n)` 生成的实体列表。`value IN xs` 支持查询计算出的列表，未命中且列表有空值时返回 `null`。

`EXISTS { MATCH (n)-[:Transfer]->(m) WHERE ... }` 检查模式是否有结果。子查询可以串接多个必选 `MATCH` 及其 `WHERE`，可关联外层绑定的实体，谓词也可以引用外层标量，支持固定和有界变长模式。可用于普通匹配、可选匹配和投影表达式，包括列表推导的筛选和投影，例如 `[x IN accounts WHERE EXISTS { MATCH (x)-[:Transfer]->() } | id(x)]`。引用外层标量的谓词按关联键批量匹配。跨多层列表推导的谓词按实际列表输入展开关联元组，并只收集命中图模式的元组。同名列表变量按各自的词法作用域解析。

`EXISTS { ... }` 与 `COUNT { ... }` 接受模式简写，以及带 `MATCH`、`OPTIONAL MATCH`、`WITH`、`UNWIND`、`CALL`、`RETURN` 和 `UNION` 的只读子查询。表达式可直接引用外层绑定，且这些绑定在内部 `WITH` 后继续可见。进入每层 `CALL` 时，通过 `CALL (n, k) { ... }` 或旧式首个 `WITH` 导入该层使用的变量。嵌套调用的聚合、去重和分页按调用输入独立应用。`COUNT { ... }` 统计实际输出行数，空结果返回 `0`。

```cypher
MATCH (n:Account)
RETURN id(n) AS account, COUNT {
  CALL (n) {
    MATCH (n)-[:Transfer]->(m:Account)
    RETURN DISTINCT id(m) AS target ORDER BY target SKIP 1 LIMIT 3
  }
  RETURN target
} AS targets_on_page
```

模式推导 `[(n)-[:Transfer]->(m) WHERE m.active | m]` 返回每次匹配的投影值，无匹配时返回空列表。投影可以返回实体和命名路径，并保留后续表达式读取它们所需的类型。

`id(n)` 和 `elementId(n)` 返回可传入 Gremlin `V()` 或 `E()` 的字符串身份，例如 `Account[1,10]`。节点和关系的身份由实体类别、映射标签及有序身份键组成。身份键采用业务键，无业务键的关系使用合成行键。`=`、`IN`、去重和分组按该身份识别实体。组件中的 `%`、逗号和方括号使用百分号转义。`properties(n)` 返回映射后的键和属性，可用 `properties(n).name` 读取字段。传入映射时返回该映射，可选匹配中的空实体返回 `null`。映射投影 `n{.id, alias: n.name, .*}` 支持属性、表达式、变量简写和全部映射属性，显式字段覆盖同名的 `.*` 字段。空实体或空值作为投影基值时返回 `null`。`CASE` 等表达式返回不同结构的映射时，字段访问和映射投影读取当前映射的字段。`labels(n)` 返回节点标签列表，`keys(n)` 返回实体上有值的属性键，`keys(map)` 也包含显式映射到 `null` 的键。

`any(x IN xs WHERE ...)` 判断是否有元素满足谓词，`all()` 判断所有元素是否满足，`none()` 判断是否没有元素满足。空列表上 `any()` 返回 `false`，`all()` 和 `none()` 返回 `true`。谓词遵循三值逻辑。

`reduce(acc = initial, x IN xs | expression)` 从左到右更新每个输入行的累加器。例如 `reduce(cost = 0, e IN relationships(p) | cost + e.amount)` 累加路径上的边属性，`reduce(ids = [], n IN nodes(p) | ids + [id(n)])` 按路径顺序构造身份列表。列表为空时返回初值，列表为 `null` 时返回 `null`。初值和列表表达式使用外层变量，累加器和元素变量只在更新表达式内绑定。更新表达式支持数值、字符串和列表运算、布尔表达式及 `CASE`，其中的图 `EXISTS` 子查询暂不支持。

Cypher 的 `$name` 引用调用方提供的值。支持 JSON 对象中的字符串、布尔值、数字、空值、列表和对象。缺少参数会返回错误，图标签、关系类型、变量名和列名仍写在查询文本中。

```sh
cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml query \
  --parameters '{"tenant":1,"account":10}' \
  'MATCH (a:Account {tenant_id: $tenant, account_id: $account}) RETURN a.account_id AS id'
```

`CALL db.labels()`、`CALL db.relationshipTypes()`、`CALL db.propertyKeys()`、`CALL db.info()` 和 `CALL db.ping()` 读取图定义及引擎状态。可用 `YIELD` 选择结果列，例如 `CALL db.labels() YIELD label RETURN label ORDER BY label`。`CALL db.schema.visualization()` 返回 `nodes` 和 `relationships` 两个列表，其中关系的 `source` 和 `target` 是对应的节点标签。过程只读图映射，无需扫描业务表。

`CALL db.schema.nodeTypeProperties()` 和 `CALL db.schema.relTypeProperties()` 从映射后的表结构返回属性名称、类型和空值约束。它们读取来源表结构或所选 CSR 的结构。`CALL dbms.components()` 返回引擎名称、版本和版本类别，`CALL dbms.info()` 返回图名称。`date()` 和 `datetime()` 支持当前时间及单个字符串或可转换标量参数，如 `date('2025-01-01')`。

Cypher 的 `WITH ... SKIP ... LIMIT ... WHERE ...` 先分页再应用同一子句的过滤条件。整数和定点算术、`sum()` 及 `sum(DISTINCT ...)` 超出结果类型范围时返回错误。Cypher 和 Gremlin 都限制生成的计划大小，Gremlin 也检查 `math()` 公式的解析复杂度。

普通 `EXPLAIN` 只加载规划所需的列类型、文件或快照元数据。需要查询结果来描述列类型的连接器提交零行语句，执行计划中的这些表使用仅包含 schema 的输入。`EXPLAIN ANALYZE` 会执行查询，业务键冲突和数据读取错误在执行时报告。

变长关系用 `*N` 或 `*MIN..MAX`，上界最大为 256。零跳要求同一个节点满足两端的节点条件，关系的类型和方向约束正跳。异构关系的精确零跳模式可从一端显式或已绑定的标签推断另一端。有向和无向模式都保留平行边，每个 `MATCH` 子句内同一条边只能使用一次。`length(p)`、`nodes(p)` 和 `relationships(p)` 读取路径长度及有序身份，变长关系变量可投影为有序边键列表。跨标签或边类型的路径身份包含标签和按列顺序排列的文本键。

定长和变长模式均可省略关系类型，也可使用 `:Transfer|Owns` 限定候选类型。变长析取模式允许各跳选择不同的候选类型。无类型变长模式的每一跳可使用不同的关系类型，可穿越不同标签和业务键结构的节点，保留平行边。零跳也可匹配孤立节点。引擎按路径段构建候选关系，以节点标签和业务键连接各段。匹配结果在查询的过滤、聚合和排序前合并。结果规模仍受查询内存、溢写和超时预算约束。

```cypher
MATCH p=(a:Account)-[*1..4]->(b)
UNWIND relationships(p) AS edge
RETURN type(edge) AS relationship, count(*) AS appearances
```

`UNWIND nodes(p) AS node`、`UNWIND relationships(p) AS edge` 和 `UNWIND r AS edge` 将元素绑定为实体，可以读取身份、标签或关系类型以及属性。列表下标、列表推导和谓词也可以访问实体，例如 `[edge IN relationships(p) | edge.amount]`。实体绑定支持经 `WITH` 投影的路径和路径列表。`collect(nodes(p))`、`collect(relationships(p))` 和返回实体列表的 `CASE` 可逐层展开或参与嵌套推导，元素保留身份及属性。路径列表与实体列表按元素身份比较。列表表达式按实际路径引用的实体键关联属性，并按路径位置恢复顺序。

`shortestPath()` 返回一条无权最短路，`allShortestPaths()` 返回全部并列最短路。每个函数接受单一关系模式，可省略关系类型。涉及整条路径的过滤以及大于一的跳数下界需要显式上界，执行时先筛选路径再选择最短结果。

```sh
cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml query \
  'MATCH p=(a:Account {tenant_id: 1, account_id: 10})-[r:Transfer*0..3]->(b:Account)
   RETURN b.account_id AS account, length(p) AS hops, r AS edges ORDER BY hops'
```

`traverse` 支持端点为同一节点类型的单一边类型。按业务键列顺序逐个传入 `--start` 和 `--target`。使用 `--incoming` 反向遍历。

- `reachable` 为每个可达节点返回最小跳数，起点的跳数为零。
- `shortest` 返回任一条最短路径。目标不可达时结果为空。
- `paths --min-hops N` 枚举指定跳数范围内的边唯一轨迹，允许节点重复，保留平行边。路径列是有序的边业务键结构体列表。

### 只读 Gremlin

```sh
cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml query --language gremlin \
  "g.V([1,10]).hasLabel('Account').repeat(__.out('Transfer')).times(2).dedup().values('account_id').order()"
```

支持 `V/E`、`has/hasLabel/hasId`、`in/out/both` 及边遍历、`inV/outV/bothV/otherV`、`values/label/id/is`、`dedup/count/order/limit/range`、有界 `repeat(...).times(n)`、`simplePath/path` 和 `as/select`。`g.V()` 从全部节点标签开始，`g.E()` 从全部关系类型开始。可通过 `hasLabel('Account','Device')` 筛选多个标签，或用 `g.V('Account[1,10]')` 指定包含标签的身份。`hasId('Account[1,10]', 'Device[2]')` 接受多个完整身份，也可按原始业务键筛选。`range(low, high)` 从零计数，包含起点并排除终点，`high = -1` 表示取到末尾。遍历支持带类型的属性列和复合业务键。方向步骤省略关系标签时会遍历所有匹配方向的边类型。`bothV()` 返回边的两个端点，`otherV()` 根据遍历路径里进入边的顶点返回另一端。`path()` 返回带标签的业务键和标量序列。支持范围为嵌入式只读 Gremlin 文本。

`project('a','b')` 对每条遍历记录生成一个结构体，`select('a','b')` 同时读取多个步骤标签。`by()` 保留当前值，`by('field')` 读取属性，`by(out('Transfer').count())` 对每条记录执行子遍历。投影缺失的字段为 `null`。可继续用 `select('b')` 取出结果中的一个值。异构值通过带类型的 Arrow 结构体保存。

`group().by(label()).by(count())` 和 `groupCount().by(label())` 按节点标签计数。Arrow Map 使用文本键保存类型信息，数值 `1` 的键可为 `@Int64:1`，字符串 `"1"` 的键为 `1`，以 `@` 开头的字符串再加一个 `@` 转义。省略 `group()` 的第二个 `by()` 时，每个分组收集原始遍历值。第二个 `by()` 可以收集属性或执行分组内的聚合。空输入产生空映射。`mean()` 返回数值的 Float64 平均值，空输入不产生结果。`sum()` 汇总数值，空输入返回零。整数和十进制数使用相应的精确数值类型，包含浮点数时使用 Float64。`min()` 和 `max()` 返回排序后的极值并保留原值类型，空输入无结果。`fold()` 把结果收成列表，保留已经指定的顺序，空输入产生空列表。`unfold()` 展开列表，也可将分组映射、投影和属性映射展开成 `key`、`value` 条目。

`valueMap('property', ...)` 返回属性映射，省略属性名时返回该类型的所有映射属性。顶点属性是列表，边属性是标量。`repeat(...).until(__.has(...))` 每轮之后检查终止条件，`until(...).repeat(...)` 在进入下一轮之前检查。循环可跨标签，并可在顶点、边与标量之间切换。每个循环最多执行 256 轮，仍需继续时返回查询错误。混合类型循环按轮求值，当前轮与终止结果可按查询预算溢写。条件支持属性、标签、标量和 `simplePath()` 筛选。

`where(out('Transfer').count().is(gt(1)))` 保留子遍历有结果的原始记录。`where('a',neq('b'))` 比较两个步骤标签的元素身份。`repeat(...).times(3).emit()` 输出每轮结果，前置 `emit().repeat(...)` 也可输出起点。`emit(__.has(...))` 筛选中间结果，终止记录仍按循环终止条件返回。

带 `emit()` 的循环在后续 `limit(n)` 或有界 `range(low, high)` 达到上界时停止展开，中间可经过 `has()`、`is()` 等过滤和 `id()`、`values(property)`、`valueMap(...)` 等投影，并可在限额前使用 `dedup()`。过滤与投影后保留的结果计入限额，`dedup()` 的身份跨迭代累计。若去重后还有过滤步骤，只对首次出现且通过过滤的结果计数。在分支内，限额分别应用于每条分支输入。`filter(traversal)` 和 `map(traversal)` 的子遍历每条输入只需一个结果，满足条件的发射循环可提前结束。有 `order()` 的遍历先完成排序，再应用限额。

`union(...)` 合并所有分支，`coalesce(...)` 选取有结果的第一个分支，`choose(predicate, yes, no)` 根据条件选分支。`choose()` 的条件在每条输入记录找到首个结果后即可结束，只运行该记录选中的分支。`coalesce()` 仅将此前没有产生结果的记录交给下一分支。分支可返回不同标签、边、属性映射和标量，也可组合 `as()/select()`。后续的属性、标签和方向步骤只处理适用的记录。

混合结果以 Arrow 结构体列 `value` 返回。顶点和边的字段名分别为 `vertex_<标签>` 和 `edge_<标签>`，记录包含 `label` 和有序文本 `key`。标量字段为 `value` 或带序号的 `value_N`，保留各自的 Arrow 类型，其他字段为空。查询可用 `hasLabel()` 等步骤收窄结果。

`count()`、`order()`、`limit()` 和 `dedup()` 可用于分支、循环体和 `until()` 条件。分支及条件中的这些步骤按每条输入记录求值。循环体中的这些步骤作用于当前轮的输入。如果循环位于分支中，每轮仍按分支输入记录隔离。分支里的 `count()` 对没有结果的输入返回 `0`，因此 `coalesce()` 会把这个零视为结果。

```gremlin
g.V().hasLabel('Account').union(
  __.out('Transfer').dedup().order().by('account_id', desc).limit(2).values('account_id')
)
```

`filter(traversal)` 保留子遍历有结果的输入，`map(traversal)` 取每条输入的子遍历第一项。`math('_ * 2')` 对当前数值求值，支持用步骤标签、映射字段和 `by()` 提供变量。

`not(traversal)` 保留子遍历没有结果的原始输入。`hasKey('name', ...)` 保留至少一个指定属性有值的节点或边，也能按 `valueMap().unfold()` 产生的条目键筛选。`skip(n)` 跳过前 `n` 条，语义等同于 `range(n,-1)`。在分支内按输入独立执行，跟在 `order()` 后时沿用该顺序。带发射循环的 `skip(n).limit(k)` 在收到足够的结果后结束。

`has(property, value)` 和 `is(value)` 保留实际值类型。`order()` 对不同数值类型按数值大小排序。`order().by(...)` 的顺序会保留到后续过滤、属性投影和 `limit()`，`dedup()` 保留排序后最先出现的结果。

### 远端前沿与持久化 CSR

`traverse --strategy auto` 优先读取已发布的代次。在线访问时，引擎检查 PostgreSQL 节点键与出边或入边端点是否有可用的 B-tree 索引前缀。对于有稳定边键的稀疏前沿，引擎在查询的只读快照中参数化批量查边，并批量验证端点。前沿扩大后，按表行数估计切换到同一快照的单次扫描。`--strategy scan`、`batch` 和 `cached` 可固定访问方式。变长 Cypher 在起点的完整业务键已绑定且满足索引条件时也可选择前沿查找，其余模式走扫描。

```sh
cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml \
  --cache-dir /tmp/graphlake-csr --cache-scope finance-read refresh --edge Transfer

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml --diagnostics \
  --cache-dir /tmp/graphlake-csr --cache-scope finance-read traverse \
  --edge Transfer --start 1 --start 10 --max-hops 3 --strategy cached reachable
```

刷新从新快照构建正反向 CSR，连同 Arrow 业务键、边身份和来源版本写入同一校验文件。同步文件后原子替换当前代次，随后同步目录。同一映射与权限范围内的刷新串行执行，已启动的查询保留原代次。读取时校验文件完整性，刷新会回收该映射的中断暂存文件。调用方按来源权限隔离缓存目录和 `cache-scope`，授权或来源身份变化时使用新的权限范围。已发布的代次由显式刷新推进，可离线遍历。Cypher 属性查询从来源快照读取。

可通过 `--timeout-ms`、`--memory-mib`、`--spill-mib`、`--partitions` 和 `--batch-size` 控制每次查询。内存上限用于 DataFusion 追踪的算子、PostgreSQL 读取批次、Parquet 范围缓冲、查询内物化和图遍历状态。进程 RSS 还包含运行时开销、解码中的临时缓冲和调用方持有的批次。图状态达到预算时返回资源错误。关闭结果流会释放本地状态和来源连接，CLI 支持 Ctrl-C。诊断记录前沿请求、扫描回退、CSR 代次和各深度访问数。PostgreSQL、MySQL、SQLite、Iceberg 和数仓字节数为已解码的 Arrow 批次大小，Parquet、CSV、JSON Lines 的 `object_bytes` 为读取的对象范围大小。Iceberg 请求数按 Catalog 加载和扫描调用统计，Delta 请求数按快照加载统计。Delta 扫描的文件和行级执行统计可通过 `EXPLAIN ANALYZE` 查看。

同一进程通过 `Engine::new` 创建的实例共享 8 个活动查询、2 GiB 内存预算和 8 GiB 临时磁盘预算。查询进入时预留声明的额度，读取完成或关闭结果流后释放。等待资源计入查询超时，也可以取消。Rust 可通过 `Engine::new_with_limits` 为一个实例及其克隆配置独立预算。CLI 单次执行按命令行参数分配预算。

CSV、JSON Lines、SQLite、MySQL、Trino、ClickHouse、Snowflake、BigQuery、Spanner 和 SAP HANA 的查询结果可以物化到受磁盘配额约束的 Arrow IPC 临时文件。多表映射去重、无键边身份、相关 `CALL` 的分批或逐行求值结果也可溢写。Cypher 诊断的 `spilled_bytes` 包含已完成的批量子查询执行阶段和 `collect()` 的算子溢写。低内存下引擎可降低内部并行度，保留排序归并的工作内存。诊断中的 `reads.snapshot_spilled_bytes` 记录这些物化操作产生的磁盘字节。MySQL、Trino、ClickHouse、Spanner 和 SAP HANA 可把单节点标签查询的整数业务键条件及可安全转换的映射或分区条件下推到源端。引擎仍在本地应用条件，ClickHouse 也支持对原生 String 列下推文本等值条件。复杂查询仍可能扫描整个源表。

## 整图算法

单一同构边类型的 PageRank 和弱连通分量包含孤立节点。可指定 `--strategy scan` 读取来源快照，或用 `--strategy cached` 读取已发布的 CSR。默认优先使用已发布代次。

```sh
cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml --diagnostics \
  analyze --edge Transfer pagerank --damping 0.85 --max-iterations 100

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml \
  analyze --edge Transfer weak-components
```

PageRank 按有向无权图计算，平行边分别计权，出度为零的节点均匀分配质量。结果列为 `__graphlake_rank`，诊断报告迭代次数和是否收敛。弱连通分量忽略方向，以分量内最小业务键作为代表，结果列 `__graphlake_component` 为保留原键类型的结构体。两种算法都遵守查询的超时、取消和内存预算。

### 多标签投影和加权算法

重复传入 `--edge` 选择多种边，端点标签自动加入图投影。可用 `--node` 加入其他节点标签，包括孤立节点。投影使用查询时的逻辑分区、业务时间和来源快照。节点和边身份包含标签与按业务键顺序排列的文本值。

```sh
cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml route \
  --edge Transfer --weight amount --k 2 \
  --start-label Account --start 1 --start 10 \
  --target-label Account --target 1 --target 30

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml \
  analyze --edge Transfer --weight amount leiden --resolution 1.0 --seed 42

cargo run --locked -- --catalog /tmp/graphlake-demo/graph.yaml \
  analyze --edge Transfer --direction undirected node2vec --dimensions 32 --seed 42
```

- `route` 使用 Dijkstra 查找加权最短路，`--k` 大于一时使用 Yen 算法返回最多 K 条无环路径。权重累加和排序保留整数、Decimal 及浮点存储值的精度。平行边按独立身份计入。结果包含 `rank`、`cost`、`cost_exact`、`hops` 以及有序的 `vertices` 和 `edges`。`cost_exact` 是精确的十进制字符串，`cost` 是兼容原有调用的近似 Float64，超出有限值范围时为空。两个端点均存在且不可达时返回空结果。
- `route --strategy bellman-ford|delta-stepping|johnson|dag-longest` 使用指定策略，要求 `--k 1`。Bellman–Ford 和 Johnson 支持负权。Bellman–Ford 拒绝从起点可达的负权环，Johnson 检查整个投影的负权环，并返回所指定端点之间的最短路。Delta-Stepping 使用非负权。DAG 最长路支持负权，要求整个投影无环。
- 投影版 PageRank 按出边权重分配质量，弱连通分量忽略方向和权重。两者分别输出 `__graphlake_rank` 和 `__graphlake_component`。
- `analyze --edge Transfer --direction undirected label-propagation` 按投影方向和边权传播社区标签，支持 `--max-iterations`，输出 `__graphlake_community`。
- Louvain 和 Leiden 按无向图优化加权模块度，零权边不参与优化。支持 `--resolution`、`--max-iterations` 和 `--seed`，输出 `__graphlake_community`。社区编号是当前投影中的标识。社区计算在迭代、局部移动、细化和聚合时检查取消和超时。仓库保留 `leiden-rs` 0.8.1 的源码与许可证，提供协作取消入口。
- Node2Vec 采用由 `--return-bias` 和 `--explore-bias` 控制的二阶随机游走，并用负采样训练 Skip-gram。输出定长 Float32 向量 `__graphlake_embedding`，种子和训练参数可配置。无训练上下文的节点保留种子初始化的向量。

`--weight` 指定所有选中边标签共享的映射字段，须为非空、有限数值，省略时为单位权重。负权只用于 Bellman–Ford、Johnson 和 DAG 最长路。PageRank、社区检测和 Node2Vec 使用 Float64 权重。方向可设为 `outgoing`、`incoming` 或 `undirected`。多标签和加权算法在查询内按内存预算构建拓扑。路径查询保留用于输出的边文本键。弱连通分量和社区检测直接使用边端点和权重。普通单表标签只投影业务键、端点、权重和过滤所需的列，多表和 SCD2 标签保留判定冲突所需的属性。

### Cypher 算法过程与查询导出

可在 Cypher 中调用 `algo.pagerank`、`algo.wcc`、`algo.labelPropagation`、`algo.louvain`、`algo.leiden`、`algo.node2vec`、`algo.shortestPath` 和 `algo.dagLongestPath`。过程接收配置对象，可指定 `labels`、`relationshipTypes`、`relationshipWeightProperty` 和 `direction`，结果通过 `YIELD` 进入后续查询。顶点身份 `id` 包含 `label` 以及按映射顺序排列的 `key`。路径过程的 `start` 和 `target` 使用相同的身份结构。

```cypher
CALL algo.labelPropagation({relationshipTypes: ['Transfer'], direction: 'undirected'})
YIELD id, communityId
RETURN id.key AS account, communityId ORDER BY account
```

在 Catalog YAML 的顶层注册导出位置。相对目录以 Catalog 文件所在目录为基准，目录须已存在。

```yaml
export_locations:
  reports: /var/lib/graphlake/reports
  warehouse: s3://analytics-bucket/graphlake/reports
```

```cypher
EXPORT TO 'reports:ranks.parquet'
CALL algo.pagerank({relationshipTypes: ['Transfer']})
YIELD id, rank
RETURN id, rank
```

`EXPORT TO` 可用于 Cypher、SQL 和 Gremlin。结果流被读取时由引擎写入已注册位置，以 `destination` 和 `rows` 返回摘要。扩展名选择 CSV 或 Parquet。CSV 包含表头，嵌套列用 JSON 表示，Parquet 保留列类型。使用相对路径，已有文件或对象会返回错误。S3 使用进程的 AWS 配置，暂存占用查询的溢写配额，单个对象最大 5 GiB。使用 `EXPLAIN` 或 `EXPLAIN ANALYZE` 时只处理内层查询。

## 嵌入 Rust 应用

```rust
use futures::TryStreamExt;
use graphlake::{Catalog, Engine, Language, QueryOptions};

# async fn run() -> anyhow::Result<()> {
let engine = Engine::new(Catalog::load("/tmp/graphlake-demo/graph.yaml")?)?;
let options = QueryOptions::default();
let mut result = engine.query(
    Language::Sql,
    r#"SELECT count(*) AS n FROM "Transfer""#,
    options,
).await?;
while let Some(batch) = result.batches.try_next().await? {
    println!("{batch:?}");
}
println!("{:?}", result.metrics());
# Ok(())
# }
```

`Engine::traverse` 接受显式类型的复合键，`Engine::traverse_with_strategy` 可固定访问方式。`Engine::with_cache` 指定本地缓存及权限范围，`Engine::refresh` 发布代次。`Engine::analyze_graph` 运行单边类型算法，`Engine::analyze_projection` 和 `Engine::route` 接受 `GraphProjection` 并返回 Arrow 结果流。应用可保留 `QueryOptions::cancellation` 的克隆以取消运行中的查询。

使用 `QueryOptions::with_parameters_json` 设置 Cypher 参数，使用 `Engine::route_with_strategy` 指定最短路或 DAG 最长路策略。

## Python 绑定

在 Python 3.10 或更新版本的虚拟环境中构建。绑定通过 Arrow C Data Interface 返回 PyArrow 批次。

```sh
python -m pip install 'maturin>=1.9.4,<2'
maturin develop --locked --manifest-path bindings/python/Cargo.toml
```

```python
import graphlake

engine = graphlake.Engine("/tmp/graphlake-demo/graph.yaml", timeout_ms=60_000)
with engine.query('SELECT count(*) AS n FROM "Transfer"', language="sql") as query:
    for batch in query:
        print(batch.to_pylist())
    print(query.versions, query.metrics())
```

`Engine.traverse`、`Engine.refresh` 和 `Engine.analyze` 共用 Rust 引擎。遍历复合键按映射顺序传入字符串列表。同步调用在等待 Rust 查询时释放 GIL，查询对象可显式取消或用上下文管理器关闭。Python 流式验证可运行 `python tests/python_smoke.py /tmp/graphlake-demo/graph.yaml`。

`Engine.query(..., parameters={"tenant": 1})` 将 Python 字典中的值绑定到 Cypher 查询，`Engine.route(..., strategy="bellman_ford")` 可显式选择路径策略。

```python
with engine.route(["Transfer"], "Account", ["1", "10"], "Account", ["1", "30"],
                  weight="amount", k=2) as query:
    print(next(query).to_pylist())
with engine.analyze_projection(["Transfer"], "node2vec",
                               settings='{"dimensions": 8, "seed": 42}') as query:
    print(next(query).to_pylist())
```

## C ABI

`graphlake-c` 构建共享库。[C 头文件](../bindings/c/graphlake.h) 定义同步 SQL、Cypher 和 Gremlin 查询入口，结果通过 Arrow C Stream Interface 交付。调用方负责调用 Arrow 的 release 回调，结果流可以在引擎句柄关闭后继续读取。`graphlake_engine_query_with_graph` 额外接收图选项 JSON，支持 `logical_partition`、`snapshot_time` 和 `require_consistent_snapshot`。`graphlake_engine_query_with_parameters` 同时接收图选项 JSON 和 Cypher 参数 JSON。[C 验证程序](../tests/c_smoke.c) 使用 `demo` 生成的图映射。

```sh
cargo build --locked -p graphlake-c
cc -Ibindings/c tests/c_smoke.c -Ltarget/debug -lgraphlake_c \
  -Wl,-rpath,"$PWD/target/debug" -o /tmp/graphlake-c-smoke
/tmp/graphlake-c-smoke /tmp/graphlake-demo/graph.yaml
```

## 验证

```sh
cargo test --locked
```

跨源集成用例需预建 S3 测试 bucket，提供 `GRAPHLAKE_TEST_BUCKET`、AWS 配置及 `GRAPHLAKE_TEST_POSTGRES`。测试 PostgreSQL 连接串需可创建测试 schema，并使用 `application_name=graphlake-validation` 与 `sslmode=disable`。

```sh
cargo test --locked --test engine postgres_snapshot_and_parquet_s3_join -- --ignored
```

Iceberg 集成用例需要已启动的测试 REST Catalog，例如 `apache/iceberg-rest-fixture:1.10.0`。在隔离的 Python 虚拟环境安装 `pyiceberg[pyarrow]==0.12.0`，配置与 Rust 测试相同的 AWS 环境变量并使用测试 bucket。生成脚本会发布一个追加快照，以及包含位置删除与等值删除的后续快照。

```sh
python tests/iceberg_fixture.py /tmp/graphlake-iceberg.json s3://test-bucket/iceberg \
  --rest-uri http://127.0.0.1:8181
```

在另一个终端运行集成测试。

```sh
GRAPHLAKE_TEST_ICEBERG_FIXTURE=/tmp/graphlake-iceberg.json \
  cargo test --locked --test engine iceberg_rest_snapshot_applies_position_and_equality_deletes -- --ignored
```

Delta 集成夹具需要 Python 3.13、`deltalake`、`pyarrow` 和 `pyroaring`，以及可写入的 S3 测试前缀。生成脚本会写入分区表、删除文件提交和包含内联删除向量的提交。MySQL 集成用例另外读取 `GRAPHLAKE_TEST_MYSQL`，该连接串需指向允许建表的隔离 MySQL 8.x 测试数据库。

```sh
python tests/delta_fixture.py /tmp/graphlake-delta-test s3://test-bucket/delta
GRAPHLAKE_TEST_DELTA_FIXTURE=/tmp/graphlake-delta-test/fixture.json \
  cargo test --locked --test engine delta_time_travel -- --ignored

GRAPHLAKE_TEST_DELTA_FIXTURE=/tmp/graphlake-delta-test/fixture.json \
  cargo test --locked --test engine mysql_snapshot_joins_delta -- --ignored
```

数仓协议测试使用本地 HTTP fixture 验证 Snowflake、BigQuery、Databricks 和 Trino，覆盖跨源关联、Time Travel、分页、游标来源校验、类型转换、超时取消、键条件下推和磁盘预算。用 `reads.warehouse_rows` 比较下推前后进入 GraphLake 的行数；这一指标不代表数仓内部扫描量或账单。两个命令分别在终端运行。

```sh
python3 tests/warehouse_fixture.py /tmp/graphlake-warehouse-uri
```

```sh
GRAPHLAKE_TEST_WAREHOUSE_URI="$(cat /tmp/graphlake-warehouse-uri)" \
GRAPHLAKE_TEST_WAREHOUSE_TOKEN=graphlake-fixture \
  cargo test --locked --test engine warehouses -- --ignored --skip clickhouse_reads_arrow --skip spanner_joins_tables
```

Spanner 回归使用 [Spanner 模拟器](https://docs.cloud.google.com/spanner/docs/emulator) 的 `projects/graphlake/instances/test/databases/finance` 数据库。用 GoogleSQL 创建并填充下面的测试表。

```sql
CREATE TABLE Accounts (id INT64 NOT NULL, status STRING(64)) PRIMARY KEY (id);
CREATE TABLE Transfers (
  id INT64 NOT NULL, src INT64 NOT NULL, dst INT64 NOT NULL, amount NUMERIC, recorded TIMESTAMP
) PRIMARY KEY (id);

INSERT INTO Accounts (id, status) VALUES (9007199254740993, 'source'), (9007199254740994, 'target');
INSERT INTO Transfers (id, src, dst, amount, recorded)
VALUES (1, 9007199254740993, 9007199254740994, NUMERIC '12.250000001', TIMESTAMP '2026-01-02T03:04:05.123456789Z');
```

```sh
GRAPHLAKE_TEST_SPANNER_URI=http://127.0.0.1:9010 \
  cargo test --locked --test engine spanner_joins_tables -- --ignored
```

ClickHouse 只读账号回归使用独立的 ClickHouse 25.8 测试实例。以测试管理员身份运行以下 SQL。

```sql
CREATE DATABASE graphlake;
CREATE TABLE graphlake.nodes (id Int64, name LowCardinality(String), amount Decimal(12, 2), posted Nullable(Date))
ENGINE = MergeTree ORDER BY id;
INSERT INTO graphlake.nodes
SELECT toInt64(number + 1), concat('node-', toString(number + 1)), toDecimal64(number, 2) + 0.25,
       if(number % 2 = 0, toDate('2026-01-02'), NULL)
FROM numbers(3000);
CREATE USER readonly_reader IDENTIFIED BY 'graphlake-test-password' SETTINGS readonly = 1;
GRANT SELECT ON graphlake.nodes TO readonly_reader;
```

```sh
GRAPHLAKE_TEST_CLICKHOUSE_URI=http://127.0.0.1:8123 \
GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD=graphlake-test-password \
  cargo test --locked --test engine clickhouse_reads_arrow -- --ignored
```

`bash tests/tls_smoke.sh target/debug/graphlake /path/to/test-ca.pem` 验证 PostgreSQL 和 MySQL 的证书链及主机名。测试实例使用临时 CA 和同一张包含 DNS SAN 的服务器证书，PostgreSQL 提供 `public.accounts`，MySQL 提供 `graphlake.accounts`，各包含一个 `BIGINT` 键值 `7`。通过 `GRAPHLAKE_TEST_PG_TLS`、`GRAPHLAKE_TEST_MYSQL_TLS` 指定可验证主机名的连接串，并以 `GRAPHLAKE_TEST_PG_TLS_WRONG_HOST`、`GRAPHLAKE_TEST_MYSQL_TLS_WRONG_HOST` 提供连接到同一实例但证书名称不匹配的地址。PostgreSQL 连接串使用 `sslmode=require`。

安装 PyArrow 后，可在新目录生成 100,001 个节点、1,000,000 条边的 Parquet 图，验证扫描和离线 CSR 的弱连通分量、离线 PageRank 及刷新统计。

```sh
cargo build --locked --bin graphlake
python tests/large_graph_smoke.py target/debug/graphlake "$(mktemp -d)/graph"
```


### 新增文件和数据库来源的集成验证

```sh
cargo test --locked --features admin --test engine sources::
cargo test --locked --features admin --lib source::sqlite::tests
cargo test --locked --features admin --test admin text_sqlite_and_duckdb_sources_browse_publish_query_and_restart
# 需要可写的测试 S3 bucket 及 AWS_REGION/AWS_ENDPOINT/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY（HTTP 测试服务另设 AWS_ALLOW_HTTP=true）：
cargo test --locked --features admin --test engine text_sources_s3_keep_versions -- --ignored
```

上述无外部服务用例覆盖跨文件图查询、整数/Decimal/NULL、CSV 表头漂移、无效 JSON 行、低内存溢写、超大记录预算、SQLite 快照与只读/取消，以及管理 API 的浏览、验证、发布、查询和重启。命名 SQL Server / Oracle 的协议回归复用 `tests/warehouse_fixture.py` 和 `named_trino_connectors_qualify_the_catalog_and_preserve_results`；协议夹具通过不代表真实 SQL Server / Oracle 的服务端类型映射已验证，后者需要部署对应数据库和 Trino Catalog。

### 扩展连接器回归命令

```sh
cargo test --locked --features admin --test engine direct_sources::duckdb_
cargo test --locked --features admin --test admin text_sqlite_and_duckdb_sources
# MongoDB 测试会创建并删除带随机后缀的 graphlake_* 测试数据库：
GRAPHLAKE_TEST_MONGODB_DSN='mongodb://127.0.0.1:27017' \
  cargo test --locked --features admin --test engine mongodb_native_ -- --ignored
# KIND 可为 doris、starrocks、tidb、oceanbase、mariadb；使用独立测试实例：
GRAPHLAKE_TEST_MYSQL_COMPAT_KIND=mariadb \
GRAPHLAKE_TEST_MYSQL_COMPAT_DSN='mysql://test:test@127.0.0.1:3306/' \
  cargo test --locked --features admin --test engine mysql_compatible_sources_ -- --ignored
# 复用上面的 warehouse_fixture.py，设置其返回的 URI 和测试令牌：
GRAPHLAKE_TEST_WAREHOUSE_URI=http://127.0.0.1:PORT \
GRAPHLAKE_TEST_WAREHOUSE_TOKEN=graphlake-fixture \
  cargo test --locked --features admin --test engine databricks_ -- --ignored
# 已存在且可写的专用 S3 兼容测试桶；KEY / SECRET 为测试凭据：
GRAPHLAKE_TEST_STORAGE_ENDPOINT=http://127.0.0.1:9000 \
GRAPHLAKE_TEST_STORAGE_BUCKET=graphlake-test \
GRAPHLAKE_TEST_STORAGE_KEY=test-key GRAPHLAKE_TEST_STORAGE_SECRET=test-secret \
  cargo test --locked --features admin --test engine object_storage_sources_ -- --ignored
```

`native_sql_sources_preserve_integer_decimal_timestamp_and_null_values` 需要真实 SQL Server 或 Oracle。用 `GRAPHLAKE_TEST_DIRECT_SQL_CATALOG` 指定模型 YAML：包含名为 `Record` 的节点，键为 `ID`，属性包含 `AMOUNT`、`CREATED`，来源指向测试表。SQL Server 使用 `ID BIGINT`、`AMOUNT DECIMAL(18,2)`、`CREATED DATETIME2(6)`；Oracle 使用 `ID NUMBER(18,0)`、`AMOUNT NUMBER(18,2)`、`CREATED TIMESTAMP(6)`。两行 ID 为 `9007199254740993` 和 `9007199254740994`，AMOUNT 分别为 `15.25`、NULL，CREATED 为 `2026-11-10 09:00:00.123456`。执行：

```sh
GRAPHLAKE_TEST_DIRECT_SQL_CATALOG=/path/to/sql-fixture.yaml \
  cargo test --locked --features admin --test engine native_sql_sources_ -- --ignored
```

ARM64 调试链接如果因可执行文件体积遇到 GNU ld 的 `R_AARCH64_CALL26` 重定位溢出，可以使用 LLD 链接器；这属于构建工具链要求，不应跳过运行验证。
