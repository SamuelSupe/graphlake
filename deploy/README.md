# GraphLake 单机 Linux 部署

服务包含管理界面、HTTP API，以及可选的 Bolt 和 Gremlin 客户端入口。HTTP 与 Bolt 均监听 `127.0.0.1`。同机客户端可直接连接；远程维护可用 SSH 将对应端口转发到本机的相同端口，例如 `ssh -N -L 7337:127.0.0.1:7337 -L 7687:127.0.0.1:7687 graph-host`。管理界面和客户端在本机仍使用 `127.0.0.1`。部署边界为受信任的单机或 SSH 隧道，回环传输使用明文协议。

同一个管理后台二进制也支持[多 Worker 部署](cluster.md)。

## 构建与安装

Linux 安装包以 Debian 12（glibc 2.36）为兼容基线，提供 ARM64 和 x86_64 两种架构。构建时在 Debian 12 安装 Rust、C/C++ 工具链、LLD 和 Node.js 24，在仓库根目录运行 `RUSTFLAGS="-C link-arg=-fuse-ld=lld" sh deploy/package.sh v1.1.4 dist`。输出文件名包含发行标签、源归档 SHA-256 的前 12 位和架构。可设置 `GRAPHLAKE_BUILD_TARGET` 指定 `aarch64-unknown-linux-gnu` 或 `x86_64-unknown-linux-gnu`，交叉构建使用对应 Rust 目标和 Debian 12 的目标链接器及系统库。可用 `GRAPHLAKE_WEB_DIST` 复用同一份前端产物。打包会拒绝要求高于 glibc 2.36 的二进制。

包内含两个 Rust 可执行文件、构建后的网页、systemd 单元和 `BUILD-INFO`。构建信息记录基础提交、完整工作树源归档的摘要、目标、打包环境的工具链以及产物哈希。两个二进制的 `--version` 与 HTTP `/healthz` 的 `build` 字段带同一个构建标识。带未提交改动的构建也会附上对应源码归档。复用 `GRAPHLAKE_BIN_DIR` 时，打包会校验二进制的目标架构和构建标识，并在 `binary_origin` 标记为 `prebuilt`。

在目标机执行以下命令。已有 `graphlake` 系统用户时跳过 `useradd`。`dist` 中的包应先通过受信任渠道传输，校验文件用于检查传输完整性。

```sh
release=v1.1.4-SOURCE_SHA12
artifact=graphlake-$release-linux-$(uname -m)
sha256sum -c "$artifact.tar.gz.sha256"
sudo useradd --system --user-group --home-dir /var/lib/graphlake --shell /usr/sbin/nologin graphlake
sudo install -d -m 0755 /opt/graphlake/releases
sudo install -d -m 0700 -o graphlake -g graphlake /var/lib/graphlake /var/lib/graphlake/catalog
sudo install -d -m 0750 /etc/graphlake
sudo mkdir -- "/opt/graphlake/releases/$release"
sudo tar -xzf "$artifact.tar.gz" --strip-components=1 -C "/opt/graphlake/releases/$release"
sudo ln -s "releases/$release" /opt/graphlake/current.next
sudo mv -Tf /opt/graphlake/current.next /opt/graphlake/current
sudo install -m 0644 /opt/graphlake/current/graphlake.service /etc/systemd/system/graphlake.service
sudo -u graphlake /opt/graphlake/current/bin/graphlake-admin --catalog-dir /var/lib/graphlake/catalog auth init
sudo systemctl daemon-reload
sudo systemctl enable --now graphlake
curl -fsS http://127.0.0.1:7337/readyz
```

将模型和相对路径引用的本地数据文件迁入 `catalog/`，以服务用户身份读取。迁移已有后台时先停旧服务，保留整个目录和隐藏文件。已有 `.graphlake-auth` 的目录沿用原管理员密码。将 YAML 中 `*_env` 字段引用的源凭据写入 `/etc/graphlake/graphlake.env`，该文件由 root 持有，权限 `0600`。修改后重启服务。使用 `journalctl -u graphlake` 查看启动和退出状态。服务单元配置 4 个共享查询槽，可根据主机内存和可用临时磁盘通过 systemd drop-in 调整。

## 扩展数据源运行依赖

SQL Server 使用内置 TDS 驱动，MongoDB 使用内置 MongoDB 驱动，DuckDB 随程序构建；这些连接器无需额外网关。Oracle 直连使用 ODPI-C，在运行时加载 Oracle Instant Client。请按目标操作系统和架构安装官方客户端及其系统依赖，为 `graphlake` 服务配置可访问的客户端库搜索路径；Oracle 客户端不随发行包分发。旧的 SQL Server、Oracle、MongoDB Trino 入口仍然可用。

每个数据源的 `*_env` 引用也适用于对象存储。显式配置的对象存储密钥不会与另一组默认环境凭据混用。云存储使用默认身份时，需在服务用户环境中提供相应凭据链；GCS 的 `service_account_env` 引用 JSON 内容，Azure 的 `account_key_env` 引用账号密钥。配置与边界见[连接器参考](../docs/reference.zh-CN.md#扩展数据库与对象存储)。

## HTTPS 入口

使用 `--public-url https://graphlake.example` 配置浏览器实际访问的 HTTPS 来源，有自定义端口时将端口写入该 URL。反代负责 TLS，保留原始 `Host`、`Origin` 和 WebSocket 升级请求，后端监听本机回环地址。管理会话 Cookie 带 `Secure`、`HttpOnly` 和 `SameSite=Strict`。转发头不会改变允许的主机名或来源。入口可使用安装包附带的 [HAProxy 配置](haproxy.cfg)，单机时将主后端改为 `127.0.0.1:7337` 并移除备用后端。多 Coordinator 模式见 [故障接管](cluster.md#coordinator-故障接管)。

## 客户端接入

先发布需要开放的模型，例如 `payments`。在「权限管理 → 成员 → 新建成员」选择「服务账号」，使用 `openssl rand -hex 32` 为每个应用生成独立令牌，再将账号加入角色。客户端使用 `svc/账号名称` 及该令牌。HTTP、Bolt 和 Gremlin 使用同一角色和租户范围。查询及导出分别授权。停用账号、移除角色或轮换令牌会关闭该账号现有连接，旧结果和下载票据随授权版本失效。人工密码登录和企业登录仍使用人工账号。

原有 `service-accounts.json` 继续接受原用户名及令牌。升级后旧配置默认提供查询权限。需要保留旧账号对特定模型的完整下载时，显式配置 `export_models`。要启用租户范围，用新令牌创建角色中的服务账号，更新客户端，然后撤销原账号。单机用同目录文件原子替换管理原有配置，权限为 `0600`。Coordinator HA 使用[集群账号接口](cluster.md#coordinator-故障接管)。

```json
{
  "analytics": {
    "token_sha256": "<64 位十六进制 SHA-256>",
    "models": ["payments"],
    "export_models": ["payments"]
  }
}
```

使用同目录、权限 `0600` 的临时文件准备完整 JSON，确认服务用户可读后用 `mv -f` 替换。轮换时更新哈希，撤销时删除账号条目，撤销全部账号时写入 `{}`。每次 HTTP 请求和驱动消息会检查变更，空闲的 Bolt 和 Gremlin 连接也会因旧令牌或旧模型授权失效而关闭。变更检测以服务进程观察到共享文件替换为准。损坏的文件会拒绝服务账号访问，恢复正确文件可重新登录。其他账号保持原连接。

用 `printf '%s' "$GRAPHLAKE_TOKEN" | sha256sum` 计算旧配置所需的哈希。HTTP 使用 Basic 认证。拥有查询权限的服务账号可列出获准的已发布模型、读图结构、查询和探索。取消、结果和下载票据按账号隔离。管理员会话可管理模型和版本，并访问其他账号的查询结果。浏览器带 Origin 的请求仍须同源，管理 Cookie 写请求须提供 Origin。

```ini
[Service]
ExecStart=
ExecStart=/opt/graphlake/current/bin/graphlake-admin --catalog-dir /var/lib/graphlake/catalog --assets-dir /opt/graphlake/current/web --query-slots 4 --driver-model payments --bolt-port 7687
```

`--driver-model` 启用 Gremlin 并设定默认模型，`--bolt-port` 另启用 Bolt。Bolt 通过数据库名选择模型。Gremlin 将遍历源别名 `g` 映射到模型 ID，值 `g` 表示默认模型。账号只有一个授权模型且无权访问默认模型时，默认选该账号的模型。未配置 JSON 文件的旧部署仍可使用环境变量 `GRAPHLAKE_DRIVER_TOKEN`，供驱动账号 `graphlake` 访问默认模型。启用实时轮换时把该账号和需要保留的授权迁入 JSON 文件，并从服务环境移除旧令牌。JSON 文件存在时以其内容为准。

Bolt 和 Gremlin 各有 16 个认证中连接名额和 16 个已认证会话名额，常规认证中连接最多等待 5 秒。Bolt 每个 TCP 对端 IP 最多占用 8 个认证中名额。超过限额时，Bolt 尽力在版本协商和 `HELLO` 后返回可重试的 `FAILURE`，拒绝响应最多并发 4 条，每条最多等待 500 毫秒。经同一个代理地址转发的连接共用该地址的额度。Gremlin 另有一个最多等待 500 毫秒的握手名额，全部占用时返回 HTTP 429。HA 模式下，驱动使用由心跳维护的本地主权窗口准入，权威写入由 etcd 围栏保护。

```sh
curl -u "svc/analytics:$GRAPHLAKE_TOKEN" http://127.0.0.1:7337/api/v1/models
```

```python
import os
from neo4j import GraphDatabase

token = os.environ["GRAPHLAKE_TOKEN"]
with GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("svc/analytics", token)) as driver:
    with driver.session(database="payments", fetch_size=256) as session:
        values = session.execute_read(
            lambda tx: tx.run("RETURN $id AS id", id=9007199254740993).value()
        )
        print(values)
```

```python
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.serializer import GraphSONSerializersV3d0
from gremlin_python.process.anonymous_traversal import traversal

conn = DriverRemoteConnection("ws://127.0.0.1:7337/gremlin", "payments",
    username="svc/analytics", password=token, message_serializer=GraphSONSerializersV3d0())
try:
    g = traversal().with_remote(conn)
    print(g.V().limit(5).to_list())
finally:
    conn.close()
```

Bolt 5.1–5.4 支持直连、自动提交、显式只读事务和托管只读事务。事务固定开始时的发布版本。首次读取时为每个 PostgreSQL 来源建立可重复读快照，并记录使用的 Parquet 对象版本。事务内重新读取已替换的 Parquet 会失败。显式事务支持 PostgreSQL 和 Parquet，其他连接器继续使用自动提交。事务超时和连接空闲超时限制快照保留时间。Bolt 的 NOOP 不延长等待 PULL 的时限。显式事务按 PULL 流式读取。客户端保留未消费的结果并再次发出 RUN 时，服务会先将旧结果的剩余记录溢写到临时文件，每个并存结果最多 64 MiB。Gremlin 支持 WebSocket、SASL PLAIN、GraphSON 3、GraphBinary 1.0，以及两种编码下受支持的只读脚本和字节码。使用 `gremlinpython` 时可选 `GraphBinarySerializersV1()`。

Bolt 按 `PULL` 传输结果，Gremlin 以状态 206 传输中间批次，每批最多 128 条、约 2 MiB。单条驱动记录的编码预算为 1 MiB。Bolt 为并发结果暂存每个结果最多 64 MiB，单进程共享 1 GiB 暂存预算。两种驱动返回原生节点、边和路径，定点小数及超出有符号 64 位的整数使用字符串。图元素标识中的二进制业务键使用小写十六进制表示。Gremlin 路径的步骤标签和标量保真以当前遍历引擎的路径表示为限。任意 Groovy、字节码策略、动态脚本绑定和 Bolt 集群路由仍超出当前范围。Bolt 和 Gremlin 在完成时返回 `model_revision`。

单机操作审计追加到 `.graphlake-auth/audit.jsonl`，未认证事件追加到 `.graphlake-auth/audit-anonymous.jsonl`。两个日志分别在约 32 MiB 轮转并各保留一个 `.previous`，长期留存可采集这两组文件。HA 操作审计输出到服务日志。每行记录时间、主体、协议、操作、模型和状态，省略令牌、查询文本、参数与结果内容。部署时可由宿主机收集日志，备份包含该目录。

协议参考见 [Neo4j Bolt](https://neo4j.com/docs/bolt/current/) 和 [Apache TinkerPop](https://tinkerpop.apache.org/docs/3.7.4/reference/)。

仓库的 [客户端验收脚本](../tests/service_drivers.py) 可对 `payments` PostgreSQL 与 Parquet 夹具重复执行。夹具包含 `CustomerAccount` 和 `Merchant` 节点，以及从前者指向后者的 `Payment` 边，至少有三个节点，且 `payment_id = 2000` 的边有定点小数 `amount = 55.000`。安装 `neo4j==5.28.2`、`gremlinpython==3.7.4` 后，设置测试令牌并执行 `python tests/service_drivers.py http://127.0.0.1:7337 bolt://127.0.0.1:7687 "$revision"`，其中 `revision` 为夹具的活动版本。脚本检查跨源结果、参数与返回值类型、鉴权、只读限制、原生图对象、事务和超过 10,000 行的分批传输，并检查 GraphBinary 连接、谓词和原生路径。

## 运行检查

这些路由接受服务公布的 `127.0.0.1:<端口>` Host，可由同机探针访问。它们不返回模型名称、连接凭据或查询内容。

| 路由 | 判定 |
| --- | --- |
| `GET /healthz` | 进程能响应，返回版本 |
| `GET /readyz` | 检查目录及生效归档，集群另检查 etcd 和至少一个可联系的 Worker。部分模型损坏且仍有可用模型时返回 200 和 `degraded`；全部受影响时返回 503 |
| `GET /api/v1/models/{id}/readiness` | 检查生效版本的来源访问和跨源映射类型。集群要求所有已配置的 Worker 通过同版本检查。全部通过返回 200，发现故障返回 503，未发布返回 409 |
| `GET /metrics` | Prometheus 文本。进程运行时长、查询槽、共享槽内活跃和排队查询、管理 HTTP 状态类别及总耗时、驱动查询按协议和结果计数 |

查询最多排队 16 个。导出有一个执行槽，下载有两个执行槽，同一身份最多占用一个下载槽。导出和下载各最多排队 4 个。每个阶段等待上限 500 毫秒，并计入请求超时。队列已满或到达等待上限返回 429。请求在自己的期限内耗尽时间时返回超时。未领取的导出文件最多保留 4 份、合计 1 GiB，超过预算先淘汰最早生成的文件。票据最多有效 60 秒。已开始的下载在连续 60 秒无数据进展时释放下载槽。

`/readyz` 是服务级探针，降级时给出 `published_models` 和 `unavailable_models` 数量。模型探针需要管理员会话或已授权的服务账号，返回 `model_revision` 和按来源别名组织的布尔值。它检查映射的表、字段、Parquet 对象元数据和关系端点类型。PostgreSQL 执行零行只读查询以验证权限。集群的 `workers` 按配置顺序返回每个 Worker 的检查结果，Worker 从共享存储读取发布归档，并使用自己的来源凭据和路径。探针使用独立执行槽，单次预算最多五秒。这些检查覆盖来源读取与映射结构，数据完整性检查走模型的数据校验流程。

停止服务时先拒绝新的业务请求，再取消活跃查询并给 HTTP 请求最多 10 秒退出。使用 `systemctl stop` 获取备份一致点。 本地表副本、同步配置和任务历史保存在 `catalog/.graphlake-local/`，须随整个 catalog 一起备份。加载过程还需要来源暂存和新副本空间；使用集群 Worker 时，该目录必须在所有读取节点以相同绝对路径可用。详见[本地表存储与部署](../docs/local-tables.zh-CN.md#存储与部署)。

在模型版本历史中选择「下线模型」，或向 `POST /api/v1/models/{id}/unpublish` 提交当前 `active_revision`。下线立即拒绝新查询、导出及邻居展开，保留草稿、版本历史和已生成的结果。收到下线前已接收的查询可继续完成。从历史回退或发布草稿可重新上线。驱动监听可随下线后的模型重启，模型重新上线后即可继续使用。

活动归档丢失或损坏时，后台继续启动，受影响模型的业务请求返回 `503 model_archive_unavailable`。模型列表、详情和服务日志标出故障。可回退到健康版本，或在版本历史上传原始备份 YAML。等效 API 为 `PUT /api/v1/models/{id}/history/{revision}`，正文为 `{"yaml":"…"}`，要求管理员会话和准确匹配的内容哈希。修复当前版本即时恢复查询。若版本清单本身损坏，从备份恢复对应的 `.graphlake-{id}.json`。归档恢复会拒绝符号链接，遇到被替换成符号链接的文件应先在宿主机处理。

## 升级、回退与恢复

1. 在停机窗口运行 `sudo systemctl stop graphlake`，保留上一发布包和 `/opt/graphlake/current` 的目标。将 `catalog/` 整体归档，保留隐藏的凭据、发布历史和版本归档。环境配置含源凭据，应单独以同等权限备份。对目录之外的数据文件及外部数据源分别安排快照和恢复。
2. 校验并解压新包至独立发布目录，沿用安装步骤原子切换 `current` 链接。若单元文件有变化，更新后执行 `systemctl daemon-reload`。启动服务，检查 `/readyz`，再验证管理员登录、指定模型的活动版本和一条有已知结果的源查询。驱动开启时一并测试。
3. 回退二进制时先停止服务，使用相同步骤切回旧发布目录和单元。需要同时恢复旧模型状态时，从停机备份恢复整套目录再启动。升级、回退和恢复后的浏览器会话需要重新登录。探索游标及临时结果也随进程重新建立。

备份和恢复示例。备份文件应写入仅管理员可访问的存储。以原服务用户运行的归档与恢复保留权限；恢复到不同 UID 的主机时按目标 `graphlake` 用户设置归属。

```sh
sudo systemctl stop graphlake
sudo sh -c 'umask 077; tar -C /var/lib/graphlake -czf /root/graphlake-catalog.tar.gz catalog'
sudo sh -c 'cd /root && sha256sum graphlake-catalog.tar.gz > graphlake-catalog.tar.gz.sha256'
# 恢复时，先在同一文件系统上解包和检查，再更换当前目录。
sudo sh -c 'cd /root && sha256sum -c graphlake-catalog.tar.gz.sha256'
restore=$(sudo mktemp -d /var/lib/graphlake/.restore.XXXXXX)
sudo tar --no-same-owner -xzf /root/graphlake-catalog.tar.gz -C "$restore"
sudo chown -R graphlake:graphlake "$restore/catalog"
sudo mv /var/lib/graphlake/catalog "$restore/before-restore"
sudo mv "$restore/catalog" /var/lib/graphlake/catalog
sudo systemctl start graphlake
curl -fsS http://127.0.0.1:7337/readyz
```

## 用户与角色

登录后，主导航的退出入口旁显示当前用户名及管理员或普通用户身份。关闭认证的开发模式显示「未启用认证」。

通过 HTTPS 登录内置 `admin`，打开「权限管理」。在「成员」标签页创建成员，再到「角色」标签页创建角色。为角色选择模型、勾选所需操作，并将用户加入角色。用户可以加入多个角色，有效权限取并集。允许登录控制台的成员在分配角色前也可以登录，模型列表为空。

在「成员 → 编辑 → 接入权限」可分别设置「允许登录控制台」和「允许接入 MCP」。仅接入 MCP 的人工账号不需要控制台密码。管理员可勾选「签发 MCP 令牌」，保存后复制一次性显示的 Bearer 令牌并通过安全渠道交付。该令牌受该账号的模型角色和租户范围限制。关闭控制台登录会拒绝本地密码及企业登录，并使旧会话失效。关闭 MCP 会撤销个人令牌，重新启用时需签发新令牌。服务账号可关闭 MCP 接入，现有 HTTP API、Bolt 和 Gremlin 授权仍由其模型角色控制。升级保留已有账号的接入方式。

| 模型权限 | 可执行操作 |
| --- | --- |
| 查询 | 查询生效版本、查看图结构、展开，恢复或取消本人查询 |
| 导出 | 运行完整导出、保留完整结果并下载 Arrow。需要同时授予同一模型的查询权限 |
| 建模 | 查看版本、修改和校验草稿。允许使用管理员已配置且这个模型已引用的来源与物理表 |
| 发布 | 查看版本、发布、回退、下线、修复归档、刷新 CSR |
| 只读审计 | 查看模型配置、YAML 和版本历史，检查生效模型的来源 |

建模者可以编辑映射和属性列。管理员负责创建模型、配置连接器与凭据，以及扩展物理表范围。请用数据库的专用只读凭据和视图约束可映射的数据列。人工账号通过浏览器和会话 API 使用，也可在 AI 页生成继承本人权限的 MCP 令牌。驱动客户端继续用独立服务账号。在角色中移除成员、修改角色或删除角色时，涉及的成员需重新登录。停用、删除成员或重置密码同样撤销其旧会话。操作日志记录实际操作者，账号和角色的变更还记录目标标识。

角色可给一个模型配置 `partitions` 等值谓词，例如 `{"direct":{"column":"tenant_id","op":"eq","value":1}}`。配置字段须出现在所有节点、边及边端点业务键中。HTTP、Bolt、Gremlin、图探索和授权后的导出均由服务端附加范围。CSR 的共享快照目前只供未限定租户的查询使用。不同角色对同一模型授予互相冲突的租户值时拒绝查询。同一成员拥有其他未设范围的查询角色时，已配置的租户限制仍然有效。敏感属性列用数据库视图或独立模型投影隔离。

普通用户每账号最多四个活动会话。登录失败按账号和来源分别计数。HTTPS 反代部署可将 `GRAPHLAKE_TRUST_PROXY_CLIENT_IP=1` 加入环境文件，前端反代须覆盖 `X-GraphLake-Client-IP`，并禁止外部直连。安装包的 HAProxy 样例已设置覆盖。缺少可信客户端 IP 时，普通有效账号按自身计数，未知账号共享来源限额。管理员使用独立的验证容量，达到共享失败限额后每 12 秒允许一次密码校验。管理员限流响应带 `429` 和 `Retry-After`。普通账号限流返回通用的凭据错误。

单机账号与角色保存在 `catalog/.graphlake-auth/users.json`，权限为 `0600`。停机备份包含该文件。HA 模式把账号和角色提交到 etcd，接管后自动加载。管理 API 使用 `GET /api/v1/users`、`GET /api/v1/roles`、`PUT /api/v1/users/{username}` 与 `PUT /api/v1/roles/{role}`。更新请求传入当前 `version`。删除请求的 JSON 正文为当前版本 UUID。用户可用 `GET /api/v1/my-roles` 查看对各模型的有效权限。

账号接口用 `console_access` 和 `mcp_access` 布尔字段设置接入权限。编辑时省略字段将保留现有设置。管理员用 `issue_mcp_token: true` 为已启用的仅 MCP 人工账号签发或轮换令牌，只有此次保存响应包含 `mcp_token` 明文。恢复后的仅 MCP 账号可以在重新启用时签发新令牌。


## 告警与审计收集

按[生产监控与恢复检查](monitoring.md)配置独立探针和通知。异地恢复使用[恢复清单和 HTTPS 切流验收](cluster.md#绑定恢复点并开放入口)。规则覆盖候选进程、主节点、Worker、客户端入口、模型来源、业务查询恢复、遥测和备份。HA 审计可从每台 Coordinator 的 journal 收集。单机审计写入 `.graphlake-auth/audit.jsonl` 和 `.graphlake-auth/audit-anonymous.jsonl`。角色变更事件 `role_changed` 在本地记录权限和成员的变更前后值。

`/metrics` 保留旧指标，另提供 `graphlake_request_duration_seconds` 直方图，按协议、操作和结果分类。用 `outcome="busy"` 统计限流，用 `outcome="timeout"` 统计超时。查询排队用 `protocol="internal",operation="query_slot"`。查询输出行数、源读取次数、对象读取字节及溢写字节是按进程累计的执行量。执行失败、预览截断或跨 Worker 查询时，这些值反映实际观测的数据量。在集群中，从 Worker 的回环 `/metrics` 抓取时需发送集群 Bearer 令牌，或由现有受信任代理在抓取时注入。

### AI 与 MCP

「侧边栏 → AI」默认展示 Agent 接入指南。用户生成自己的个人 AI 令牌，并将 MCP 地址和 Bearer 令牌配置到 Agent 中；AI 实时继承该用户的查询、建模、发布权限和租户范围，无需单独授权。页面可查看当前可用模型、测试连接、轮换或撤销令牌。管理员在「运行配置 → MCP 服务」独立启停查询服务（`/mcp/query`）和建模服务（`/mcp/modeling`），并设置查询上限和发布开关。Agent 可分别接入两项服务。旧 `/mcp` 地址继续按已启用的能力提供工具。普通用户新建草稿需要已有模板和预授权目标标识的建模角色，不能扩展已批准的数据源与物理表。个人令牌仅用于 MCP，不能访问管理 API；账号停用、删除和密码重置会撤销令牌，角色变更按最新权限生效，备份恢复会清除个人令牌。远程连接使用现有 HTTPS 入口。详见 [MCP 操作说明](../docs/mcp.md)。

### OpenTelemetry 上报

管理员可在「运行配置 → 日志与遥测」编辑本地日志格式、OTLP 地址、信号开关、采样率、上报预算和请求头引用。单机及未启用故障接管的 Coordinator 默认保存到模型目录中的 `.graphlake-telemetry.yaml`。如果启动时已显式指定文件，页面读写该文件。启用故障接管的 Coordinator 须显式指定每台主机的本地配置文件。高级参数和配置文件路径可展开「高级上报设置」查看。确认重新加载后，只有成功读取配置才会替换表单。读取失败时可以继续编辑或重试。请给服务账号配置文件及其父目录的写入权限，以便原子替换。保存后重启对应进程，页面会标出待重启配置，并显示当前信号的失败次数、累计拒收条数与最近完整接收时间。

请求头值从服务进程环境读取。通过页面新增凭据绑定或变更带凭据的 Collector 来源时，请使用 `GRAPHLAKE_OTLP_` 前缀的环境变量，并通过 `GRAPHLAKE_OTLP_ALLOWED_ORIGINS` 指定逗号分隔的允许来源（例如 `https://collector.example.net:4318`）。既有的同来源绑定可继续使用。Worker 和备份命令可按下方方法使用各自的配置文件。

「健康检查」可单次检查已发布模型的来源、端点及 Worker，并显示遥测上报状态。切换页签会保留未保存的遥测配置。生产告警在部署侧维护 [prometheus-rules.yaml](prometheus-rules.yaml)，由 Prometheus 加载，通知渠道由 Alertmanager 管理。

「备份与恢复」可发起[手动备份](cluster.md#在线控制状态备份与恢复)，也显示定时任务和异地校验的最近尝试、最近成功和数据检查点。定时备份默认写入 `/var/lib/graphlake-backups/export-status.json`。异地校验可指定 `--status-file /var/lib/graphlake-backups/verify-status.json`。如使用独立目录，在后台进程设置 `GRAPHLAKE_BACKUP_STATUS_DIR` 指向状态文件所在目录，并为服务用户授权读取。选择「恢复备份」可打开[目标站 HTTPS 恢复页](cluster.md#绑定恢复点并开放入口)，检查已同步的恢复点、确认恢复、查看各阶段结果并执行验收。

单机部署可停止服务后以私有权限复制模型目录，包括隐藏的版本和认证文件。还原时同样先停止服务，并保留文件权限。PostgreSQL、Parquet 及配置引用的其他源数据需要另行备份。

默认关闭 OTLP。复制随包提供的 [telemetry.yaml](telemetry.yaml)，在文件中设置 `enabled: true` 和接收端基址。设置 `GRAPHLAKE_TELEMETRY_CONFIG=/etc/graphlake/telemetry.yaml` 后重启服务，或传入 `--telemetry-config /etc/graphlake/telemetry.yaml`。命令行参数优先。单机、Coordinator、Worker 及备份导出和恢复命令都可加载配置。systemd 服务和备份单元从现有环境文件读取变量，保持原环境文件权限。

```yaml
enabled: true
endpoint: https://collector.example.net:4318
service_name: graphlake
instance_id: coordinator-a
log_format: json
metrics: true
logs: true
traces: true
trace_sample_ratio: 0.1
export_interval_ms: 15000
timeout_ms: 1500
headers_env:
  authorization: GRAPHLAKE_OTLP_AUTHORIZATION
```

- 接收端应启用 OTLP/HTTP Protobuf。GraphLake 会追加 `/v1/metrics`、`/v1/logs`、`/v1/traces`。HTTP 默认限回环，测试私网 Collector 可显式设 `allow_insecure_http: true`。HTTPS 校验系统信任证书。示例中环境变量的值为完整的 `Bearer …`。各进程应使用不同的 `instance_id`，省略时启动生成。
- 信号可分别关闭。根 Trace 默认采样 10%，接入的 W3C Trace Context 用于继续同一条链路。HTTP、Bolt、Gremlin、Worker 和分布式执行阶段采用固定操作名。响应体下载的传输时间由入口代理统计，HTTP Trace 在生成响应时结束。
- `log_format: json` 将运行事件和 HA 审计写为结构化日志，单机审计文件保持 JSONL。可单独开启本地 JSON 日志而关闭 OTLP。上报不包含查询文本、参数、结果、认证头或令牌。远端审计默认省略账号、模型和目标名称，需要时设 `include_audit_identity: true`。审计变更详情保留在本地。
- Collector 请求由 SDK 后台发送。`export_interval_ms` 设置指标上报周期，日志和 Trace 的定时上报间隔最长为 5 秒，各最多排队 2,048 条。接收端异常时，查询与本地采集继续。关注 `graphlake_otlp_export_failures_total` 和本地 `otlp_export_failed`。发送失败的日志与 Trace 可能丢弃，本地审计可继续收集。正常退出时尝试冲刷剩余队列。


### 企业登录代理

如已有企业 IdP，可由受信任的 HTTPS 代理完成 OIDC 或 SAML 登录并强制 MFA，再将稳定的用户名映射到 GraphLake 中预创建的本地账号和角色。为后端设置 `GRAPHLAKE_PROXY_IDENTITY_SECRET`。代理必须从所有外部请求删除 `X-GraphLake-Proxy-User` 和 `X-GraphLake-Proxy-Secret`，仅在通过 IdP 验证时重写为已验证用户名与共享随机凭据。跨主机的代理到后端链路须经私有认证隧道或 mTLS。此模式的后端监听路由不得能被其他客户端直接访问，代理内部日志不能记录共享凭据。

加入代理身份后，普通用户必须在每个会话请求携带有效的代理身份，GraphLake 登录页提供「使用企业账号登录」。内置 `admin` 继续使用本地恢复密码，生产部署可把管理员密码入口限制在维护网络或 VPN。企业会话及 MFA 策略由 IdP 和代理管理，GraphLake 的退出操作清理本应用会话。服务账号继续使用独立令牌。


Arrow 同时允许两个下载，每个身份最多占用一个名额。每次有数据传输时续期 60 秒，停滞 60 秒后释放名额。持续传输可以超过 15 分钟。1 MiB 以内的普通 Arrow 预览存放在内存。托管同步工作区和 SQLite 键索引使用 Coordinator 本机临时盘，`TMPDIR` 可指定本机磁盘目录。连续同步或 CSR 自动构建失败时，自动重试从 30 秒开始退避，最高 16 分钟。手动重试可立即发起。

托管同步上传失败后，重试会按已提交清单校验并复用本地快照文件，仅下载缺失或校验失败的文件。确认已提交代次后，后续同步会在本地回收旧分片。
