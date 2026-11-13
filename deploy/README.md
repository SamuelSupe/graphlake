# GraphLake 单机 Linux 部署

服务包含管理界面、HTTP API，以及可选的 Bolt 和 Gremlin 客户端入口。HTTP 与 Bolt 均监听 `127.0.0.1`。同机客户端可直接连接；远程维护可用 SSH 将对应端口转发到本机的相同端口，例如 `ssh -N -L 7337:127.0.0.1:7337 -L 7687:127.0.0.1:7687 graph-host`。管理界面和客户端在本机仍使用 `127.0.0.1`。部署边界为受信任的单机或 SSH 隧道，回环传输使用明文协议。

## 构建与安装

在与目标机架构和 glibc 兼容的 Linux 上安装仓库指定版本的 Rust、C 编译工具链和 Node.js 24。在仓库根目录运行 `sh deploy/package.sh v0.1.0 dist`。包内含两个 Rust 可执行文件、构建后的网页和 systemd 单元。已有构建产物时，可用 `GRAPHLAKE_BIN_DIR`、`GRAPHLAKE_WEB_DIST` 指定其目录。

在目标机执行以下命令。已有 `graphlake` 系统用户时跳过 `useradd`。`dist` 中的包应先通过受信任渠道传输，校验文件用于检查传输完整性。

```sh
release=v0.1.0
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

## 客户端接入

先发布需要开放的模型，例如 `payments`。为每个应用生成独立随机令牌。配置文件只保存令牌的 SHA-256 和显式模型授权，令牌原文由调用方保存。文件位于 `<模型目录>/.graphlake-auth/service-accounts.json`，由服务用户读取，权限 `0600`。修改、撤销或轮换凭据后重启服务。

```json
{
  "analytics": {
    "token_sha256": "<64 位十六进制 SHA-256>",
    "models": ["payments"]
  }
}
```

令牌可用 `openssl rand -hex 32` 生成。用 `printf '%s' "$GRAPHLAKE_TOKEN" | sha256sum` 计算哈希。HTTP、Bolt 和 Gremlin 使用同一个账号名和令牌。HTTP 使用 Basic 认证。服务账号可列出获准的已发布模型、读图结构、查询、探索和导出，取消、结果和下载票据按账号隔离。管理员会话可管理模型和版本，并访问其他账号的查询结果。浏览器带 Origin 的请求仍须同源，管理 Cookie 写请求须提供 Origin。

```ini
[Service]
ExecStart=
ExecStart=/opt/graphlake/current/bin/graphlake-admin --catalog-dir /var/lib/graphlake/catalog --assets-dir /opt/graphlake/current/web --query-slots 4 --driver-model payments --bolt-port 7687
```

`--driver-model` 启用 Gremlin 并设定默认模型，`--bolt-port` 另启用 Bolt。Bolt 通过数据库名选择模型。Gremlin 将遍历源别名 `g` 映射到模型 ID，值 `g` 表示默认模型。账号只有一个授权模型且无权访问默认模型时，默认选该账号的模型。原有 `graphlake` 和 `GRAPHLAKE_DRIVER_TOKEN` 仍可用于驱动，权限限定为 `--driver-model` 指定的模型。

```sh
curl -u "analytics:$GRAPHLAKE_TOKEN" http://127.0.0.1:7337/api/v1/models
```

```python
import os
from neo4j import GraphDatabase

token = os.environ["GRAPHLAKE_TOKEN"]
with GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("analytics", token)) as driver:
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
    username="analytics", password=token, message_serializer=GraphSONSerializersV3d0())
try:
    g = traversal().with_remote(conn)
    print(g.V().limit(5).to_list())
finally:
    conn.close()
```

Bolt 5.1–5.4 支持直连、自动提交、显式只读事务和托管只读事务。事务固定开始时的发布版本。首次读取时为每个 PostgreSQL 来源建立可重复读快照，并记录使用的 Parquet 对象版本。事务内重新读取已替换的 Parquet 会失败。显式事务支持 PostgreSQL 和 Parquet，其他连接器继续使用自动提交。事务超时和连接空闲超时限制快照保留时间。Gremlin 支持 WebSocket、SASL PLAIN、GraphSON 3 以及受支持的只读 bytecode。

Bolt 按 `PULL` 传输结果，Gremlin 以状态 206 传输中间批次，每批最多 128 条、约 2 MiB。单条记录保留编码预算。两种驱动返回原生节点、边和路径，定点小数及超出有符号 64 位的整数使用字符串。Gremlin 路径的步骤标签和标量保真以当前遍历引擎的路径表示为限。GraphBinary、任意 Groovy、字节码策略、动态脚本绑定和 Bolt 集群路由仍超出当前范围。Bolt 和 Gremlin 在完成时返回 `model_revision`。

操作审计追加到 `.graphlake-auth/audit.jsonl`。每行记录时间、主体、协议、操作、模型和状态，省略令牌、查询文本、参数与结果内容。部署时可由宿主机收集日志，备份包含该目录。

协议参考见 [Neo4j Bolt](https://neo4j.com/docs/bolt/current/) 和 [Apache TinkerPop](https://tinkerpop.apache.org/docs/3.7.4/reference/)。

仓库的 [客户端验收脚本](../tests/service_drivers.py) 可对 `payments` PostgreSQL 与 Parquet 夹具重复执行。夹具包含 `CustomerAccount` 和 `Merchant` 节点，以及从前者指向后者的 `Payment` 边，至少有三个节点，且 `payment_id = 2000` 的边有定点小数 `amount = 55.000`。安装 `neo4j==5.28.2`、`gremlinpython==3.7.4` 后，设置测试令牌并执行 `python tests/service_drivers.py http://127.0.0.1:7337 bolt://127.0.0.1:7687 "$revision"`，其中 `revision` 为夹具的活动版本。脚本检查跨源结果、参数与返回值类型、鉴权、只读限制、原生图对象、事务和超过 10,000 行的分批传输。

## 运行检查

这些路由接受服务公布的 `127.0.0.1:<端口>` Host，可由同机探针访问。它们不返回模型名称、连接凭据或查询内容。

| 路由 | 判定 |
| --- | --- |
| `GET /healthz` | 进程能响应，返回版本 |
| `GET /readyz` | 已发布模型的目录、版本清单、活动归档、内容哈希和配置结构可读取。启用驱动时指定模型也必须处于已发布状态 |
| `GET /metrics` | Prometheus 文本。进程运行时长、查询槽、共享槽内活跃和排队查询、管理 HTTP 状态类别及总耗时、驱动查询按协议和结果计数 |

查询最多排队 16 个，导出和下载各有一个执行槽，分别最多排队 4 个。每个阶段等待上限 500 毫秒，并计入请求超时。队列已满或到达等待上限返回 429。请求在自己的期限内耗尽时间时返回超时。未领取的导出文件最多保留 4 份、合计 1 GiB，超过预算先淘汰最早生成的文件。票据最多有效 60 秒。

`/readyz` 的 `503` 表示目录检查失败、另一个检查正在执行或服务正在停止。探针无数据源网络请求。用代表性只读业务查询另行探测 PostgreSQL、Parquet 等来源。停止服务时先拒绝新的业务请求，再取消活跃查询并给 HTTP 请求最多 10 秒退出。应使用 `systemctl stop` 获取备份一致点。

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
