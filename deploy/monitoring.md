# 生产监控与恢复检查

在能通过正式地址访问 HTTPS、Bolt 和 Gremlin 的监控主机安装 Python 3。安装包中的 `graphlake-monitor` 使用 Python 标准库，以独立进程检查入口、候选 Coordinator、Worker、模型和备份。示例配置见 [monitor.json](monitor.json)。监控服务默认在回环端口 9337 提供 `/metrics`。Prometheus 在同机抓取，跨主机抓取可用认证隧道。

1. 在每个 Coordinator 的启动参数加入独立的 `--status-port`。候选节点等待主权时仍能在该回环端口响应 `/status`。用私有 SSH 转发把这些端口和 Worker 的回环端口映射到监控主机。Worker 探针需要集群 Bearer 令牌。也可通过校验证书的私有 HTTPS 转发访问。请给每个监控目标使用固定的标识。
2. 把 `ingress` 设置为客户端实际使用的地址。HTTPS 检查服务身份，并通过同一个入口执行模型探针与业务查询。Bolt 进行 5.4 协商，Gremlin 执行 WebSocket 升级并验证服务端握手。启用 TLS 的目标校验系统 CA，可用 `ca_file` 指定部署使用的 CA 文件。回环测试可使用明文。仅配置部署已启用的协议。
3. 创建专用的角色服务账号，授予待监控模型的查询权限。模型会随发布自动发现。为每个模型设置一条已知结果的只读查询，限制返回行数，并对多行结果显式排序。结果按 API 的字符串或 null 表达比较，同时检查查询版本和截断标记。模型恢复需要来源探针及业务查询都通过。发布新模型后要同步配置查询。模型下线后移除对应配置。定时状态最多发现 256 个模型。
4. 将令牌写入 root 所有、权限 0600 的 `/etc/graphlake/monitor.env`。把实例配置放入 `/etc/graphlake/monitor.json`，允许服务账号读取。安装并启动 `graphlake-monitor.service`。备份状态文件由同一主机的 GraphLake 用户读取。异地运行独立的监控进程，并在 Prometheus 中加入对应目标。
5. 在 Prometheus 中加载随包的 `prometheus-rules.yaml`，或从「运行配置」下载已保存版本。独立监控端的抓取任务名为 `graphlake-monitor`。如抓取实际生产入口的 `/metrics`，使用 `graphlake-active`。两台 Coordinator 的候选端口交给监控进程，避免把等待主权的候选误判为应用不可用。配置 Alertmanager 路由，并开启接收端的恢复通知。

可用 `graphlake-monitor --config /etc/graphlake/monitor.json --once` 检查单轮结果。持续服务使用 `--state-file` 保存故障标签，重启后仍需成功执行已知结果查询才清除原来源故障。正常探针完成后如果来源仍可读但查询返回错误或错值，告警继续保留。HTTP 查询已知的来源连接失败使用 503 和 `source_unavailable`。配置或查询语义错误仍有独立计数 `graphlake_query_errors_total{category="configuration"}`。访问失败不输出令牌、查询文本或连接串。

## 遥测与备份

`graphlake_otlp_last_success_timestamp_seconds` 表示最近一次完整接收。Collector 部分拒收会增加 `graphlake_otlp_rejected_total`，并设置 `graphlake_otlp_last_rejected`。收到后续完整接收后，该批次状态清除并更新成功时间。传输失败和无法解析的响应计入 `graphlake_otlp_export_failures_total`。没有信号时，最近成功时间保持原值。

随包的备份单元把每次导出结果写到 `/var/lib/graphlake-backups/export-status.json`。异地副本站在同步后对最新档案运行

```sh
graphlake-admin backup verify --file /srv/graphlake-replica/latest.json \
  --status-file /var/lib/graphlake-backups/verify-status.json
```

导出失败或副本校验失败会更新状态，但保留上次成功时间。异地备份年龄同时检查成功校验时间和原始归档生成时间。旧格式备份仍可离线校验。无原始时间的档案无法证明达到恢复点预算，重新导出后同步至副本站即可生成完整状态。演练应分别覆盖写入失败、异地副本损坏及备份过期，并在恢复后检查归档校验与告警解除。

Coordinator 的回环状态端口同时提供 `/metrics`。各节点应分别以固定的 Prometheus 实例标签抓取。设置 `GRAPHLAKE_HA_METRICS_FILE=/var/lib/graphlake/ha-metrics.json`，使用每个节点独立的本地可写文件，以便在失权退出、由 systemd 重启为候选节点后继续暴露计数。主权暂不明时，`/status` 返回 `leader_uncertain`，`ready=false`。

| 指标 | `reason` |
| --- | --- |
| `graphlake_etcd_confirmation_failures_total` | `timeout`、`unavailable`、`owner_changed`、`lease_expired`、`safety_window_expired` |
| `graphlake_coordinator_leadership_losses_total` | `owner_changed`、`lease_expired`、`safety_window_expired` |
| `graphlake_coordinator_promotions_total` | `initial`、`leader_released`、`safety_window_expired`、`lease_disappeared`。前主没能提交退出原因时使用 `lease_disappeared` |

同一节点的业务端口也暴露这组计数。如果同时抓取两套端口，按节点状态端口统计，避免重复计数。
