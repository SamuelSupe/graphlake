# AI clients with MCP

GraphLake exposes two authenticated MCP services on the existing administration listener. Use **侧边栏 → AI** (`/ai`) to connect an Agent and **运行配置 → MCP 服务** to manage the services. Both services are disabled by default. Publication is an additional opt-in under the modeling service.

| Service | Path | Tools |
| --- | --- | --- |
| `graphlake-query` | `/mcp/query` | List queryable models, inspect published schema and run read-only queries. |
| `graphlake-modeling` | `/mcp/modeling` | List permitted models and targets, read, create, validate and save drafts, and publish when authorized. |

Each service enforces its own tool set for both discovery and invocation. The existing `/mcp` URL remains available for clients configured before the split and exposes tools from the enabled services according to the user's roles. Existing policy files continue to honor their master switch and capability flags.

## Connect an Agent as yourself

The **AI** sidebar opens a three-step Agent setup guide: generate a personal token, add GraphLake to the Agent, then start with a copyable query or modeling prompt. Token management, JSON configuration, optional connection diagnostics and permission details expand on demand. Accounts with console and MCP access can manage their own personal AI token. MCP checks the account's access permission and applies the token owner's current **查询**, **建模**, **发布** roles and tenant restrictions.

1. Sign in as the user the Agent should represent. Expand **我的权限** when needed to check available models. An administrator assigns existing model roles through **权限管理**; audit-only or unassigned users cannot generate a token or invoke MCP, but can still revoke a previously issued token.
2. Select **生成个人令牌**, then **复制令牌**. The server generates a random 256-bit token and stores only its SHA-256 digest. Each user has one active personal token. A token expires 90 days after issuance, including tokens issued before the upgrade. The plaintext is available only in the issuing page until leaving, refreshing or locking it; copy it to the Agent's credential configuration, not its conversation. Do not use the account password. An existing token can be reused; expand **管理令牌** to rotate or revoke it.
3. The guide shows connection settings for the services the current user's roles allow. It opens the settings directly when one service is allowed and shows **选择 MCP 服务** when both are allowed. Query requires query permission. Modeling requires modeling or publishing permission. Register each service you need as a separate Streamable HTTP entry in the Agent with its own name and URL. Both entries use Bearer Token authentication and can use the same personal token. For a JSON client, expand **使用 JSON 配置** and replace `<PERSONAL_AI_TOKEN>` with the token in each entry's `Authorization` header.
4. Optionally expand **验证连接** and run **测试 MCP 连接** for the selected service. Tests can reuse a token generated on the current page. If you return later, paste your saved token into the test field. The test completes initialization and shows the authenticated owner and model/tool counts. Expand **查看模型与工具** for its model capabilities, modeling targets if available, and tools. It does not query data or write models. The test supports cancellation and clears manually entered tokens afterwards. No token is saved in browser storage.
5. Copy a query or modeling prompt from **开始使用** into the Agent. Examples appear only for the current user's available model capabilities. Ask it to list models and inspect a schema before querying. For modeling, first read an authorized template, validate edits, and save a draft; publish only with explicit user intent. For ordinary users, new models require modeling permission on both the template and future target ID, and may only reuse the template's approved sources and physical tables.

Administrators can enable query and modeling independently under **运行配置 → MCP 服务**. When both are disabled, **前往 MCP 配置** in the AI guide takes administrators directly to this tab. Each service applies the owner's live model permissions in addition to its own tool set. Query results are limited to 1–500 rows and a 1–60000 ms timeout, capped by the existing server timeout. The regular queue, memory and tenant controls still apply. Built-in administrator tokens represent the administrator and can use all models through the tools each service exposes.

MCP edits stay in place when switching tabs within **运行配置**. Save or discard pending edits before returning to the AI guide to test the connection. Revision conflicts retain edits and require explicitly discarding them before reloading.

## Personal token lifecycle

- **重新生成令牌** invalidates the previous token. **撤销令牌** removes access without logging out the owner or changing model roles. Both use the last read token revision to prevent a stale page from replacing or revoking a newer token.
- Role and tenant changes affect the next request using the same token and interrupt requests authenticated before the change. No token reissue is needed for a permission change. If all query/modeler/publisher permissions are removed, MCP access is denied.
- Disabling or deleting the owner, or resetting their password, removes their personal token. Recreating the same username does not restore access. Built-in administrator tokens are bound to the administrator's authentication configuration and become invalid when its password hash changes.
- Personal tokens authenticate only the MCP endpoints. Token management, user and role edits, and service settings require the user's browser session and the applicable permissions. Audit events attribute MCP operations to the owning user.
- Token digests persist with the account directory and survive normal restarts. Restoring a backup clears all personal AI tokens; users must issue new tokens after recovery. An uncertain HA token update is resolved by refreshing token status and explicitly rotating again if the plaintext response was lost.

Example for clients accepting `mcpServers` with HTTP headers:

```json
{
  "mcpServers": {
    "graphlake-query": {
      "url": "https://graphlake.example/mcp/query",
      "headers": {
        "Authorization": "Bearer <PERSONAL_AI_TOKEN>"
      }
    },
    "graphlake-modeling": {
      "url": "https://graphlake.example/mcp/modeling",
      "headers": {
        "Authorization": "Bearer <PERSONAL_AI_TOKEN>"
      }
    }
  }
}
```

Personal Bearer tokens act as the account that owns them. For MCP-only accounts, an administrator can turn off console login and issue a token from **权限管理 → 成员**. The token appears once and the member can use it directly in an Agent. Turning off MCP access revokes the personal token. Browser cookies and human passwords do not authenticate MCP.

Role-based service accounts use Bearer or Basic credentials (`svc/name:token`) with their explicit MCP model grants. Modeling and publication require the corresponding model grants and server settings. Legacy `service-accounts.json` accounts remain query-only and use Basic authentication. Choose a client that supports pre-provisioned credentials via custom HTTP headers or bearer tokens. Keep tokens out of URLs.

On a local development server use the exact loopback address displayed by GraphLake. For remote access use the configured `--public-url` HTTPS origin and preserve `Host` and `Authorization` at the reverse proxy. Requests carrying an `Origin` must match that origin; the service does not enable cross-origin browser access.

## Tools and workflows

| Tool | Use | Required access |
| --- | --- | --- |
| `list_models` | Visible models, active revisions, per-model capabilities and unused authorized modeling target IDs | Authenticated user or compatible service account |
| `get_schema` | Published labels, keys, properties and relationship endpoints | Query enabled + model query role |
| `query` | Cypher, SQL or Gremlin; optional Cypher parameter object | Query enabled + model query role |
| `get_model` | Draft YAML, revision and active revision | Modeling enabled + modeler or publisher role |
| `validate_model` | Source columns and mapping validation; optional key/endpoint data checks | Modeling enabled + modeler role |
| `create_model` | New unpublished draft using an authorized template | Modeling enabled + modeler roles on template and target |
| `save_model` | Version-checked draft update | Modeling enabled + modeler role |
| `publish_model` | Validate data, then activate a saved draft | Modeling and publication enabled + publisher role |
| `get_mutation` | Recover an HA mutation outcome | Modeling enabled + access to the owned receipt's model; HA only |

Tools are discovered according to both global settings and the caller's roles. Every invocation rechecks permission; hiding a tool is not the authorization boundary. Draft configuration requires modeler or publisher rights (or the built-in administrator). Queries always use a published version. Query results contain column types, string/null cell values, model/source versions and `truncated`; refine a query when the row or existing preview byte limit is reached. MCP does not provide full-result export or change source records.

The `query` tool accepts `freshness` with optional `max_observation_age_seconds`, `max_reconciliation_age_seconds`, `require_current_csr` and `require_consistent_snapshot`. These constraints can tighten the published model policy. Supply the applicable `csr_generation` for a Cypher request that requires the current CSR. Results include the generation used, current local-table generation, observation and reconciliation timestamps, and how deletes are captured. A request with missing or stale evidence returns a conflict before query results are delivered.

`list_models.models[].capabilities` contains `query`, `read_model`, `modeling` and `publish`. These combine global MCP settings with the account's model roles; `query` additionally requires a published version without an archive error. They describe authorization, not data-source reachability or successful model validation. Invocations still recheck permissions and tenant scope. `modeling_targets` contains only authorized IDs that have not been created yet; creation also requires an existing template with modeling permission.

Typical query sequence: `list_models` → `get_schema` → `query`. Supply `model_revision` from `get_schema` when a query must use the inspected revision. Omitting it selects the current published revision at invocation time.

Typical modeling sequence:

1. Read a template with `get_model`; call `validate_model` without YAML to inspect its approved column types.
2. Create a draft with `create_model`, or edit an existing draft using `save_model` and its exact `revision`.
3. Call `validate_model` with `check_data: true`; errors are returned with `isError: true` and existing validation locations.
4. Publish only when requested by the user. Supply the draft `revision` and the previously read `active_revision` (explicitly `null` if unpublished). Otherwise review and publish in the browser.

Draft writes do not change the active model. On a revision conflict, re-read and reconcile the draft rather than automatically overwriting it. Source/physical-table expansion remains an administrator action. Model text and returned data are untrusted content for the AI client, not instructions.

## End-to-end verification with Codex

Codex CLI 0.169.0-alpha.11.1 with `gpt-6.1-sol` ran these checks against isolated GraphLake instances on OrbStack Linux. The test data came from the [Parquet demo](quickstart.md#build-and-query-the-demo), with three `Account` nodes and four `Transfer` edges. The fixtures had two published models over those files, `graph` and `ai-ui`. Each test account had a single role on `graph`, either query, modeler, or publisher. None had access to `ai-ui`.

To reproduce the checks, use a fresh demo catalog for each run. Copy `graph.yaml` to `ai-ui.yaml` and give the copy a distinct display name before starting the workbench. Enable both services and publication under **运行配置 → MCP 服务**, then create the three accounts and issue one personal token per account. Use the corresponding Codex server entry for each run. Set `GRAPHLAKE_QUERY_TOKEN` to the query account's token. Set `GRAPHLAKE_MODELING_TOKEN` to the modeler's token for draft operations and to the publisher's token for publication. Substitute the instance's exact MCP address if it uses a different origin or port.

```toml
[mcp_servers.graphlake_query]
url = "http://127.0.0.1:7337/mcp/query"
bearer_token_env_var = "GRAPHLAKE_QUERY_TOKEN"

[mcp_servers.graphlake_modeling]
url = "http://127.0.0.1:7337/mcp/modeling"
bearer_token_env_var = "GRAPHLAKE_MODELING_TOKEN"
```

These runs used Codex's per-tool approval setting for `save_model` or `publish_model` in the relevant disposable test process. If your Agent prompts for confirmation, approve the intended draft save or publication. See the [Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).

### Query and save a draft

1. Connect the query account to `/mcp/query`. Call `list_models` and `get_schema` for `graph`, then run the two Cypher statements below as separate `query` calls. The Agent saw only `graph` and received counts of **3 accounts** and **4 transfers**.

   ```cypher
   MATCH (n:Account) RETURN count(n) AS accounts
   MATCH (:Account)-[r:Transfer]->(:Account) RETURN count(r) AS transfers
   ```

2. Connect the modeler to `/mcp/modeling`. Call `get_model` for `graph`, change only the top-level `graph` name, and pass the full YAML to `validate_model`. Send that YAML, the draft `revision` from `get_model`, and a new `mutation_id` to `save_model`. The Agent read the new name and revision back with `get_model`. The saved YAML on disk matched the update and `active_revision` remained unchanged.
3. Have the query Agent call `query` on `ai-ui`. Have the modeling Agent call `save_model` on `ai-ui` with the same modified YAML, the target draft's current revision, and a new `mutation_id`. Both calls reached GraphLake and returned a tool error with `structuredContent.code = "model_forbidden"`. The server audit recorded `403` for each and `ai-ui` retained its original draft revision. A separate Codex run also tried to initialize `/mcp/modeling` with the query-only token and received HTTP `403` during the handshake.

### Publish and query the new version

1. Start with the original demo mapping. As the modeler, use `get_model`, then rename the node label `Account` to `AgentAccount` in `nodes` and both `Transfer` endpoints. Run `validate_model` with `check_data: true`, call `save_model`, and read back the new draft revision. Before publication, the query Agent still saw the old `Account` label and counted 3 accounts on the old active revision. Both this query and the later post-publication query omitted `model_revision`.
2. Check that `tools/list` for the modeler includes `save_model` and omits `publish_model`. To test an actual publish request with only modeling permission, temporarily add the publisher role so Codex discovers the tool, then remove that role before forwarding its `publish_model` request to GraphLake. This tested the same personal token with its current role at the moment of invocation. The server returned `model_forbidden`, its audit recorded `403`, and both the saved draft and the old active revision remained in place.
3. Connect the separate publisher-only account to `/mcp/modeling`. Call `publish_model` with `model: "graph"`, the saved draft `revision`, the current `active_revision`, and a new `mutation_id`. The call succeeded, and the server returned the saved draft revision as the new `active_revision`.
4. Connect the query account to `/mcp/query` and call `get_schema` again. The Agent received the new `AgentAccount` label and the just-published schema revision. It then ran `MATCH (n:AgentAccount) RETURN count(n) AS accounts` and received **3** with `model_revision` equal to the version returned by `publish_model`. This verified the default query path switched to the new published version.

## Protocol and persistence

The endpoint implements the JSON-response form of [MCP Streamable HTTP](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) for **2025-03-26, 2025-06-18 and 2025-11-25**. It supports `initialize`, `ping`, `tools/list`, and `tools/call`. Each POST contains one JSON-RPC message and advertises `Accept: application/json, text/event-stream`; post-initialization clients send `MCP-Protocol-Version`. Notifications are acknowledged with empty HTTP 202 responses. No session ID is allocated. GET/DELETE return 405 because there is no standalone SSE stream or protocol session. There is no stdio entry point, subscription stream, server-initiated sampling, OAuth flow, or 2026 protocol implementation. Queries terminate through the configured timeout; cancellation notifications do not control running calls.

MCP policy is stored with the existing account/role directory (`.graphlake-auth/users.json` for standalone service; the `users` control record for HA). Administrator-only GET/PUT `/api/v1/mcp/settings` uses an `If-Match` revision to prevent lost configuration updates. Changes apply to the next MCP request; already accepted operations may finish within their existing budgets. Changing MCP policy does not interrupt Bolt, Gremlin or regular HTTP connections. Account disablement, role changes and token rotation retain the existing revocation behavior. Configuration is included in account backups; restored accounts remain disabled until their credentials and roles are restored through the existing recovery process.

GET `/api/v1/mcp/access` returns `account_allowed`, the user's permitted model scope and each service's endpoint, enabled state and role eligibility. GET/POST/DELETE `/api/v1/mcp/token` read, issue/rotate or revoke only that user's token. POST and DELETE accept `{ "revision": null }` for no existing token or the UUID returned by the previous GET. Administrators issue tokens for MCP-only accounts through the [member update API](../deploy/README.md#用户与角色).

Every model mutation accepts a caller-generated `mutation_id` UUID. In HA mode this is the existing durable idempotency key: retry the identical operation with the same ID after a lost response, or read `get_mutation`. A missing receipt can still be in flight. A committed result includes the mutation ID. Standalone mode relies on create-only writes and optimistic revisions; it does not persist idempotency receipts. An accepted HA mutation may finish after a disconnect, and a committed operation remains committed; re-read its model or receipt to determine the outcome.

MCP tool calls use the existing audit sink (`protocol: mcp`) and operation metrics. Audit entries identify account, tool, model and outcome, without copying tokens, query text or returned rows.
