use super::*;

const TOKEN: &str = "graphlake-mcp-integration-agent-token";
const READER: &str = "graphlake-mcp-integration-reader-token";

fn rpc(server: &Server, token: &str, method: &str, params: Value) -> reqwest::RequestBuilder {
    rpc_at(server, "/mcp", token, method, params)
}

fn rpc_at(
    server: &Server,
    path: &str,
    token: &str,
    method: &str,
    params: Value,
) -> reqwest::RequestBuilder {
    server
        .http
        .post(format!("{}{path}", server.base))
        .bearer_auth(token)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
}

async fn call(server: &Server, token: &str, name: &str, arguments: Value) -> Result<Value> {
    call_at(server, "/mcp", token, name, arguments).await
}

async fn call_at(
    server: &Server,
    path: &str,
    token: &str,
    name: &str,
    arguments: Value,
) -> Result<Value> {
    let result: Value = rpc_at(
        server,
        path,
        token,
        "tools/call",
        json!({"name":name,"arguments":arguments}),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    assert!(result.get("error").is_none(), "{result}");
    Ok(result["result"].clone())
}

async fn configure(server: &Server, patch: Value) -> Result<Value> {
    let snapshot: Value = server
        .request(Method::GET, "mcp/settings")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut config = snapshot["config"].clone();
    for (key, value) in patch.as_object().unwrap() {
        config[key] = value.clone();
    }
    Ok(server
        .request(Method::PUT, "mcp/settings")
        .header(
            "If-Match",
            format!("\"{}\"", snapshot["revision"].as_str().unwrap()),
        )
        .json(&config)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

#[tokio::test]
async fn mcp_queries_models_and_enforces_account_policy_across_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    payments_file(&root.path().join("payments.parquet"), false, &[8, 9, 10])?;
    let yaml = "graph: Payments\nsources:\n  src:\n    type: parquet\n    tables: { rows: [./payments.parquet] }\nnodes:\n  Payment: { source: src.rows, key: [tenant_id, payment_id], properties: [channel] }\n";
    fs::write(root.path().join("payments.yaml"), yaml)?;
    fs::write(root.path().join("private.yaml"), yaml)?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    for (name, token, grants) in [
        (
            "agent",
            TOKEN,
            json!({"payments":["query","modeler","publisher"],"ai-model":["query","modeler","publisher"]}),
        ),
        ("reader", READER, json!({"payments":["query"]})),
    ] {
        server
            .request(Method::PUT, &format!("users/{name}"))
            .json(&json!({"service":true,"token":token,"disabled":false}))
            .send()
            .await?
            .error_for_status()?;
        server
            .request(Method::PUT, &format!("roles/{name}"))
            .json(&json!({"members":[name],"grants":grants}))
            .send()
            .await?
            .error_for_status()?;
    }
    assert_eq!(
        rpc(&server, TOKEN, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    for method in [Method::GET, Method::PUT] {
        assert_eq!(
            server
                .guest(method.clone(), "mcp/settings")
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            server
                .guest(method, "mcp/settings")
                .basic_auth("svc/agent", Some(TOKEN))
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    let enabled = configure(
        &server,
        json!({"enabled":true,"modeling":true,"max_rows":1}),
    )
    .await?;
    let query_path = "/mcp/query";
    let model_path = "/mcp/modeling";
    for (path, service, expected_tools, unavailable_tool, arguments) in [
        (
            query_path,
            "graphlake-query",
            vec![
                "list_models",
                "get_schema",
                "list_tasks",
                "run_task",
                "query",
            ],
            "get_model",
            json!({"model":"payments"}),
        ),
        (
            model_path,
            "graphlake-modeling",
            vec![
                "list_models",
                "get_model",
                "validate_model",
                "validate_model_impact",
                "create_model",
                "save_model",
            ],
            "query",
            json!({"model":"payments","language":"cypher","query":"RETURN 1"}),
        ),
    ] {
        let init: Value = rpc_at(&server, path, TOKEN, "initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).send().await?.error_for_status()?.json().await?;
        assert_eq!(init["result"]["serverInfo"]["name"], service);
        let tools: Value = rpc_at(&server, path, TOKEN, "tools/list", json!({}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let names = tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, expected_tools);
        let denied = call_at(&server, path, TOKEN, unavailable_tool, arguments).await?;
        assert_eq!(denied["structuredContent"]["code"], "model_forbidden");
        let scope = call_at(&server, path, TOKEN, "list_models", json!({})).await?;
        let capabilities = &scope["structuredContent"]["models"][0]["capabilities"];
        assert_eq!(capabilities["query"], path == query_path);
        assert_eq!(capabilities["modeling"], path == model_path);
        assert_eq!(
            scope["structuredContent"]["modeling_targets"],
            if path == model_path {
                json!(["ai-model"])
            } else {
                json!([])
            }
        );
        assert_eq!(
            rpc_at(&server, path, TOKEN, "ping", json!({}))
                .header("Origin", "https://untrusted.example")
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        rpc_at(&server, model_path, READER, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .request(Method::PUT, "mcp/settings")
            .header("If-Match", "\"stale\"")
            .json(&enabled["config"])
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    let mut invalid_config = enabled["config"].clone();
    invalid_config["max_rows"] = json!(0);
    assert_eq!(
        server
            .request(Method::PUT, "mcp/settings")
            .header(
                "If-Match",
                format!("\"{}\"", enabled["revision"].as_str().unwrap())
            )
            .json(&invalid_config)
            .send()
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        rpc(&server, "incorrect-token", "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .http
            .post(format!("{}/mcp", server.base))
            .header("Cookie", server.cookie.as_ref().unwrap())
            .header("Origin", &server.base)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        rpc(&server, TOKEN, "ping", json!({}))
            .header("Origin", "https://untrusted.example")
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        rpc(&server, TOKEN, "ping", json!({}))
            .header("Host", "untrusted.example")
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .http
            .get(format!("{}/mcp", server.base))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    let initialized: Value = rpc(&server, TOKEN, "initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}})).send().await?.error_for_status()?.json().await?;
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    assert!(initialized["result"]["capabilities"]["tools"].is_object());
    let notification = server
        .http
        .post(format!("{}/mcp", server.base))
        .bearer_auth(TOKEN)
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .send()
        .await?;
    assert_eq!(notification.status(), StatusCode::ACCEPTED);
    assert!(notification.bytes().await?.is_empty());
    let tools: Value = rpc(&server, READER, "tools/list", json!({}))
        .send()
        .await?
        .json()
        .await?;
    let names = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "list_models",
            "get_schema",
            "list_tasks",
            "run_task",
            "query"
        ]
    );
    let list = call(&server, READER, "list_models", json!({})).await?;
    assert_eq!(
        list["structuredContent"]["models"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(list["structuredContent"]["models"][0]["id"], "payments");
    assert_eq!(
        list["structuredContent"]["models"][0]["capabilities"],
        json!({"query":true,"read_model":false,"modeling":false,"publish":false})
    );
    assert_eq!(list["structuredContent"]["modeling_targets"], json!([]));
    let scope = call(&server, TOKEN, "list_models", json!({})).await?;
    assert_eq!(
        scope["structuredContent"]["modeling_targets"],
        json!(["ai-model"])
    );
    assert_eq!(
        scope["structuredContent"]["models"][0]["capabilities"]["publish"],
        false
    );
    assert_eq!(
        call(&server, READER, "get_model", json!({"model":"payments"})).await?["isError"],
        true
    );
    assert_eq!(
        call(&server, TOKEN, "get_model", json!({"model":"private"})).await?["isError"],
        true
    );
    let schema = call(&server, READER, "get_schema", json!({"model":"payments"})).await?;
    assert_eq!(
        schema["structuredContent"]["schema"]["nodes"]["Payment"]["key"],
        json!(["tenant_id", "payment_id"])
    );
    let result = call_at(&server, query_path, READER, "query", json!({"model":"payments","language":"cypher","query":"MATCH (p:Payment) WHERE p.payment_id >= $minimum RETURN p.payment_id AS id ORDER BY id","parameters":{"minimum":9}})).await?;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(result["structuredContent"]["rows"], json!([["9"]]));
    assert_eq!(result["structuredContent"]["truncated"], true);
    let strict = call_at(
        &server,
        query_path,
        READER,
        "query",
        json!({
            "model": "payments",
            "language": "cypher",
            "query": "MATCH (p:Payment) RETURN p.payment_id AS id",
            "freshness": {"max_observation_age_seconds": 60},
        }),
    )
    .await?;
    assert_eq!(strict["isError"], true, "{strict}");
    assert_eq!(strict["structuredContent"]["code"], "freshness_required");
    for query in [
        json!({"model":"private","language":"cypher","query":"MATCH (p) RETURN p"}),
        json!({"model":"payments","language":"cypher","query":"CREATE (p:Payment) RETURN p"}),
        json!({"model":"payments","language":"cypher","query":"RETURN 1","timeout_ms":60000}),
        json!({"model":"payments","language":"cypher","query":"RETURN 1","tenant_partition":{"column":"tenant_id","op":"eq","value":2}}),
    ] {
        assert_eq!(
            call(&server, READER, "query", query).await?["isError"],
            true
        );
    }

    for (language, statement) in [
        ("sql", "SELECT count(*) AS total FROM \"Payment\""),
        ("gremlin", "g.V().count()"),
    ] {
        let result = call(
            &server,
            READER,
            "query",
            json!({"model":"payments","language":language,"query":statement}),
        )
        .await?;
        assert_eq!(result["isError"], false, "{result}");
        assert_eq!(result["structuredContent"]["rows"], json!([["3"]]));
    }
    let groups: Value = server
        .request(Method::GET, "roles")
        .send()
        .await?
        .json()
        .await?;
    server.request(Method::PUT, "roles/reader").json(&json!({"version":groups["reader"]["version"],"members":["reader"],"grants":{"payments":["query"]},"partitions":{"payments":{"column":"tenant_id","op":"eq","value":2}}})).send().await?.error_for_status()?;
    let scoped = call(&server, READER, "query", json!({"model":"payments","language":"cypher","query":"MATCH (p:Payment) RETURN p.payment_id AS id"})).await?;
    assert_eq!(scoped["isError"], false, "{scoped}");
    assert_eq!(scoped["structuredContent"]["rows"], json!([]));

    let draft = yaml.replace("graph: Payments", "graph: AI Payments");
    let unauthorized = call(&server, TOKEN, "create_model", json!({"model":"not-granted","template_model":"payments","yaml":draft,"mutation_id":Uuid::new_v4()})).await?;
    assert_eq!(unauthorized["isError"], true);
    let escape = call(&server, TOKEN, "create_model", json!({"model":"ai-model","template_model":"payments","yaml":draft.replace("./payments.parquet","../private.parquet"),"mutation_id":Uuid::new_v4()})).await?;
    assert_eq!(escape["isError"], true);
    assert_eq!(escape["structuredContent"]["code"], "source_scope");
    let created = call_at(&server, model_path, TOKEN, "create_model", json!({"model":"ai-model","template_model":"payments","yaml":draft,"mutation_id":Uuid::new_v4()})).await?;
    assert_eq!(created["isError"], false, "{created}");
    assert!(created["structuredContent"]["active_revision"].is_null());
    let scope = call(&server, TOKEN, "list_models", json!({})).await?;
    assert_eq!(scope["structuredContent"]["modeling_targets"], json!([]));
    let new_model = scope["structuredContent"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["id"] == "ai-model")
        .unwrap();
    assert_eq!(
        new_model["capabilities"],
        json!({"query":false,"read_model":true,"modeling":true,"publish":false})
    );
    let validation = call(
        &server,
        TOKEN,
        "validate_model",
        json!({"model":"ai-model","check_data":true}),
    )
    .await?;
    assert_eq!(validation["isError"], false, "{validation}");
    assert_eq!(validation["structuredContent"]["status"], "passed");
    assert!(
        validation["structuredContent"]["labels"]["Payment"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "payment_id")
    );
    let invalid = call(
        &server,
        TOKEN,
        "validate_model",
        json!({"model":"ai-model","yaml":draft.replace("[channel]","[missing_column]")}),
    )
    .await?;
    assert_eq!(invalid["isError"], true);
    let revision = created["structuredContent"]["revision"].clone();
    let publish_args = json!({"model":"ai-model","revision":revision,"active_revision":null,"mutation_id":Uuid::new_v4()});
    assert_eq!(
        call(&server, TOKEN, "publish_model", publish_args.clone()).await?["isError"],
        true
    );
    configure(&server, json!({"publish":true})).await?;
    let published = call_at(&server, model_path, TOKEN, "publish_model", publish_args).await?;
    assert_eq!(published["isError"], false, "{published}");
    assert_eq!(published["structuredContent"]["active_revision"], revision);
    let scope = call(&server, TOKEN, "list_models", json!({})).await?;
    let new_model = scope["structuredContent"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["id"] == "ai-model")
        .unwrap();
    assert_eq!(
        new_model["capabilities"],
        json!({"query":true,"read_model":true,"modeling":true,"publish":true})
    );
    let edited = draft.replace("graph: AI Payments", "graph: Updated AI Payments");
    let saved = call(
        &server,
        TOKEN,
        "save_model",
        json!({"model":"ai-model","yaml":edited,"revision":revision,"mutation_id":Uuid::new_v4()}),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    assert_eq!(saved["structuredContent"]["active_revision"], revision);
    let conflict = call(
        &server,
        TOKEN,
        "save_model",
        json!({"model":"ai-model","yaml":draft,"revision":revision,"mutation_id":Uuid::new_v4()}),
    )
    .await?;
    assert_eq!(conflict["isError"], true);
    assert_eq!(conflict["structuredContent"]["code"], "revision_conflict");
    configure(&server, json!({"modeling":false,"publish":false})).await?;
    assert_eq!(
        rpc_at(&server, model_path, TOKEN, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        rpc_at(&server, query_path, READER, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    let saved_config = configure(
        &server,
        json!({"query":false,"modeling":true,"publish":true}),
    )
    .await?;
    assert_eq!(
        rpc_at(&server, query_path, TOKEN, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        rpc_at(&server, model_path, TOKEN, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    let scope = call(&server, TOKEN, "list_models", json!({})).await?;
    assert!(
        scope["structuredContent"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .all(|model| model["capabilities"]["query"] == false)
    );
    assert_eq!(
        call(&server, READER, "list_models", json!({})).await?["structuredContent"]["models"],
        json!([])
    );
    assert_eq!(
        call(
            &server,
            READER,
            "query",
            json!({"model":"payments","language":"cypher","query":"RETURN 1"})
        )
        .await?["isError"],
        true
    );
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let persisted: Value = server
        .request(Method::GET, "mcp/settings")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(persisted["config"], saved_config["config"]);
    assert_eq!(
        call(&server, TOKEN, "get_model", json!({"model":"ai-model"})).await?["structuredContent"]
            ["yaml"],
        edited
    );
    let user: Value = server
        .request(Method::GET, "users")
        .send()
        .await?
        .json()
        .await?;
    let agent = user
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["username"] == "agent")
        .unwrap();
    server
        .request(Method::PUT, "users/agent")
        .json(&json!({"service":true,"disabled":true,"version":agent["version"]}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        rpc(&server, TOKEN, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    configure(&server, json!({"enabled":false})).await?;
    assert_eq!(
        rpc(&server, READER, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let audit = fs::read_to_string(root.path().join(".graphlake-auth/audit.jsonl"))?;
    let events = audit
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(
        events
            .iter()
            .any(|v| v["protocol"] == "mcp" && v["action"] == "save_model")
    );
    assert!(!audit.contains(TOKEN));
    Ok(())
}

async fn login_member(server: &Server, name: &str) -> Result<String> {
    let response = server
        .guest(Method::POST, "auth/login")
        .json(&json!({"username":name,"password":PASSWORD}))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.headers()["Set-Cookie"]
        .to_str()?
        .split_once(';')
        .unwrap()
        .0
        .to_owned())
}

#[tokio::test]
async fn account_access_separates_console_sessions_and_mcp_credentials() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    payments_file(&root.path().join("payments.parquet"), false, &[8, 9, 10])?;
    fs::write(
        root.path().join("payments.yaml"),
        "graph: Payments\nsources:\n  src:\n    type: parquet\n    tables: { rows: [./payments.parquet] }\nnodes:\n  Payment: { source: src.rows, key: [tenant_id, payment_id], properties: [channel] }\n",
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    configure(
        &server,
        json!({"enabled":true,"query":true,"modeling":true}),
    )
    .await?;
    let update = |name: &str, changes: Value| {
        server
            .request(Method::PUT, &format!("users/{name}"))
            .json(&changes)
    };
    let member_login = |name: &str| {
        server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username":name,"password":PASSWORD}))
    };

    let legacy: Value = update("alice", json!({"password":PASSWORD,"disabled":false}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(legacy["console_access"], true);
    assert_eq!(legacy["mcp_access"], true);
    let headless: Value = update(
        "automation",
        json!({"disabled":false,"console_access":false,"issue_mcp_token":true}),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    let automation_token = headless["mcp_token"]
        .as_str()
        .context("missing one-time MCP token")?;
    update(
        "pipeline",
        json!({"service":true,"token":TOKEN,"disabled":false,"mcp_access":false}),
    )
    .send()
    .await?
    .error_for_status()?;
    server
        .request(Method::PUT, "roles/readers")
        .json(&json!({"members":["alice","automation","pipeline"],"grants":{"payments":["query"]}}))
        .send()
        .await?
        .error_for_status()?;

    let alice_cookie = login_member(&server, "alice").await?;
    let issue = |cookie: &str| {
        server
            .guest(Method::POST, "mcp/token")
            .header("Cookie", cookie)
            .json(&json!({"revision":null}))
    };
    let issued: Value = issue(&alice_cookie)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let alice_token = issued["token"].as_str().context("missing personal token")?;
    rpc_at(&server, "/mcp/query", alice_token, "ping", json!({}))
        .send()
        .await?
        .error_for_status()?;
    rpc_at(&server, "/mcp/query", automation_token, "ping", json!({}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        member_login("automation").send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .guest(Method::GET, "models")
            .bearer_auth(automation_token)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        rpc(&server, TOKEN, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let basic = server
        .http
        .post(format!("{}/mcp", server.base))
        .basic_auth("svc/pipeline", Some(TOKEN))
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"ping"}));
    assert_eq!(basic.send().await?.status(), StatusCode::UNAUTHORIZED);
    server
        .guest(Method::GET, "models")
        .basic_auth("svc/pipeline", Some(TOKEN))
        .send()
        .await?
        .error_for_status()?;

    let users: Vec<Value> = server
        .request(Method::GET, "users")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(users.iter().all(|user| user.get("mcp_token").is_none()));
    let alice = users
        .iter()
        .find(|user| user["username"] == "alice")
        .unwrap();
    let console_only: Value = update(
        "alice",
        json!({"version":alice["version"],"disabled":false,"mcp_access":false}),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    for path in ["/mcp", "/mcp/query", "/mcp/modeling"] {
        assert_eq!(
            rpc_at(&server, path, alice_token, "ping", json!({}))
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let cookie = login_member(&server, "alice").await?;
    assert_eq!(issue(&cookie).send().await?.status(), StatusCode::FORBIDDEN);
    let scope: Value = server
        .guest(Method::GET, "mcp/access")
        .header("Cookie", &cookie)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(scope["account_allowed"], false);
    assert_eq!(scope["scope"]["models"], json!([]));

    let mcp_only: Value = update("alice", json!({"version":console_only["version"],"disabled":false,"console_access":false,"mcp_access":true,"issue_mcp_token":true}))
        .send().await?.error_for_status()?.json().await?;
    assert_eq!(
        member_login("alice").send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .guest(Method::GET, "models")
            .header("Cookie", cookie)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let new_token = mcp_only["mcp_token"]
        .as_str()
        .context("missing replacement MCP token")?;
    drop(server);
    let server = Server::start(root.path(), "", 0)?;
    assert_eq!(
        rpc(&server, alice_token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    rpc_at(&server, "/mcp/query", new_token, "ping", json!({}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username":"alice","password":PASSWORD}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test]
async fn personal_ai_tokens_inherit_live_user_permissions_and_are_mcp_only() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    payments_file(&root.path().join("payments.parquet"), false, &[8, 9, 10])?;
    let yaml = "graph: Payments\nsources:\n  src:\n    type: parquet\n    tables: { rows: [./payments.parquet] }\nnodes:\n  Payment: { source: src.rows, key: [tenant_id, payment_id], properties: [channel] }\n";
    fs::write(root.path().join("payments.yaml"), yaml)?;
    fs::write(root.path().join("private.yaml"), yaml)?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    for (user, model) in [("alice", "payments"), ("bob", "private")] {
        server
            .request(Method::PUT, &format!("users/{user}"))
            .json(&json!({"password":PASSWORD,"disabled":false}))
            .send()
            .await?
            .error_for_status()?;
        server
            .request(Method::PUT, &format!("roles/{user}"))
            .json(&json!({"members":[user],"grants":{model:["query"]}}))
            .send()
            .await?
            .error_for_status()?;
    }
    configure(
        &server,
        json!({"enabled":true,"modeling":true,"publish":true}),
    )
    .await?;
    let alice = login_member(&server, "alice").await?;
    let bob = login_member(&server, "bob").await?;
    let issued: Value = server
        .guest(Method::POST, "mcp/token")
        .header("Cookie", &alice)
        .json(&json!({"revision":null}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let token = issued["token"].as_str().unwrap().to_owned();
    assert_eq!(issued["owner"], "alice");
    assert!(token.starts_with("glai_"));
    let status: Value = server
        .guest(Method::GET, "mcp/token")
        .header("Cookie", &alice)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(status.get("token").is_none());
    assert_eq!(status["active"], true);
    let bob_status: Value = server
        .guest(Method::GET, "mcp/token")
        .header("Cookie", &bob)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(bob_status["owner"], "bob");
    assert_eq!(bob_status["active"], false);
    assert_eq!(
        server
            .guest(Method::POST, "mcp/token")
            .header("Cookie", &bob)
            .json(&json!({"owner":"alice","revision":issued["revision"]}))
            .send()
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        server
            .guest(Method::DELETE, "mcp/token")
            .header("Cookie", &bob)
            .json(&json!({"revision":issued["revision"]}))
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    for path in ["users", "roles", "mcp/token", "mcp/settings"] {
        assert_eq!(
            server
                .guest(Method::GET, path)
                .bearer_auth(&token)
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        server
            .guest(Method::PUT, "mcp/settings")
            .header("Cookie", &alice)
            .json(&json!({"enabled":false}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let access: Value = server
        .guest(Method::GET, "mcp/access")
        .header("Cookie", &alice)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(access["owner"], "alice");
    assert_eq!(access["services"]["query"]["allowed"], true);
    assert_eq!(access["services"]["modeling"]["allowed"], false);
    assert_eq!(
        rpc_at(&server, "/mcp/modeling", &token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        rpc_at(&server, "/mcp/query", &token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(access["scope"]["models"].as_array().unwrap().len(), 1);
    let listed = call(&server, &token, "list_models", json!({})).await?;
    assert_eq!(listed["structuredContent"]["owner"], "alice");
    assert_eq!(listed["structuredContent"]["models"][0]["id"], "payments");
    assert_eq!(
        call(&server, &token, "get_model", json!({"model":"payments"})).await?["isError"],
        true
    );
    assert_eq!(
        call(
            &server,
            &token,
            "query",
            json!({"model":"private","language":"sql","query":"SELECT 1"})
        )
        .await?["isError"],
        true
    );
    assert_eq!(
        call(
            &server,
            &token,
            "query",
            json!({"model":"payments","language":"sql","query":"SELECT count(*) FROM \"Payment\""})
        )
        .await?["structuredContent"]["rows"],
        json!([["3"]])
    );
    let groups: Value = server
        .request(Method::GET, "roles")
        .send()
        .await?
        .json()
        .await?;
    let group: Value = server.request(Method::PUT,"roles/alice").json(&json!({"version":groups["alice"]["version"],"members":["alice"],"grants":{"payments":["query","modeler","publisher"]},"partitions":{"payments":{"column":"tenant_id","op":"eq","value":2}}})).send().await?.error_for_status()?.json().await?;
    assert_eq!(
        call(
            &server,
            &token,
            "query",
            json!({"model":"payments","language":"sql","query":"SELECT count(*) FROM \"Payment\""})
        )
        .await?["structuredContent"]["rows"],
        json!([["0"]])
    );
    assert_eq!(
        call(&server, &token, "get_model", json!({"model":"payments"})).await?["isError"],
        false
    );
    let group: Value = server.request(Method::PUT,"roles/alice").json(&json!({"version":group["version"],"members":["alice"],"grants":{"payments":["modeler"]}})).send().await?.error_for_status()?.json().await?;
    assert_eq!(
        call(
            &server,
            &token,
            "query",
            json!({"model":"payments","language":"sql","query":"SELECT 1"})
        )
        .await?["isError"],
        true
    );
    let draft = call(&server, &token, "get_model", json!({"model":"payments"})).await?;
    assert_eq!(call(&server,&token,"save_model",json!({"model":"payments","yaml":yaml.replace("graph: Payments","graph: AI draft"),"revision":draft["structuredContent"]["revision"],"mutation_id":Uuid::new_v4()})).await?["isError"], false);
    assert_eq!(call(&server,&token,"publish_model",json!({"model":"payments","revision":draft["structuredContent"]["revision"],"active_revision":null,"mutation_id":Uuid::new_v4()})).await?["isError"], true);
    let alice = login_member(&server, "alice").await?;
    let rotated: Value = server
        .guest(Method::POST, "mcp/token")
        .header("Cookie", &alice)
        .json(&json!({"revision":issued["revision"]}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let next_token = rotated["token"].as_str().unwrap().to_owned();
    assert_eq!(
        rpc(&server, &token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        rpc(&server, &next_token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        server
            .guest(Method::DELETE, "mcp/token")
            .header("Cookie", &alice)
            .json(&json!({"revision":issued["revision"]}))
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    // Revocation does not invalidate the owner's browser login.
    server
        .guest(Method::DELETE, "mcp/token")
        .header("Cookie", &alice)
        .json(&json!({"revision":rotated["revision"]}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        rpc(&server, &next_token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let replacement: Value = server
        .guest(Method::POST, "mcp/token")
        .header("Cookie", &alice)
        .json(&json!({"revision":null}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let replacement_token = replacement["token"].as_str().unwrap().to_owned();
    server.request(Method::PUT,"roles/alice").json(&json!({"version":group["version"],"members":["alice"],"grants":{"payments":["auditor"]}})).send().await?.error_for_status()?;
    assert_eq!(
        rpc(&server, &replacement_token, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let alice = login_member(&server, "alice").await?;
    assert_eq!(
        server
            .guest(Method::POST, "mcp/token")
            .header("Cookie", &alice)
            .json(&json!({"revision":replacement["revision"]}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    // A user who lost all AI capabilities can still revoke the old credential.
    server
        .guest(Method::DELETE, "mcp/token")
        .header("Cookie", &alice)
        .json(&json!({"revision":replacement["revision"]}))
        .send()
        .await?
        .error_for_status()?;
    let admin_token: Value = server
        .request(Method::POST, "mcp/token")
        .json(&json!({"revision":null}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let admin_secret = admin_token["token"].as_str().unwrap().to_owned();
    let admin_tools: Value = rpc(&server, &admin_secret, "tools/list", json!({}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let names = admin_tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(names.contains(&"run_task") && names.contains(&"validate_model_impact"));
    assert!(!names.contains(&"get_mutation"));
    assert_eq!(call(&server,&admin_secret,"list_models",json!({})).await?["structuredContent"]["models"].as_array().unwrap().len(),2);
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    assert_eq!(
        rpc(&server, &admin_secret, "ping", json!({}))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    let bob = login_member(&server, "bob").await?;
    let bob_token: Value = server
        .guest(Method::POST, "mcp/token")
        .header("Cookie", &bob)
        .json(&json!({"revision":null}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let users: Value = server
        .request(Method::GET, "users")
        .send()
        .await?
        .json()
        .await?;
    let bob_user = users
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["username"] == "bob")
        .unwrap();
    server
        .request(Method::PUT, "users/bob")
        .json(&json!({"version":bob_user["version"],"disabled":true}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        rpc(
            &server,
            bob_token["token"].as_str().unwrap(),
            "ping",
            json!({})
        )
        .send()
        .await?
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let directory = fs::read_to_string(root.path().join(".graphlake-auth/users.json"))?;
    let audit = fs::read_to_string(root.path().join(".graphlake-auth/audit.jsonl"))?;
    for secret in [&token, &next_token, &replacement_token, &admin_secret] {
        assert!(!directory.contains(secret));
        assert!(!audit.contains(secret));
    }
    assert!(
        audit
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|event| event["protocol"] == "mcp"
                && event["subject"] == "alice"
                && event["action"] == "save_model")
    );
    Ok(())
}

#[tokio::test]
async fn issuing_mcp_token_after_restore_cannot_reenable_old_console_password() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    server
        .request(Method::PUT, "users/restore-user")
        .json(&json!({"password":PASSWORD,"disabled":false}))
        .send()
        .await?
        .error_for_status()?;
    drop(server);

    let path = root.path().join(".graphlake-auth/users.json");
    let mut directory: Value = serde_json::from_slice(&fs::read(&path)?)?;
    let restored = &mut directory["accounts"]["restore-user"];
    restored["disabled"] = true.into();
    restored["reset_required"] = true.into();
    let version = restored["version"].clone();
    fs::write(path, serde_json::to_vec(&directory)?)?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let issued: Value = server.request(Method::PUT, "users/restore-user")
        .json(&json!({"version":version, "disabled":false, "console_access":false,"issue_mcp_token":true}))
        .send().await?.error_for_status()?.json().await?;
    assert!(issued["mcp_token"].is_string());
    let response = server
        .request(Method::PUT, "users/restore-user")
        .json(&json!({"version":issued["version"],"disabled":false,"console_access":true}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.json::<Value>().await?["code"], "password_required");
    let new_password = "fixture-only replacement console password";
    server.request(Method::PUT, "users/restore-user")
        .json(&json!({"version":issued["version"],"disabled":false,"console_access":true,"password":new_password}))
        .send().await?.error_for_status()?;
    for (password, expected) in [
        (PASSWORD, StatusCode::UNAUTHORIZED),
        (new_password, StatusCode::OK),
    ] {
        assert_eq!(
            server
                .guest(Method::POST, "auth/login")
                .json(&json!({"username":"restore-user","password":password}))
                .send()
                .await?
                .status(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn mcp_schema_describes_published_semantics_and_logical_types() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let data = root.path().join("grants.csv");
    fs::write(
        &data,
        "tenant,id,action,seconds,start,end,current,system\n1,10,read,42,10,999,true,iam\n",
    )?;
    let original = r#"graph: IAM grants
sources:
  iam:
    type: csv
    tables:
      grants:
        files: [grants.csv]
        columns:
          - {name: tenant, data_type: Int64, nullable: false}
          - {name: id, data_type: Int64, nullable: false}
          - {name: action, data_type: Utf8}
          - {name: seconds, data_type: 'Int64'}
          - {name: start, data_type: Int64}
          - {name: end, data_type: Int64}
          - {name: current, data_type: Boolean}
          - {name: system, data_type: Utf8}
nodes:
  Account: {source: iam.grants, key: [tenant, id]}
edges:
  GRANTED:
    inputs:
      - table: iam.grants
        columns: {age: seconds}
        casts: {age: Float64}
    key: [tenant, id]
    from: {node: Account, columns: [tenant, id]}
    to: {node: Account, columns: [tenant, id]}
    properties: [action, age, start, system]
    scd2: {valid_from: start, valid_to: end, current_flag: current, open_end: 999}
"#;
    let model = "grants";
    fs::write(root.path().join("grants.yaml"), original)?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    for (name, token, roles) in [
        ("modeler", TOKEN, json!(["modeler", "publisher"])),
        ("reader", READER, json!(["query"])),
    ] {
        server
            .request(Method::PUT, &format!("users/{name}"))
            .json(&json!({"service":true,"token":token,"disabled":false}))
            .send()
            .await?
            .error_for_status()?;
        let mut group = json!({"members":[name],"grants":{"grants":roles}});
        if token == READER {
            group["partitions"] = json!({"grants":{"column":"tenant","op":"eq","value":1}});
        }
        server
            .request(Method::PUT, &format!("roles/{name}"))
            .json(&group)
            .send()
            .await?
            .error_for_status()?;
    }
    configure(
        &server,
        json!({"enabled":true,"query":true,"modeling":true,"publish":true}),
    )
    .await?;

    let query_path = "/mcp/query";
    let mode = json!({"model":model,"include_types":true});
    let forbidden = call(&server, TOKEN, "get_schema", mode.clone()).await?;
    assert_eq!(forbidden["isError"], true, "{forbidden}");
    let old = call_at(&server, query_path, READER, "get_schema", mode.clone()).await?;
    assert_eq!(old["isError"], false, "{old}");
    let old = &old["structuredContent"];
    let previous = &old["model_revision"];
    assert_eq!(old["type_resolution"]["status"], "resolved", "{old}");
    assert_eq!(
        old["schema"]["nodes"]["Account"]["property_details"]["id"],
        json!({"data_type":"Int64","nullable":false})
    );
    assert_eq!(
        old["schema"]["edges"]["GRANTED"]["semantic_status"],
        "unspecified"
    );
    assert_eq!(
        old["schema"]["edges"]["GRANTED"]["property_details"]["age"]["data_type"],
        "Float64"
    );
    assert_eq!(
        old["schema"]["edges"]["GRANTED"]["temporal"],
        json!({
            "history":{"bounds":"[)","valid_from":"start","valid_to":null,"null_start_is_unbounded":true,"open_end":999},
            "current":{"mode":"current_flag","field":null,"value":true}
        })
    );

    let draft = format!(
        r#"{original}    semantics:
      description: IAM recorded a direct grant.
      relation:
        kind: entitlement
        statement: The account was granted the action.
        does_not_imply: [The action was used.]
      properties:
        age: {{unit: s, description: Age at collection}}
        start: {{unit: ms}}
        action: {{value_meanings: {{read: Read the resource}}}}
      temporal: {{description: The grant record is effective in this interval.}}
      evidence: {{source_system: system, record_key: [tenant, id], observed_at: start}}
semantics:
  description: Grants reported by the source.
  limitations: [Effective access is evaluated separately.]
"#
    );
    let model_path = "/mcp/modeling";
    for invalid in [
        draft.replace("record_key: [tenant, id]", "record_key: [tenant, end]"),
        draft.replace("age: {unit: s", "action: {unit: s").replace(
            "        action: {value_meanings: {read: Read the resource}}\n",
            "",
        ),
        draft.replace("observed_at: start", "observed_at: action"),
    ] {
        let checked = call_at(
            &server,
            model_path,
            TOKEN,
            "validate_model",
            json!({"model":model,"yaml":invalid}),
        )
        .await?;
        assert_eq!(checked["isError"], true, "{checked}");
        assert_eq!(
            checked["structuredContent"]["location"],
            json!({"kind":"field","label":"GRANTED","field":"semantics","index":null}),
            "{checked}"
        );
    }

    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        json!({
            "model":model,"yaml":draft,"revision":previous,"mutation_id":Uuid::new_v4()
        }),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let revision = &saved["structuredContent"]["revision"];
    let after_save = call_at(&server, query_path, READER, "get_schema", mode.clone()).await?;
    assert_eq!(after_save["structuredContent"]["model_revision"], *previous);
    assert_eq!(
        after_save["structuredContent"]["schema"]["semantic_status"],
        "unspecified"
    );
    let published = call_at(&server, model_path, TOKEN, "publish_model", json!({
        "model":model,"revision":revision,"active_revision":previous,"mutation_id":Uuid::new_v4()
    })).await?;
    assert_eq!(published["isError"], false, "{published}");
    let schema = call_at(&server, query_path, READER, "get_schema", mode.clone()).await?;
    let schema = &schema["structuredContent"];
    assert_eq!(schema["model_revision"], *revision);
    assert_eq!(
        schema["schema"]["semantics"]["description"],
        "Grants reported by the source."
    );
    let edge = &schema["schema"]["edges"]["GRANTED"];
    assert_eq!(edge["semantics"]["relation"]["kind"], "entitlement");
    assert_eq!(
        edge["semantics"]["evidence"]["record_key"],
        json!(["tenant", "id"])
    );
    assert_eq!(edge["property_details"]["age"]["unit"], "s");
    assert_eq!(edge["property_details"]["age"]["data_type"], "Float64");
    assert!(edge["property_details"].get("seconds").is_none());
    assert!(!schema.to_string().contains("grants.csv"));

    fs::rename(&data, root.path().join("grants.offline"))?;
    for (args, status) in [
        (mode.clone(), "unavailable"),
        (json!({"model":model}), "not_requested"),
    ] {
        let result = call_at(&server, query_path, READER, "get_schema", args).await?;
        assert_eq!(result["isError"], false, "{result}");
        let value = &result["structuredContent"];
        assert_eq!(value["model_revision"], *revision);
        assert_eq!(value["type_resolution"]["status"], status);
        let age = &value["schema"]["edges"]["GRANTED"]["property_details"]["age"];
        assert_eq!(
            age,
            &json!({"data_type":null,"nullable":null,"unit":"s","description":"Age at collection"})
        );
        assert!(!value.to_string().contains("grants.csv"));
    }
    fs::rename(root.path().join("grants.offline"), &data)?;

    let invalid = draft.replace("observed_at: start", "observed_at: action");
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        json!({"model":model,"yaml":invalid,"revision":revision,"mutation_id":Uuid::new_v4()}),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let rejected = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        json!({"model":model,"revision":saved["structuredContent"]["revision"],
            "active_revision":revision,"mutation_id":Uuid::new_v4()}),
    )
    .await?;
    assert_eq!(
        rejected["structuredContent"]["code"], "validation_failed",
        "{rejected}"
    );

    server
        .request(Method::POST, "models/grants/rollback")
        .json(&json!({"revision":previous,"active_revision":revision}))
        .send()
        .await?
        .error_for_status()?;
    let rolled_back = call_at(&server, query_path, READER, "get_schema", mode).await?;
    assert_eq!(
        rolled_back["structuredContent"]["model_revision"],
        *previous
    );
    assert_eq!(
        rolled_back["structuredContent"]["schema"]["semantic_status"],
        "unspecified"
    );
    Ok(())
}

#[tokio::test]
async fn business_tasks_return_evidence_and_gate_model_publication() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let model = include_str!("../../examples/security-tasks/model.yaml");
    let mut tables: Value =
        serde_json::from_str(include_str!("../../examples/security-tasks/fixture.json"))?;
    let device = &mut tables["tables"]["security.alert_device"];
    device["columns"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"score","data_type":"Utf8","nullable":false}));
    for row in device["rows"].as_array_mut().unwrap() {
        row["score"] = json!("-0.0");
    }
    let suite = serde_json::to_string(&tables)?;
    let mut sources = serde_json::Map::new();
    for (index, (reference, table)) in tables["tables"].as_object().unwrap().iter().enumerate() {
        let file = format!("fixture-{index}.jsonl");
        let mut contents = String::new();
        for row in table["rows"].as_array().unwrap() {
            use std::fmt::Write;
            writeln!(contents, "{row}")?;
        }
        fs::write(root.path().join(&file), contents)?;
        sources.insert(
            reference.strip_prefix("security.").unwrap().into(),
            json!({"files":[file],"columns":table["columns"]}),
        );
    }
    let model = model
        .replace(
            "{type: sqlite, path: security.sqlite}",
            &json!({"type":"jsonl","tables":sources}).to_string(),
        )
        .replace("    source: security.alert_device\n",
            "    inputs: [{table: security.alert_device, where: [{column: score, op: eq, value: -0.0}]}]\n");
    let suite_path = root.path().join(".tasks-security.json");
    fs::write(&suite_path, &suite)?;
    fs::write(root.path().join("security.yaml"), &model)?;
    let options = ["--timeout-ms", "10000"];
    let mut server = Server::start_with(root.path(), "", 0, &options)?;
    server.login().await?;
    for (name, token, roles) in [
        ("modeler", TOKEN, json!(["modeler", "publisher"])),
        ("reader", READER, json!(["query"])),
    ] {
        server
            .request(Method::PUT, &format!("users/{name}"))
            .json(&json!({"service":true,"token":token,"disabled":false}))
            .send()
            .await?
            .error_for_status()?;
        let mut group = json!({"members":[name],"grants":{"security":roles}});
        if token == READER {
            group["partitions"] = json!({"security":{"column":"tenant_id","op":"eq","value":"t1"}});
        }
        server
            .request(Method::PUT, &format!("roles/{name}"))
            .json(&group)
            .send()
            .await?
            .error_for_status()?;
    }
    configure(
        &server,
        json!({"enabled":true,"query":true,"modeling":true,"publish":true}),
    )
    .await?;
    let model_path = "/mcp/modeling";
    let query_path = "/mcp/query";
    let draft = call_at(
        &server,
        model_path,
        TOKEN,
        "get_model",
        json!({"model":"security"}),
    )
    .await?;
    assert!(
        draft["structuredContent"]["active_revision"].is_null(),
        "{draft}"
    );
    let before = call_at(
        &server,
        query_path,
        READER,
        "list_tasks",
        json!({"model":"security"}),
    )
    .await?;
    assert_eq!(
        before["structuredContent"]["code"], "unpublished",
        "{before}"
    );
    let v1 = &draft["structuredContent"]["revision"];
    let activation = |revision: &Value, active: &Value| {
        json!({
            "model":"security","revision":revision,"active_revision":active
        })
    };
    let mutation = |revision: &Value, active: &Value| {
        let mut value = activation(revision, active);
        value["mutation_id"] = json!(Uuid::new_v4());
        value
    };

    let wrong_unit = call_at(
        &server, model_path, TOKEN, "validate_model",
        json!({"model":"security","yaml":model.replace("observed_at_ms: {unit: ms}", "observed_at_ms: {unit: s}")}),
    ).await?;
    assert_eq!(
        wrong_unit["structuredContent"]["location"]["field"], "tasks",
        "{wrong_unit}"
    );
    let initial = call_at(
        &server,
        model_path,
        TOKEN,
        "validate_model_impact",
        activation(v1, &Value::Null),
    )
    .await?;
    assert_eq!(
        initial["structuredContent"]["status"], "passed",
        "{initial}"
    );
    let published = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(v1, &Value::Null),
    )
    .await?;
    assert_eq!(
        published["structuredContent"]["active_revision"], *v1,
        "{published}"
    );
    let listed = call_at(
        &server,
        query_path,
        READER,
        "list_tasks",
        json!({"model":"security"}),
    )
    .await?;
    assert_eq!(
        listed["structuredContent"]["tasks"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        listed["structuredContent"]["tasks"][0]["validation"]["report_id"],
        initial["structuredContent"]["report_id"]
    );

    let offboard = &tables["cases"][0];
    let alert = &tables["cases"][2];
    let run_args = |case: &Value| {
        json!({"model":"security","model_revision":v1,
        "task":case["task"],"parameters":case["parameters"]})
    };
    for case in [offboard, alert] {
        let outcome = call_at(&server, query_path, READER, "run_task", run_args(case)).await?;
        assert_eq!(
            outcome["structuredContent"]["execution_status"], "complete",
            "{outcome}"
        );
        assert_eq!(outcome["structuredContent"]["scope"]["tenant_id"], "t1");
        let mut actual = outcome["structuredContent"]["findings"]
            .as_array()
            .unwrap()
            .clone();
        let mut expected = case["expected"]["findings"].as_array().unwrap().clone();
        actual.sort_by_key(Value::to_string);
        expected.sort_by_key(Value::to_string);
        assert_eq!(actual, expected);
    }
    let mut outside_scope = run_args(offboard);
    outside_scope["parameters"]["tenant_id"] = json!("t2");
    let denied = call_at(&server, query_path, READER, "run_task", outside_scope).await?;
    assert_eq!(
        denied["structuredContent"]["code"], "invalid_task_parameters",
        "{denied}"
    );
    let denied = call(&server, READER, "validate_model_impact", activation(v1, v1)).await?;
    assert_eq!(denied["isError"], true);
    let mut stale = run_args(alert);
    stale["model_revision"] = json!("z".repeat(43));
    let denied = call_at(&server, query_path, READER, "run_task", stale).await?;
    assert_eq!(
        denied["structuredContent"]["code"], "active_revision_changed",
        "{denied}"
    );

    let regression = model.replace("    source: security.granted\n",
        "    inputs: [{table: security.granted, where: [{column: resource_id, op: ne, value: analytics}]}]\n");
    let save = |yaml: &str, revision: &Value| {
        json!({"model":"security","yaml":yaml,
        "revision":revision,"mutation_id":Uuid::new_v4()})
    };
    let positive_zero = model.replace("value: -0.0", "value: 0.0");
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        save(&positive_zero, v1),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let zero_revision = &saved["structuredContent"]["revision"];
    let report = call_at(
        &server,
        model_path,
        TOKEN,
        "validate_model_impact",
        activation(zero_revision, v1),
    )
    .await?;
    let case = &report["structuredContent"]["cases"][2];
    assert_eq!(case["status"], "failed", "{report}");
    assert_eq!(case["baseline_matches"], true, "{case}");
    assert_eq!(case["candidate_matches"], false, "{case}");
    assert_eq!(case["removed"][0]["evidence"][0]["record_id"], "ad1");
    assert_eq!(
        report["structuredContent"]["mapping_changes"]["ALERT_DEVICE"],
        json!(["inputs"])
    );
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        save(&regression, zero_revision),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let v2 = &saved["structuredContent"]["revision"];
    let report = call_at(
        &server,
        model_path,
        TOKEN,
        "validate_model_impact",
        activation(v2, v1),
    )
    .await?;
    assert_eq!(report["structuredContent"]["status"], "failed", "{report}");
    let case = &report["structuredContent"]["cases"][0];
    assert_eq!(case["baseline_matches"], true);
    assert_eq!(case["candidate_matches"], false);
    assert_eq!(case["removed"][0]["evidence"][1]["record_id"], "g1");
    assert_eq!(
        report["structuredContent"]["mapping_changes"]["GRANTED"],
        json!(["inputs", "source"])
    );
    let http_report: Value = server
        .request(Method::POST, "models/security/impact")
        .json(&json!({"revision":v2,"active_revision":v1}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        http_report["report_id"],
        report["structuredContent"]["report_id"]
    );
    let rejected = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(v2, v1),
    )
    .await?;
    assert_eq!(
        rejected["structuredContent"]["code"], "business_impact_failed",
        "{rejected}"
    );
    let response = server
        .request(Method::POST, "models/security/publish")
        .json(&json!({"revision":v2,"active_revision":v1}))
        .send()
        .await?;
    assert_eq!(
        response.json::<Value>().await?["code"],
        "business_impact_failed"
    );

    let without_version = model.replace(", source_version: source_version}", "}");
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        save(&without_version, v2),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let evidence_revision = &saved["structuredContent"]["revision"];
    let report = call_at(
        &server,
        model_path,
        TOKEN,
        "validate_model_impact",
        activation(evidence_revision, v1),
    )
    .await?;
    assert_eq!(report["structuredContent"]["status"], "failed", "{report}");
    let case = &report["structuredContent"]["cases"][0];
    assert_eq!(case["baseline_matches"], true);
    assert_eq!(case["candidate_matches"], false);
    let changed = &case["changed"][0];
    assert_eq!(changed[0]["evidence"][1]["source_version"], "fixture-1");
    assert!(changed[1]["evidence"][1].get("source_version").is_none());
    let rejected = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(evidence_revision, v1),
    )
    .await?;
    assert_eq!(
        rejected["structuredContent"]["code"], "business_impact_failed",
        "{rejected}"
    );

    let removal = model.replace(
        "tasks: [offboarded_account_exposure, alert_identity_device_context]\n",
        "",
    );
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        save(&removal, evidence_revision),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let v3 = &saved["structuredContent"]["revision"];
    let rejected = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(v3, v1),
    )
    .await?;
    assert_eq!(
        rejected["structuredContent"]["code"], "task_policy_mismatch",
        "{rejected}"
    );

    let mut versionless = tables.clone();
    for case in versionless["cases"].as_array_mut().unwrap() {
        let expected = &mut case["expected"];
        if let Some(anchor) = expected["anchor"].as_object_mut() {
            anchor.remove("source_version");
        }
        for finding in expected["findings"].as_array_mut().unwrap() {
            for evidence in finding["evidence"].as_array_mut().unwrap() {
                evidence.as_object_mut().unwrap().remove("source_version");
            }
        }
    }
    fs::write(&suite_path, serde_json::to_string(&versionless)?)?;
    let rejected = server
        .request(Method::POST, "models/security/rollback")
        .json(&json!({"revision":v1,"active_revision":v1}))
        .send()
        .await?;
    assert_eq!(
        rejected.json::<Value>().await?["code"],
        "business_impact_failed"
    );
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        save(&without_version, v3),
    )
    .await?;
    assert_eq!(saved["structuredContent"]["revision"], *evidence_revision);
    let published = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(evidence_revision, v1),
    )
    .await?;
    assert_eq!(
        published["structuredContent"]["active_revision"], *evidence_revision,
        "{published}"
    );
    fs::write(&suite_path, &suite)?;

    let documented = model.replace(
        "Synthetic identity, grant and alert evidence",
        "Evidence from synthetic grant and alert sources",
    );
    let saved = call_at(
        &server,
        model_path,
        TOKEN,
        "save_model",
        save(&documented, evidence_revision),
    )
    .await?;
    assert_eq!(saved["isError"], false, "{saved}");
    let v4 = &saved["structuredContent"]["revision"];
    let published = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(v4, evidence_revision),
    )
    .await?;
    assert_eq!(
        published["structuredContent"]["active_revision"], *v4,
        "{published}"
    );
    let rollback_preview: Value = server
        .request(Method::POST, "models/security/impact/rollback")
        .json(&json!({"revision":evidence_revision,"active_revision":v4}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(rollback_preview["status"], "failed");
    let affected = &rollback_preview["cases"][0];
    assert_eq!(affected["baseline_matches"], true);
    assert_eq!(affected["candidate_matches"], false);
    assert_eq!(
        affected["baseline_findings"],
        affected["candidate_findings"]
    );
    assert_eq!(
        affected["changed"][0][0]["evidence"][1]["source_version"],
        "fixture-1"
    );
    assert!(
        affected["changed"][0][1]["evidence"][1]
            .get("source_version")
            .is_none()
    );
    let rejected = server
        .request(Method::POST, "models/security/rollback")
        .json(&json!({"revision":evidence_revision,"active_revision":v4}))
        .send()
        .await?;
    assert_eq!(
        rejected.json::<Value>().await?["code"],
        "business_impact_failed"
    );
    server
        .request(Method::POST, "models/security/rollback")
        .json(&json!({"revision":v1,"active_revision":v4}))
        .send()
        .await?
        .error_for_status()?;
    let report = call_at(
        &server,
        model_path,
        TOKEN,
        "validate_model_impact",
        activation(v4, v1),
    )
    .await?;
    assert_eq!(report["structuredContent"]["status"], "passed", "{report}");
    for case in report["structuredContent"]["cases"].as_array().unwrap() {
        assert_eq!(case["baseline_matches"], true, "{case}");
        assert_eq!(case["candidate_matches"], true, "{case}");
        for field in ["added", "removed", "changed"] {
            assert_eq!(case[field], json!([]), "{case}");
        }
    }

    drop(server);
    let mut server = Server::start_with(root.path(), "", 0, &options)?;
    let restarted = call_at(&server, query_path, READER, "run_task", run_args(alert)).await?;
    assert_eq!(
        restarted["structuredContent"]["execution_status"], "complete",
        "{restarted}"
    );
    assert_eq!(
        restarted["structuredContent"]["coverage"]["accounts_with_multiple_identities"],
        json!(["okta:alice"])
    );
    let mut incomplete_suite = tables.clone();
    incomplete_suite["tables"]
        .as_object_mut()
        .unwrap()
        .remove("security.used_ip");
    fs::write(&suite_path, serde_json::to_string(&incomplete_suite)?)?;
    let uncovered = call_at(
        &server,
        model_path,
        TOKEN,
        "validate_model_impact",
        activation(v4, v1),
    )
    .await?;
    assert_eq!(
        uncovered["structuredContent"]["status"], "inconclusive",
        "{uncovered}"
    );
    assert_eq!(
        uncovered["structuredContent"]["cases"][2]["status"], "not_covered",
        "{uncovered}"
    );
    fs::write(&suite_path, format!("{suite}\n"))?;
    let stale = call_at(&server, query_path, READER, "run_task", run_args(alert)).await?;
    assert_eq!(
        stale["structuredContent"]["code"], "task_unvalidated",
        "{stale}"
    );
    fs::remove_file(&suite_path)?;
    let rejected = call_at(
        &server,
        model_path,
        TOKEN,
        "publish_model",
        mutation(v4, v1),
    )
    .await?;
    assert_eq!(
        rejected["structuredContent"]["code"], "missing_task_suite",
        "{rejected}"
    );

    server.login().await?;
    let suite_endpoint = "models/security/task-suite";
    let http_activation = json!({"revision":v4,"active_revision":v1});
    server
        .request(Method::PUT, "users/suite-modeler")
        .json(&json!({"password":PASSWORD,"disabled":false}))
        .send()
        .await?
        .error_for_status()?;
    server
        .request(Method::PUT, "roles/suite-modeler")
        .json(&json!({"members":["suite-modeler"],"grants":{"security":["modeler"]}}))
        .send()
        .await?
        .error_for_status()?;
    let login = server
        .guest(Method::POST, "auth/login")
        .json(&json!({"username":"suite-modeler","password":PASSWORD}))
        .send()
        .await?
        .error_for_status()?;
    let modeler_cookie = login.headers()["set-cookie"]
        .to_str()?
        .split(';')
        .next()
        .unwrap();
    assert_eq!(
        server
            .guest(Method::GET, suite_endpoint)
            .header("Cookie", modeler_cookie)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .guest(Method::PUT, suite_endpoint)
            .header("Cookie", modeler_cookie)
            .json(&json!({"revision":null,"content":suite}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .guest(Method::POST, "models/security/impact/rollback")
            .header("Cookie", modeler_cookie)
            .json(&http_activation)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );

    server
        .request(Method::PUT, suite_endpoint)
        .json(&json!({"revision":null,"content":suite}))
        .send()
        .await?
        .error_for_status()?;
    let mounted: Value = server
        .request(Method::GET, suite_endpoint)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(mounted["source"], "console");
    assert_eq!(mounted["content"], suite);
    assert_eq!(
        server
            .request(Method::PUT, suite_endpoint)
            .json(&json!({"revision":null,"content":suite}))
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    let preview: Value = server
        .request(Method::POST, "models/security/impact")
        .json(&http_activation)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(preview["status"], "passed");
    assert_eq!(preview["suite_revision"], mounted["revision"]);

    let edits = [format!("{suite}\n"), format!("{suite}\n\n")];
    let write = |content: &str| {
        server
            .request(Method::PUT, suite_endpoint)
            .json(&json!({"revision":mounted["revision"],"content":content}))
            .send()
    };
    let (left, right) = tokio::join!(write(&edits[0]), write(&edits[1]));
    let responses = [left?.status(), right?.status()];
    let winner = match responses {
        [
            StatusCode::OK,
            StatusCode::CONFLICT | StatusCode::TOO_MANY_REQUESTS,
        ] => 0,
        [
            StatusCode::CONFLICT | StatusCode::TOO_MANY_REQUESTS,
            StatusCode::OK,
        ] => 1,
        _ => panic!("expected exactly one suite save to succeed: {responses:?}"),
    };
    let latest: Value = server
        .request(Method::GET, suite_endpoint)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(latest["content"], edits[winner]);
    assert_eq!(
        write(&edits[1 - winner]).await?.status(),
        StatusCode::CONFLICT
    );
    let rejected = server.request(Method::POST, "models/security/publish")
        .json(&json!({"revision":v4,"active_revision":v1,"preview":{"report_id":preview["report_id"]}}))
        .send().await?;
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(
        rejected.json::<Value>().await?["code"],
        "impact_preview_changed"
    );
    let business: Value = server
        .request(Method::GET, "models/security/impact")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(business["active"], "stale");
    let current: Value = server
        .request(Method::POST, "models/security/impact")
        .json(&http_activation)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    server.request(Method::POST, "models/security/publish")
        .json(&json!({"revision":v4,"active_revision":v1,"preview":{"report_id":current["report_id"]}}))
        .send().await?.error_for_status()?;
    let business: Value = server
        .request(Method::GET, "models/security/impact")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(business["active"], "passed");

    let restored_draft: Value = server
        .request(Method::PUT, "models/security")
        .header("If-Match", format!("\"{}\"", v4.as_str().unwrap()))
        .json(&json!({"yaml":model}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(restored_draft["revision"], *v1);
    let rollback_target = json!({"revision":v1,"active_revision":v4});
    let preview_activation = |path: &'static str| {
        server
            .request(Method::POST, path)
            .json(&rollback_target)
            .send()
    };
    let draft_report: Value = preview_activation("models/security/impact")
        .await?
        .error_for_status()?
        .json()
        .await?;
    let rollback_report: Value = preview_activation("models/security/impact/rollback")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(draft_report["status"], "passed");
    assert_eq!(rollback_report["status"], "passed");
    assert_ne!(draft_report["report_id"], rollback_report["report_id"]);
    let rollback = |report: &Value| {
        server.request(Method::POST, "models/security/rollback")
        .json(&json!({"revision":v1,"active_revision":v4,"preview":{"report_id":report["report_id"]}}))
        .send()
    };
    let wrong_action = rollback(&draft_report).await?;
    assert_eq!(wrong_action.status(), StatusCode::CONFLICT);
    assert_eq!(
        wrong_action.json::<Value>().await?["code"],
        "impact_preview_changed"
    );
    server
        .request(Method::PUT, suite_endpoint)
        .json(&json!({"revision":latest["revision"],"content":suite}))
        .send()
        .await?
        .error_for_status()?;
    let stale = rollback(&rollback_report).await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(
        stale.json::<Value>().await?["code"],
        "impact_preview_changed"
    );
    let fresh: Value = preview_activation("models/security/impact/rollback")
        .await?
        .error_for_status()?
        .json()
        .await?;
    let rolled_back: Value = rollback(&fresh).await?.error_for_status()?.json().await?;
    assert_eq!(rolled_back["active_revision"], *v1);

    let strict_yaml =
        format!("{model}\nfreshness:\n  policy:\n    max_observation_age_seconds: 60\n");
    let strict_draft: Value = server
        .request(Method::PUT, "models/security")
        .header("If-Match", format!("\"{}\"", v1.as_str().unwrap()))
        .json(&json!({"yaml":strict_yaml}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    server
        .request(Method::POST, "models/security/publish")
        .json(&json!({"revision":strict_draft["revision"],"active_revision":v1}))
        .send()
        .await?
        .error_for_status()?;
    let mut strict_task = run_args(alert);
    strict_task["model_revision"] = strict_draft["revision"].clone();
    let strict = call_at(&server, query_path, READER, "run_task", strict_task).await?;
    assert_eq!(
        strict["structuredContent"]["code"], "freshness_required",
        "{strict}"
    );
    Ok(())
}

#[tokio::test]
async fn ordinary_model_preview_is_bound_to_the_reviewed_draft() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let yaml = include_str!("../../examples/text/graph.yaml");
    fs::write(root.path().join("graph.yaml"), yaml)?;
    fs::write(
        root.path().join("accounts.csv"),
        include_str!("../../examples/text/accounts.csv"),
    )?;
    fs::write(
        root.path().join("transfers.jsonl"),
        include_str!("../../examples/text/transfers.jsonl"),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let initial: Value = server
        .request(Method::GET, "models/graph")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let active = &initial["active_revision"];
    let save = |current: &Value, yaml: &str| {
        server
            .request(Method::PUT, "models/graph")
            .header("If-Match", format!("\"{}\"", current.as_str().unwrap()))
            .json(&json!({"yaml":yaml}))
            .send()
    };
    let first: Value = save(&initial["revision"], &format!("{yaml}\n# drafted once\n"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let preview = |target: &Value| {
        server
            .request(Method::POST, "models/graph/impact")
            .json(&json!({"revision":target,"active_revision":active}))
            .send()
    };
    let report: Value = preview(&first["revision"])
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(report["status"], "not_required");

    let second: Value = save(&first["revision"], &format!("{yaml}\n# drafted twice\n"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let publish = |target: &Value, id: &Value| {
        server
            .request(Method::POST, "models/graph/publish")
            .json(&json!({"revision":target,"active_revision":active,"preview":{"report_id":id},"mutation_id":Uuid::new_v4()}))
            .send()
    };
    assert_eq!(
        publish(&first["revision"], &report["report_id"])
            .await?
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    let mismatched = publish(&second["revision"], &report["report_id"]).await?;
    assert_eq!(mismatched.status(), StatusCode::CONFLICT);
    assert_eq!(
        mismatched.json::<Value>().await?["code"],
        "impact_preview_changed"
    );
    let fresh: Value = preview(&second["revision"])
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(fresh["report_id"].is_string());
    let published: Value = publish(&second["revision"], &fresh["report_id"])
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(published["active_revision"], second["revision"]);
    Ok(())
}
