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
            vec!["list_models", "get_schema", "query"],
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
    assert_eq!(names, ["list_models", "get_schema", "query"]);
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
    assert_eq!(admin_tools["result"]["tools"].as_array().unwrap().len(), 8);
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
