import { useEffect, useRef, useState, type FormEvent } from 'react';
import { CheckCircle2, Copy, Eye, PlugZap } from 'lucide-react';
import { errorText } from './api';
import McpScope from './McpScope';
import { inspectMcp, mcpServiceLabels, mcpServices, type McpAccess, type McpInspection, type McpService } from './mcp';

export default function McpConnection({ snapshot, locked, tokenUpdate }: {
  snapshot: McpAccess; locked: boolean; tokenUpdate?: { value: string };
}) {
  const [selected, select] = useState<McpService>();
  const visibleServices = mcpServices.filter(key => snapshot.services[key].allowed);
  const service = selected && visibleServices.includes(selected) ? selected
    : visibleServices.find(key => snapshot.services[key].enabled) ?? visibleServices.at(0);
  const status = service && snapshot.services[service];
  return <section className="mcp-step" aria-labelledby="mcp-connect-title">
    <div className="mcp-step-title">
      <span className="mcp-step-number" aria-hidden="true">2</span>
      <h3 id="mcp-connect-title">添加到 Agent</h3>
    </div>
    <div className="mcp-step-body">
      {visibleServices.length > 1 && <fieldset className="settings-fields">
        <legend>选择 MCP 服务</legend>
        <div className="settings-actions">{visibleServices.map(key => {
          const { enabled } = snapshot.services[key];
          return <label key={key} className="settings-toggle">
            <input type="radio" name="mcp-service" checked={service === key} onChange={() => select(key)} />
            {mcpServiceLabels[key]}{!enabled && '（未启用）'}
          </label>;
        })}</div>
      </fieldset>}
      {service && status ? <Connection key={service} service={service} endpoint={status.endpoint} revision={snapshot.revision}
        blocked={locked ? '请先恢复登录会话。' : !status.enabled ? '该服务尚未启用。' : ''} tokenUpdate={tokenUpdate} />
        : <p className="settings-hint">当前账号没有可接入的 MCP 服务。请联系管理员分配查询、建模或发布权限。</p>}
    </div>
  </section>;
}

function Connection({ service, endpoint, revision, blocked, tokenUpdate }: {
  service: McpService; endpoint: string; revision: string; blocked: string; tokenUpdate?: { value: string };
}) {
  const issuedToken = tokenUpdate?.value;
  const [token, setToken] = useState('');
  const [showToken, setShowToken] = useState(false);
  const [testing, setTesting] = useState(false);
  const [progress, setProgress] = useState('');
  const [result, setResult] = useState<McpInspection>();
  const [error, setError] = useState('');
  const [copyStatus, setCopyStatus] = useState('');
  const [copyError, setCopyError] = useState('');
  const active = useRef<AbortController | undefined>(undefined);
  const feedback = useRef<HTMLDivElement>(null);
  const name = 'graphlake-' + service;
  const snippet = JSON.stringify({
    mcpServers: { [name]: { url: endpoint, headers: { Authorization: 'Bearer <PERSONAL_AI_TOKEN>' } } },
  }, null, 2);

  useEffect(() => {
    setToken('');
    setShowToken(false);
    setTesting(false);
    setProgress('');
    setError('');
    setResult(undefined);
    return () => {
      active.current?.abort();
      active.current = undefined;
    };
  }, [blocked, tokenUpdate, revision]);

  async function copy(value: string, message: string) {
    setCopyError('');
    setCopyStatus('');
    try {
      await navigator.clipboard.writeText(value);
      setCopyStatus(message);
    } catch {
      setCopyError('无法访问剪贴板，请选中文本手动复制。');
    }
  }

  async function test(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (blocked || active.current) return;
    const form = event.currentTarget;
    const controller = new AbortController();
    active.current = controller;
    const deadline = window.setTimeout(() => controller.abort('timeout'), 10_000);
    setTesting(true);
    setResult(undefined);
    setError('');
    setShowToken(false);
    try {
      const inspection = await inspectMcp(service, issuedToken || token.trim(), controller.signal, setProgress);
      if (active.current === controller && !controller.signal.aborted) setResult(inspection);
    } catch (cause) {
      if (active.current === controller) {
        if (controller.signal.aborted) {
          setError(controller.signal.reason === 'timeout' ? '连接测试超时，请检查服务状态后重试。' : '连接测试已取消。');
        } else {
          setError(errorText(cause));
        }
      }
    } finally {
      window.clearTimeout(deadline);
      if (active.current === controller) {
        form.reset();
        active.current = undefined;
        setTesting(false);
        setToken('');
        setProgress('');
        requestAnimationFrame(() => feedback.current?.focus());
      }
      controller.abort();
    }
  }

  return <>
      <p className="settings-hint">在 Agent 的 MCP 设置中添加服务，填入以下信息。</p>
      <dl className="mcp-connection-fields">
        <div><dt>名称</dt><dd><code>{name}</code></dd></div>
        <div><dt>传输方式</dt><dd>Streamable HTTP</dd></div>
        <div><dt>认证</dt><dd>Bearer Token（个人令牌）</dd></div>
      </dl>
      <div className="mcp-field">
        <strong>MCP 地址</strong>
        <div className="mcp-address">
          <code>{endpoint}</code>
          <button type="button" className="button" onClick={() => void copy(endpoint, 'MCP 地址已复制。')}>
            <Copy size={15} aria-hidden="true" />复制地址
          </button>
        </div>
      </div>
      <details className="mcp-details">
        <summary>使用 JSON 配置</summary>
        <div className="settings-stack">
          <pre className="mcp-snippet" tabIndex={0} aria-label="客户端 JSON 配置"><code>{snippet}</code></pre>
          <p className="settings-hint">在 Agent 的配置中，将 <code>&lt;PERSONAL_AI_TOKEN&gt;</code> 替换为你的令牌。</p>
          <div className="settings-actions">
            <button type="button" className="button" onClick={() => void copy(snippet, 'JSON 配置已复制。')}>
              <Copy size={15} aria-hidden="true" />复制配置
            </button>
          </div>
        </div>
      </details>
      {copyStatus && <p className="settings-hint" role="status">{copyStatus}</p>}
      {copyError && <p className="notice error" role="alert">{copyError}</p>}
      <details className="mcp-details mcp-diagnostics">
        <summary>验证连接<span>可选</span></summary>
        <form method="post" action={'/mcp/' + service} aria-labelledby="mcp-test-title" onSubmit={test} className="mcp-test" aria-busy={testing}>
          <h4 id="mcp-test-title" className="sr-only">验证连接与模型权限</h4>
          {blocked ? <p className="settings-hint" role="status">{blocked}</p> : issuedToken ?
            <p className="settings-hint">使用本页生成的令牌验证身份与模型权限。</p> :
            <div className="mcp-field">
              <label htmlFor="mcp-token">个人令牌（必填）</label>
              <div className="mcp-input-action">
                <input id="mcp-token" name="token" type={showToken ? 'text' : 'password'}
                  autoComplete="current-password" autoCapitalize="none" autoCorrect="off" spellCheck={false}
                  minLength={32} maxLength={256} required value={token} disabled={testing}
                  aria-describedby="mcp-token-help" onChange={event => {
                    setToken(event.target.value);
                    setResult(undefined);
                    setError('');
                  }} />
                <button type="button" className="button" aria-label="显示测试令牌" aria-pressed={showToken}
                  disabled={testing} onClick={() => setShowToken(value => !value)}>
                  <Eye size={16} aria-hidden="true" />{showToken ? '隐藏' : '显示'}
                </button>
              </div>
              <p id="mcp-token-help" className="settings-hint">粘贴已保存的个人令牌。测试结束后会清空。</p>
            </div>}
          <div className="settings-actions">
            <button type="submit" className="button" disabled={!!blocked || testing}>
              <PlugZap size={16} aria-hidden="true" />{testing ? '正在测试…' : '测试 MCP 连接'}
            </button>
            {testing && <button type="button" className="button" onClick={() => active.current?.abort('cancel')}>取消测试</button>}
          </div>
          {progress && <p role="status">{progress}</p>}
          {(error || result) && <div ref={feedback} tabIndex={-1} className="mcp-test-feedback">
            {error && <p className="notice error" role="alert">{error}</p>}
            {result && <>
              <p className="mcp-success" role="status">
                <CheckCircle2 size={18} aria-hidden="true" />
                <span>以 {result.owner} 身份连接成功 · {result.tools.length} 个工具 · {result.models.length} 个可见模型</span>
              </p>
              <details className="mcp-details">
                <summary>查看模型与工具</summary>
                <div className="settings-stack">
                  <p className="settings-hint">MCP {result.protocolVersion}。本次仅检查工具与权限。</p>
                  <McpScope models={result.models} targets={result.modelingTargets} />
                  <ul className="mcp-tool-list">{result.tools.map(tool => <li key={tool.name}>
                    <code>{tool.name}</code><p className="settings-hint">{tool.description}</p>
                  </li>)}</ul>
                </div>
              </details>
            </>}
          </div>}
        </form>
      </details>
  </>;
}
