import { useEffect, useRef, useState, type FormEvent } from 'react';
import { CheckCircle2, Copy, Eye, PlugZap } from 'lucide-react';
import { errorText } from './api';
import McpScope from './McpScope';
import { inspectMcp, type McpInspection, type McpSnapshot } from './mcp';

export default function McpConnection({ snapshot, blocked, issuedToken }: {
  snapshot: McpSnapshot; blocked: string; issuedToken: string;
}) {
  const [token, setToken] = useState('');
  const [showToken, setShowToken] = useState(false);
  const [testing, setTesting] = useState(false);
  const [progress, setProgress] = useState('');
  const [result, setResult] = useState<McpInspection>();
  const [error, setError] = useState('');
  const [copied, setCopied] = useState('');
  const [copyError, setCopyError] = useState('');
  const active = useRef<AbortController | undefined>(undefined);
  const feedback = useRef<HTMLDivElement>(null);
  const snippet = JSON.stringify({ mcpServers: { graphlake: { url: snapshot.endpoint, headers: { Authorization: 'Bearer <PERSONAL_AI_TOKEN>' } } } }, null, 2);

  useEffect(() => { setToken(issuedToken); setResult(undefined); setError(''); }, [issuedToken]);

  useEffect(() => {
    if (blocked) {
      active.current?.abort(); active.current = undefined;
      setTesting(false); setToken(''); setShowToken(false); setResult(undefined); setError(''); setProgress('');
    }
  }, [blocked]);
  useEffect(() => () => { active.current?.abort(); active.current = undefined; }, []);

  async function copy(value: string, message: string) {
    setCopyError(''); setCopied('');
    try { await navigator.clipboard.writeText(value); setCopied(message); }
    catch { setCopyError('无法访问剪贴板，请选中文本手动复制。'); }
  }
  async function test(event: FormEvent) {
    event.preventDefault();
    if (blocked || testing) return;
    const form = event.currentTarget as HTMLFormElement;
    const controller = new AbortController(); active.current = controller;
    const deadline = window.setTimeout(() => controller.abort('timeout'), 10_000);
    setTesting(true); setResult(undefined); setError(''); setShowToken(false);
    try {
      const inspection = await inspectMcp(token.trim(), controller.signal, setProgress);
      if (active.current === controller && !controller.signal.aborted) setResult(inspection);
    } catch (cause) {
      if (active.current === controller) setError(controller.signal.aborted
        ? controller.signal.reason === 'timeout' ? '连接测试超时，请检查服务状态后重试。' : '连接测试已取消。'
        : errorText(cause));
    } finally {
      window.clearTimeout(deadline);
      if (active.current === controller) {
        form.reset();
        active.current = undefined; setTesting(false); setToken(''); setProgress('');
        requestAnimationFrame(() => feedback.current?.focus());
      }
      controller.abort();
    }
  }

  return <section className="mcp-step" aria-labelledby="mcp-connect-title">
    <div className="mcp-step-title"><span className="mcp-step-number" aria-hidden="true">2</span><h3 id="mcp-connect-title">添加到 Agent</h3></div>
    <div className="mcp-step-body">
    <p className="settings-hint">在 Agent 的 MCP 设置中添加 HTTP 服务，名称填写 <code>graphlake</code>。</p>
    <div className="mcp-field">
      <label htmlFor="mcp-endpoint">MCP 地址</label>
      <div className="mcp-input-action"><input id="mcp-endpoint" name="mcp_endpoint" readOnly value={snapshot.endpoint} spellCheck={false} /><button type="button" className="button" onClick={() => void copy(snapshot.endpoint, 'MCP 地址已复制。')}><Copy size={15} aria-hidden="true" />复制地址</button></div>
    </div>
    <p className="settings-hint">认证选择 <strong>Bearer Token</strong>，填入上一步的令牌。</p>
    <details className="mcp-details">
      <summary>使用 JSON 配置</summary>
      <div className="settings-stack">
        <pre className="mcp-snippet" tabIndex={0} aria-label="客户端 JSON 配置"><code>{snippet}</code></pre>
        <button type="button" className="button" onClick={() => void copy(snippet, '客户端配置已复制，请替换个人 AI 令牌。')}><Copy size={15} aria-hidden="true" />复制配置</button>
        <p className="settings-hint">将 <code>&lt;PERSONAL_AI_TOKEN&gt;</code> 替换为你的令牌，仅放入认证配置，不要发送到对话。</p>
        <p className="settings-hint">传输方式：Streamable HTTP。远程部署使用 HTTPS。协议版本：{snapshot.protocol_versions.join('、')}。</p>
      </div>
    </details>
    {copied && <p className="settings-hint" role="status">{copied}</p>}
    {copyError && <p className="notice error" role="alert">{copyError}</p>}
    {blocked && <p className="notice" role="status">{blocked}</p>}
    <details className="mcp-details mcp-diagnostics"><summary>验证连接<span>可选</span></summary>
    <form method="post" action="/mcp" aria-labelledby="mcp-test-title" onSubmit={test} className="mcp-test" aria-busy={testing}>
      <h4 id="mcp-test-title" className="sr-only">验证连接与模型权限</h4>
      <div className="mcp-field">
        <label htmlFor="mcp-token">个人 AI 令牌（必填）</label>
        <div className="mcp-input-action"><input id="mcp-token" name="token" type={showToken ? 'text' : 'password'} autoComplete="current-password" autoCapitalize="none" autoCorrect="off" spellCheck={false} minLength={32} maxLength={256} required value={token} disabled={!!blocked || testing} aria-describedby="mcp-token-help" onChange={event => { setToken(event.target.value); setResult(undefined); setError(''); }} /><button type="button" className="button" aria-label={showToken ? '隐藏测试令牌' : '显示测试令牌'} aria-pressed={showToken} disabled={!!blocked || testing} onClick={() => setShowToken(value => !value)}><Eye size={16} aria-hidden="true" />{showToken ? '隐藏' : '显示'}</button></div>
        <p id="mcp-token-help" className="settings-hint">新令牌会自动填入。测试后清空，不保存到浏览器。</p>
      </div>
      <div className="settings-actions"><button type="submit" className="button" disabled={!!blocked || testing}><PlugZap size={16} aria-hidden="true" />{testing ? '正在测试…' : '测试 MCP 连接'}</button>
        {testing && <button type="button" className="button" onClick={() => active.current?.abort('cancel')}>取消测试</button>}</div>
      {progress && <p role="status">{progress}</p>}
      {(error || result) && <div ref={feedback} tabIndex={-1} className="mcp-test-feedback">
        {error && <p className="notice error" role="alert">{error}</p>}
        {result && <>
          <p className="mcp-success" role="status"><CheckCircle2 size={18} aria-hidden="true" />以 {result.owner} 身份连接成功 · {result.tools.length} 个工具 · {result.models.length} 个可见模型</p>
          <p className="settings-hint">已读取工具与权限，未执行查询或写入。</p>
          <details className="mcp-details"><summary>查看模型与工具</summary><div className="settings-stack"><McpScope models={result.models} targets={result.modelingTargets} /><ul className="mcp-tool-list">{result.tools.map(tool => <li key={tool.name}><code>{tool.name}</code><p className="settings-hint">{tool.description}</p></li>)}</ul></div></details>
        </>}
      </div>}
    </form>
    </details>
    </div>
  </section>;
}
