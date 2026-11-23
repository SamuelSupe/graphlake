import { t } from './locale.ts';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Check, CheckCircle2, Copy, Eye, PlugZap } from 'lucide-react';
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
      <h3 id="mcp-connect-title">{t('添加到 Agent')}</h3>
    </div>
    <div className="mcp-step-body">
      {visibleServices.length > 1 && <fieldset className="mcp-service-fieldset">
        <legend className="sr-only">{t('选择 MCP 服务')}</legend>
        <div className="mcp-service-cards">{visibleServices.map(key => {
          const { enabled } = snapshot.services[key];
          return <label key={key} className={'mcp-service-card' + (service === key ? ' is-selected' : '')}>
            <input type="radio" name="mcp-service" checked={service === key} onChange={() => select(key)} />
            <span className="mcp-service-label"><strong>{mcpServiceLabels[key]}</strong>
              <code>/mcp/{key}</code>
            </span>
            <span className={'release-label ' + (enabled ? 'published' : '')}><i aria-hidden="true" />{enabled ? t('已启用') : t('未启用')}</span>
          </label>;
        })}</div>
      </fieldset>}
      {service && status ? <Connection key={service} service={service} endpoint={status.endpoint} revision={snapshot.revision}
        blocked={locked ? t('请先恢复登录会话。') : !status.enabled ? t('该服务尚未启用。') : ''} tokenUpdate={tokenUpdate} />
        : <p className="settings-hint">{t('当前账号没有可接入的 MCP 服务。请联系管理员分配查询、建模或发布权限。')}</p>}
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
  const [copiedKey, setCopiedKey] = useState<'' | 'endpoint' | 'snippet'>('');
  const [copyStatus, setCopyStatus] = useState('');
  const [copyError, setCopyError] = useState('');
  const copyTimer = useRef<number | undefined>(undefined);
  const active = useRef<AbortController | undefined>(undefined);
  const feedback = useRef<HTMLDivElement>(null);
  const name = 'graphlake-' + service;
  const snippet = JSON.stringify({
    mcpServers: { [name]: { url: endpoint, headers: { Authorization: 'Bearer <PERSONAL_AI_TOKEN>' } } },
  }, null, 2);

  useEffect(() => () => window.clearTimeout(copyTimer.current), []);

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

  async function copy(kind: 'endpoint' | 'snippet', value: string, message: string) {
    setCopyError('');
    setCopyStatus('');
    try {
      await navigator.clipboard.writeText(value);
      setCopiedKey(kind);
      setCopyStatus(message);
      window.clearTimeout(copyTimer.current);
      copyTimer.current = window.setTimeout(() => setCopiedKey(''), 2200);
    } catch {
      setCopyError(t('无法访问剪贴板，请选中文本手动复制。'));
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
          setError(controller.signal.reason === 'timeout' ? t('连接测试超时，请检查服务状态后重试。') : t('连接测试已取消。'));
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
      <p className="settings-hint">{t('在 Agent 的 MCP 设置中添加服务，填入以下信息。')}</p>
      <dl className="mcp-connection-fields">
        <div><dt>{t('名称')}</dt><dd><code>{name}</code></dd></div>
        <div><dt>{t('传输方式')}</dt><dd>Streamable HTTP</dd></div>
        <div><dt>{t('认证')}</dt><dd>{t('Bearer Token（个人令牌）')}</dd></div>
      </dl>
      <div className="mcp-field">
        <strong>{t('MCP 地址')}</strong>
        <div className="mcp-address">
          <code>{endpoint}</code>
          <button type="button" className={'button' + (copiedKey === 'endpoint' ? ' copied' : '')} onClick={() => void copy('endpoint', endpoint, t('MCP 地址已复制。'))}>
            {copiedKey === 'endpoint' ? <Check size={15} aria-hidden="true" /> : <Copy size={15} aria-hidden="true" />}
            {copiedKey === 'endpoint' ? t('已复制') : t('复制地址')}
          </button>
        </div>
      </div>
      <details className="mcp-details">
        <summary>{t('使用 JSON 配置')}</summary>
        <div className="settings-stack">
          <div className="code-snippet-box">
            <div className="code-snippet-bar">
              <span>JSON</span>
              <button type="button" className={'button compact' + (copiedKey === 'snippet' ? ' copied' : '')} onClick={() => void copy('snippet', snippet, t('JSON 配置已复制。'))}>
                {copiedKey === 'snippet' ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
                {copiedKey === 'snippet' ? t('已复制') : t('复制配置')}
              </button>
            </div>
            <pre className="mcp-snippet" tabIndex={0} aria-label={t('客户端 JSON 配置')}><code>{snippet}</code></pre>
          </div>
          <p className="settings-hint">{t('在 Agent 的配置中，将 ')}<code>&lt;PERSONAL_AI_TOKEN&gt;</code>{t(' 替换为你的令牌。')}</p>
        </div>
      </details>
      <span className="sr-only" role="status">{copyStatus}</span>
      {copyError && <p className="notice error" role="alert">{copyError}</p>}
      <details className="mcp-details mcp-diagnostics">
        <summary>{t('验证连接')}<span>{t('可选')}</span></summary>
        <form method="post" action={'/mcp/' + service} aria-labelledby="mcp-test-title" onSubmit={test} className="mcp-test" aria-busy={testing}>
          <h4 id="mcp-test-title" className="sr-only">{t('验证连接与模型权限')}</h4>
          {blocked ? <p className="settings-hint" role="status">{blocked}</p> : issuedToken ?
            <p className="settings-hint">{t('使用本页生成的令牌验证身份与模型权限。')}</p> :
            <div className="mcp-field">
              <label htmlFor="mcp-token">{t('个人令牌（必填）')}</label>
              <div className="mcp-input-action">
                <input id="mcp-token" name="token" type={showToken ? 'text' : 'password'}
                  autoComplete="current-password" autoCapitalize="none" autoCorrect="off" spellCheck={false}
                  minLength={32} maxLength={256} required value={token} disabled={testing}
                  aria-describedby="mcp-token-help" onChange={event => {
                    setToken(event.target.value);
                    setResult(undefined);
                    setError('');
                  }} />
                <button type="button" className="button" aria-label={t('显示测试令牌')} aria-pressed={showToken}
                  disabled={testing} onClick={() => setShowToken(value => !value)}>
                  <Eye size={16} aria-hidden="true" />{showToken ? t('隐藏') : t('显示')}
                </button>
              </div>
              <p id="mcp-token-help" className="settings-hint">{t('粘贴已保存的个人令牌。测试结束后会清空。')}</p>
            </div>}
          <div className="settings-actions">
            <button type="submit" className="button" disabled={!!blocked || testing}>
              <PlugZap size={16} aria-hidden="true" />{testing ? t('正在测试…') : t('测试 MCP 连接')}
            </button>
            {testing && <button type="button" className="button" onClick={() => active.current?.abort('cancel')}>{t('取消测试')}</button>}
          </div>
          {progress && <p role="status">{progress}</p>}
          {(error || result) && <div ref={feedback} tabIndex={-1} className="mcp-test-feedback">
            {error && <p className="notice error" role="alert">{error}</p>}
            {result && <>
              <p className="mcp-success" role="status">
                <CheckCircle2 size={18} aria-hidden="true" />
                <span>{t('以 {0} 身份连接成功 · {1} 个工具 · {2} 个可见模型', result.owner, result.tools.length, result.models.length)}</span>
              </p>
              <details className="mcp-details">
                <summary>{t('查看模型与工具')}</summary>
                <div className="settings-stack">
                  <p className="settings-hint">MCP {result.protocolVersion}{t('。本次仅检查工具与权限。')}</p>
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
