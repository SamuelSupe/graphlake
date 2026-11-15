import { useEffect, useRef, useState, type FormEvent } from 'react';
import { api, ApiError, errorText } from './api';
import type { McpConfig, McpSnapshot } from './mcp';

export default function McpServiceSettings({ locked, onDirty, changed }: {
  locked: boolean; onDirty: (key: string, value: boolean) => void; changed: () => void;
}) {
  const [snapshot, setSnapshot] = useState<McpSnapshot>();
  const [config, setConfig] = useState<McpConfig>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [conflict, setConflict] = useState(false);
  const [message, setMessage] = useState('');
  const [attempt, reload] = useState(0);
  const errorRef = useRef<HTMLDivElement>(null);
  const dirty = !!(snapshot && config && (Object.keys(config) as (keyof McpConfig)[]).some(key => config[key] !== snapshot.config[key]));

  useEffect(() => {
    if (locked || snapshot) return;
    const controller = new AbortController();
    api<McpSnapshot>('/mcp/settings', { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) { setSnapshot(value); setConfig(value.config); setError(''); }
    }, cause => { if (!controller.signal.aborted) setError(errorText(cause)); });
    return () => controller.abort();
  }, [locked, snapshot, attempt]);
  useEffect(() => { onDirty('/ai', dirty); }, [dirty, onDirty]);
  useEffect(() => () => onDirty('/ai', false), [onDirty]);

  function change<K extends keyof McpConfig>(key: K, value: McpConfig[K]) {
    setConfig(current => current && { ...current, [key]: value, ...(key === 'modeling' && !value ? { publish: false } : {}) });
    setMessage('');
    if (!conflict) setError('');
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    if (!snapshot || !config || busy || locked || !dirty || conflict) return;
    setBusy(true); setError(''); setMessage('');
    try {
      const next = await api<McpSnapshot>('/mcp/settings', {
        method: 'PUT', headers: { 'If-Match': '"' + snapshot.revision + '"' }, body: JSON.stringify(config),
      });
      setSnapshot(next); setConfig(next.config); setMessage('MCP 配置已保存并立即生效。'); changed();
    } catch (cause) {
      const changed = cause instanceof ApiError && cause.code === 'mcp_conflict';
      setConflict(changed);
      setError(changed ? '配置已被其他页面更新。你的修改仍保留，请先重新加载最新配置再编辑。' : errorText(cause));
      requestAnimationFrame(() => errorRef.current?.focus());
    } finally { setBusy(false); }
  }
  function refresh() {
    setSnapshot(undefined); setConfig(undefined); setConflict(false); setMessage(''); setError(''); reload(value => value + 1);
  }
  return <>
      {!snapshot || !config ? <section className="settings-card">
        {error ? <><p className="notice error" role="alert">{error}</p><button type="button" className="button" disabled={locked} onClick={refresh}>重试读取 MCP 配置</button></>
          : <p role="status">正在读取 MCP 配置…</p>}
      </section> : <>
        <form className="settings-card mcp-form" method="post" action="/api/v1/mcp/settings" aria-labelledby="mcp-title" onSubmit={save} aria-busy={busy}>
          <div><h2 id="mcp-title">服务与能力</h2><p className="settings-hint">决定外部 AI 可以做什么，具体能力继承用户当前的模型权限。</p></div>
          <fieldset className="mcp-capabilities" disabled={locked || busy}>
            <legend className="sr-only">工具能力</legend>
            <div className="mcp-capability">
              <input id="mcp-enabled" type="checkbox" name="enabled" checked={config.enabled} aria-describedby="mcp-enabled-help" onChange={event => change('enabled', event.target.checked)} />
              <div><label htmlFor="mcp-enabled">启用 MCP 服务</label><p id="mcp-enabled-help" className="settings-hint">保存后生效。关闭后，外部 AI 将无法调用 MCP 工具。</p></div>
            </div>
            <div className="mcp-capability">
              <input id="mcp-query" type="checkbox" name="query" checked={config.query} aria-describedby="mcp-query-help" onChange={event => change('query', event.target.checked)} />
              <div><label htmlFor="mcp-query">允许查询数据</label><p id="mcp-query-help" className="settings-hint">只读查询已发布模型，支持 Cypher、SQL 和 Gremlin。需要查询角色。</p></div>
            </div>
            <div className="mcp-capability">
              <input id="mcp-modeling" type="checkbox" name="modeling" checked={config.modeling} aria-describedby="mcp-modeling-help" onChange={event => change('modeling', event.target.checked)} />
              <div><label htmlFor="mcp-modeling">允许构建和编辑模型</label><p id="mcp-modeling-help" className="settings-hint">读取、校验和保存草稿，需要建模角色。新建模型须预授权目标标识，并复用模板已批准的数据源和物理表。</p></div>
            </div>
            <div className="mcp-capability">
              <input id="mcp-publish" type="checkbox" name="publish" checked={config.publish} disabled={!config.modeling} aria-describedby="mcp-publish-help" onChange={event => change('publish', event.target.checked)} />
              <div><label htmlFor="mcp-publish">允许发布模型</label><p id="mcp-publish-help" className="settings-hint">需先启用建模，并授予独立的发布角色。发布会切换后续查询使用的版本；关闭时可在图模型页面人工发布。</p></div>
            </div>
          </fieldset>
          <fieldset className="settings-fields" disabled={locked || busy}>
            <legend>查询限制</legend>
            <div className="settings-grid">
              <div className="mcp-field"><label htmlFor="mcp-max-rows">最多返回行数（必填）</label><input id="mcp-max-rows" type="number" name="max_rows" min="1" max="500" step="1" required aria-describedby="mcp-rows-help" value={Number.isNaN(config.max_rows) ? '' : config.max_rows} onChange={event => change('max_rows', event.target.valueAsNumber)} /><small id="mcp-rows-help" className="settings-hint">1–500 行，超出时标记截断</small></div>
              <div className="mcp-field"><label htmlFor="mcp-timeout">查询超时（毫秒，必填）</label><input id="mcp-timeout" type="number" name="timeout_ms" min="1" max="60000" step="1" required aria-describedby="mcp-timeout-help" value={Number.isNaN(config.timeout_ms) ? '' : config.timeout_ms} onChange={event => change('timeout_ms', event.target.valueAsNumber)} /><small id="mcp-timeout-help" className="settings-hint">1–60000 毫秒；服务上限 {snapshot.server_timeout_ms} 毫秒</small></div>
            </div>
            {config.timeout_ms > snapshot.server_timeout_ms && <p className="settings-hint">受服务上限限制，实际超时为 {snapshot.server_timeout_ms} 毫秒。</p>}
          </fieldset>
          {error && <div ref={errorRef} tabIndex={-1} className="notice error" role="alert"><p>{error}</p>{conflict && <button type="button" className="button" disabled={locked || busy} onClick={refresh}>放弃修改并重新加载</button>}</div>}
          {message && <p className="notice" role="status">{message}</p>}
          <div className="settings-actions">
            <button type="submit" className="button primary" disabled={locked || busy || !dirty || conflict}>{busy ? '正在保存…' : '保存 MCP 配置'}</button>
            {!conflict && <button type="button" className="button" disabled={locked || busy} onClick={dirty ? () => { setConfig(snapshot.config); setMessage(''); setError(''); } : refresh}>{dirty ? '撤销修改' : '重新加载'}</button>}
          </div>
        </form>
      </>}
  </>;
}
