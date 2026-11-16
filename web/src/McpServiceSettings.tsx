import { t } from './locale.ts';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import { api, ApiError, errorText } from './api';
import type { McpConfig, McpSnapshot } from './mcp';

function editableSnapshot(snapshot: McpSnapshot): McpSnapshot {
  const { config } = snapshot;
  // Older configurations can keep capability flags set under the master switch.
  if (config.enabled) return snapshot;
  return { ...snapshot, config: { ...config, query: false, modeling: false, publish: false } };
}

export default function McpServiceSettings({ locked, visible, onDirty }: {
  locked: boolean; visible: boolean; onDirty: (value: boolean) => void;
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
    if (locked || !visible || snapshot) return;
    const controller = new AbortController();
    api<McpSnapshot>('/mcp/settings', { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) {
        const editable = editableSnapshot(value);
        setSnapshot(editable); setConfig(editable.config); setError('');
      }
    }, cause => { if (!controller.signal.aborted) setError(errorText(cause)); });
    return () => controller.abort();
  }, [locked, visible, snapshot, attempt]);
  useEffect(() => { onDirty(dirty); }, [dirty, onDirty]);

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
        method: 'PUT', headers: { 'If-Match': '"' + snapshot.revision + '"' },
        body: JSON.stringify({ ...config, enabled: config.query || config.modeling }),
      });
      setSnapshot(next); setConfig(next.config); setMessage(t('MCP 配置已保存并立即生效。'));
    } catch (cause) {
      const changed = cause instanceof ApiError && cause.code === 'mcp_conflict';
      setConflict(changed);
      setError(changed ? t('配置已被其他页面更新。你的修改仍保留，请先重新加载最新配置再编辑。') : errorText(cause));
      requestAnimationFrame(() => errorRef.current?.focus());
    } finally { setBusy(false); }
  }
  function refresh() {
    setSnapshot(undefined); setConfig(undefined); setConflict(false); setMessage(''); setError(''); reload(value => value + 1);
  }
  return <>
      {!snapshot || !config ? <section className="settings-card">
        {error ? <><p className="notice error" role="alert">{error}</p><button type="button" className="button" disabled={locked} onClick={refresh}>{t('重试读取 MCP 配置')}</button></>
          : <p role="status">{t('正在读取 MCP 配置…')}</p>}
      </section> : <>
        <form className="settings-card mcp-form" method="post" action="/api/v1/mcp/settings" aria-labelledby="mcp-title" onSubmit={save} aria-busy={busy}>
          <div><h2 id="mcp-title">{t('MCP 服务')}</h2><p className="settings-hint">{t('查询和建模可独立启停，保存后生效。各服务按用户的模型权限提供工具。')}</p></div>
          <fieldset className="settings-fields" disabled={locked || busy}>
            <legend>{t('查询 MCP')}</legend>
            <div className="mcp-capability">
              <input id="mcp-query" type="checkbox" name="query" checked={config.query} aria-describedby="mcp-query-help" onChange={event => change('query', event.target.checked)} />
              <div><label htmlFor="mcp-query">{t('启用查询服务')}</label><p id="mcp-query-help" className="settings-hint"><code>/mcp/query</code>{t(' · 只读查询已发布模型，支持 Cypher、SQL 和 Gremlin。')}</p></div>
            </div>
            <div className="settings-grid">
              <div className="mcp-field"><label htmlFor="mcp-max-rows">{t('最多返回行数（必填）')}</label><input id="mcp-max-rows" type="number" name="max_rows" min="1" max="500" step="1" required aria-describedby="mcp-rows-help" value={Number.isNaN(config.max_rows) ? '' : config.max_rows} onChange={event => change('max_rows', event.target.valueAsNumber)} /><small id="mcp-rows-help" className="settings-hint">{t('1–500 行，超出时标记截断')}</small></div>
              <div className="mcp-field"><label htmlFor="mcp-timeout">{t('查询超时（毫秒，必填）')}</label><input id="mcp-timeout" type="number" name="timeout_ms" min="1" max="60000" step="1" required aria-describedby="mcp-timeout-help" value={Number.isNaN(config.timeout_ms) ? '' : config.timeout_ms} onChange={event => change('timeout_ms', event.target.valueAsNumber)} /><small id="mcp-timeout-help" className="settings-hint">{t('1–60000 毫秒，服务上限 {0} 毫秒', snapshot.server_timeout_ms)}</small></div>
            </div>
            {config.timeout_ms > snapshot.server_timeout_ms && <p className="settings-hint">{t('受服务上限限制，实际超时为 {0} 毫秒。', snapshot.server_timeout_ms)}</p>}
          </fieldset>
          <fieldset className="settings-fields" disabled={locked || busy}>
            <legend>{t('建模 MCP')}</legend>
            <div className="mcp-capability">
              <input id="mcp-modeling" type="checkbox" name="modeling" checked={config.modeling} aria-describedby="mcp-modeling-help" onChange={event => change('modeling', event.target.checked)} />
              <div><label htmlFor="mcp-modeling">{t('启用建模服务')}</label><p id="mcp-modeling-help" className="settings-hint"><code>/mcp/modeling</code>{t(' · 读取、创建、校验和保存模型草稿。新建须预授权目标标识并复用已批准的数据源和物理表。')}</p></div>
            </div>
            <div className="mcp-capability">
              <input id="mcp-publish" type="checkbox" name="publish" checked={config.publish} disabled={!config.modeling} aria-describedby="mcp-publish-help" onChange={event => change('publish', event.target.checked)} />
              <div><label htmlFor="mcp-publish">{t('允许发布模型')}</label><p id="mcp-publish-help" className="settings-hint">{t('需先启用建模，并授予独立的发布角色。发布会切换后续查询使用的版本；关闭时可在图模型页面人工发布。')}</p></div>
            </div>
          </fieldset>
          {error && <div ref={errorRef} tabIndex={-1} className="notice error" role="alert"><p>{error}</p>{conflict && <button type="button" className="button" disabled={locked || busy} onClick={refresh}>{t('放弃修改并重新加载')}</button>}</div>}
          {message && <p className="notice" role="status">{message}</p>}
          <div className="settings-actions">
            <button type="submit" className="button primary" disabled={locked || busy || !dirty || conflict}>{busy ? t('正在保存…') : t('保存 MCP 配置')}</button>
            {!conflict && <button type="button" className="button" disabled={locked || busy} onClick={dirty ? () => { setConfig(snapshot.config); setMessage(''); setError(''); } : refresh}>{dirty ? t('撤销修改') : t('重新加载')}</button>}
          </div>
        </form>
      </>}
  </>;
}
