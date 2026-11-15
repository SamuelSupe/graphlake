import { useCallback, useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { Plus, RefreshCw, X } from 'lucide-react';
import { api, errorText, type ModelSummary } from './api';
import { ModelHealth } from './ModelHealth';
import BackupStatus from './BackupStatus';
import McpServiceSettings from './McpServiceSettings';

const tabs = [
  ['telemetry', '日志与遥测'],
  ['health', '健康检查'],
  ['backup', '备份与恢复'],
  ['mcp', 'MCP 服务'],
] as const;
export type SettingsTab = typeof tabs[number][0];

type Signal = 'metrics' | 'logs' | 'traces';
type Config = {
  enabled: boolean; endpoint: string | null; service_name: string; instance_id: string | null;
  log_format: 'text' | 'json'; metrics: boolean; logs: boolean; traces: boolean;
  trace_sample_ratio: number; export_interval_ms: number; timeout_ms: number;
  allow_insecure_http: boolean; include_audit_identity: boolean; headers_env: Record<string, string>;
};
export type SettingsSnapshot = {
  active: Config; config: Config; revision: string;
  restart_required: boolean; role: 'coordinator' | 'standalone' | null; file: string | null;
  signals: { name: Signal; failures: number; rejected: number; last_success_ms: number | null }[];
};
type Header = { id: number; name: string; variable: string };
const names: Record<Signal, string> = { metrics: '指标', logs: '日志', traces: '链路' };

function Toggle({ name, label, checked, change }: { name: string; label: string; checked: boolean; change: (value: boolean) => void }) {
  const id = useId();
  return <div className="settings-toggle">
    <input id={id} name={name} type="checkbox" checked={checked} onChange={event => change(event.target.checked)} />
    <label htmlFor={id}>{label}</label>
  </div>;
}

export default function Settings({ models, locked, onDirty, openModel, initialTab }: { initialTab?: SettingsTab; openModel: (id: string, workspace: 'graph' | 'catalogs') => void; models: ModelSummary[]; locked: boolean; onDirty: (key: string, value: boolean) => void }) {
  const [tab, setTab] = useState<SettingsTab>(initialTab ?? 'telemetry');
  const tabId = useId();
  const tabButton = (value: SettingsTab) => tabId + '-' + value;
  const tabPanel = (value: SettingsTab) => tabButton(value) + '-panel';
  const panel = (value: SettingsTab) => ({
    id: tabPanel(value), role: 'tabpanel', 'aria-labelledby': tabButton(value), hidden: tab !== value, 'data-settings-panel': value,
  });
  const [snapshot, setSnapshot] = useState<SettingsSnapshot>();
  const [config, setConfig] = useState<Config>();
  const [headers, setHeaders] = useState<Header[]>([]);
  const headerId = useRef(0);
  const errorRef = useRef<HTMLParagraphElement>(null);
  const dirty = useRef(false);
  const [busy, setBusy] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [attempt, reload] = useState(0);
  const [mcpDirty, setMcpDirty] = useState(false);
  const onMcpDirty = useCallback((value: boolean) => {
    setMcpDirty(value);
    onDirty('/settings/mcp', value);
  }, [onDirty]);

  function adopt(next: SettingsSnapshot) {
    setSnapshot(next); setConfig(next.config);
    setHeaders(Object.entries(next.config.headers_env).map(([name, variable]) => ({ id: headerId.current++, name, variable })));
    dirty.current = false; onDirty('/settings', false);
  }

  useEffect(() => {
    if (locked) return;
    const controller = new AbortController();
    api<SettingsSnapshot>('/settings', { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) {
        setError('');
        if (dirty.current) setSnapshot(previous => previous && { ...previous, signals: value.signals });
        else adopt(value);
      }
    }, cause => { if (!controller.signal.aborted) setError(errorText(cause)); });
    return () => controller.abort();
  }, [attempt, locked, onDirty]);
  useEffect(() => () => {
    onDirty('/settings', false);
    onDirty('/settings/mcp', false);
  }, [onDirty]);

  function changed() {
    dirty.current = true; onDirty('/settings', true); setMessage(''); setError('');
  }
  function change<K extends keyof Config>(key: K, value: Config[K]) {
    setConfig(current => current && { ...current, [key]: value }); changed();
  }

  async function refreshStatus() {
    setRefreshing(true);
    try {
      const next = await api<SettingsSnapshot>('/settings');
      if (dirty.current) setSnapshot(previous => previous && { ...previous, signals: next.signals });
      else adopt(next);
    } catch (cause) { setError(errorText(cause)); }
    finally { setRefreshing(false); }
  }

  async function save(event: FormEvent) {
    event.preventDefault();
    if (locked || busy || !config || !snapshot?.file) return;
    setBusy(true); setMessage(''); setError('');
    try {
      const entries = headers.map(({ name, variable }) => [name.trim().toLowerCase(), variable.trim()]);
      if (new Set(entries.map(([name]) => name)).size !== entries.length) throw new Error('同一个 HTTP 请求头只能配置一次。');
      const next = await api<SettingsSnapshot>('/settings', {
        method: 'PUT', headers: { 'If-Match': '"' + snapshot.revision + '"' },
        // PUT replaces the document, so preserve server fields this form does not edit.
        body: JSON.stringify({ ...config, headers_env: Object.fromEntries(entries) }),
      });
      adopt(next);
      setMessage(next.restart_required ? '配置已保存。重启此进程后应用运行配置。' : '配置已保存。');
    } catch (cause) {
      setError(errorText(cause));
      requestAnimationFrame(() => errorRef.current?.focus());
    } finally { setBusy(false); }
  }

  function reloadConfig() {
    if (dirty.current && !window.confirm('重新加载会放弃本页未保存的修改。继续？')) return;
    if (snapshot) adopt(snapshot);
    setMessage(''); setError(''); reload(value => value + 1);
  }

  const readOnly = !snapshot?.file || locked || busy;
  const editableTab = tab === 'telemetry';
  return <main className="workbench">
    <header className="workspace-header">
      <div className="workspace-title"><h1>运行配置</h1></div>
      <div className="workspace-context">
        {(dirty.current || mcpDirty) && <span className="dirty"><i />未保存</span>}
        {editableTab && snapshot && <>
        <button className="button compact" type="button" disabled={busy} onClick={reloadConfig}>重新加载配置</button>
        <button className="button primary compact" type="submit" form="runtime-settings" disabled={readOnly}>{busy ? '正在保存…' : '保存配置'}</button>
        </>}
      </div>
    </header>
    <div className="settings-tabs" role="tablist" aria-label="运行配置分类">
      {tabs.map(([key, label], index) => <button key={key} id={tabButton(key)} type="button" role="tab"
        autoFocus={key === 'mcp' && initialTab === 'mcp'}
        aria-controls={tabPanel(key)} aria-selected={key === tab} tabIndex={key === tab ? 0 : -1} onClick={() => setTab(key)} onKeyDown={event => {
          const next = event.key === 'ArrowRight' ? (index + 1) % tabs.length
            : event.key === 'ArrowLeft' ? (index + tabs.length - 1) % tabs.length
            : event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : null;
          if (next === null) return;
          event.preventDefault();
          const key = tabs[next][0];
          setTab(key);
          document.getElementById(tabButton(key))?.focus();
        }}>{label}</button>)}
    </div>
    {(editableTab || tab === 'health') && error && <p ref={errorRef} tabIndex={-1} role="alert" className="notice error settings-feedback">{error}</p>}
    {editableTab && message && <p role="status" className="notice settings-feedback">{message}</p>}
    {(!config || !snapshot) && <section {...panel('telemetry')} aria-busy={!error}>
      <div className="settings-workspace">
        {error ? <button className="button" type="button" onClick={reloadConfig}>重试读取配置</button> : <p role="status">正在读取配置…</p>}
      </div>
    </section>}
    {config && snapshot && <form id="runtime-settings" method="post" action="/api/v1/settings" onSubmit={save} onInvalidCapture={event => {
      const field = event.target;
      if (!(field instanceof HTMLInputElement)) return;
      event.preventDefault();
      if (event.currentTarget.querySelector('input:invalid') !== field) return;
      setTab('telemetry');
      for (let parent = field.parentElement; parent && parent !== event.currentTarget; parent = parent.parentElement) {
        if (parent instanceof HTMLDetailsElement) parent.open = true;
      }
      setError(field.validationMessage);
      requestAnimationFrame(() => field.focus());
    }}>
      <section {...panel('telemetry')}><div className="settings-workspace">
        <div className="settings-card">
          <h2 id="telemetry-title">指标、日志与链路</h2>
          <div className="settings-status" role="status">
            <span>当前 OTLP <strong>{snapshot.active.enabled ? '已开启' : '已关闭'}</strong></span>
            <span>当前本地日志 <strong>{snapshot.active.log_format.toUpperCase()}</strong></span>
            {snapshot.restart_required && <span className="settings-pending">待重启</span>}
          </div>
          {snapshot.active.enabled && <p className="settings-hint">生效接收端 <code>{snapshot.active.endpoint}</code><br />
            已启用信号 {Object.entries(names).filter(([key]) => snapshot.active[key as Signal]).map(([, name]) => name).join('、')}</p>}
          {!snapshot.file && <p className="notice">为该进程通过 <code>--telemetry-config</code> 指定本机配置文件后，可在此编辑。</p>}
          <fieldset className="settings-fields" disabled={readOnly}>
            <legend>本地日志</legend>
            <div className="settings-actions">
              {(['text', 'json'] as const).map(value => <label className="settings-toggle" key={value}>
                <input name="log_format" type="radio" value={value} checked={config.log_format === value} onChange={() => change('log_format', value)} />
                {value === 'text' ? '纯文本' : '结构化 JSON'}
              </label>)}
            </div>
          </fieldset>
          <fieldset className="settings-fields" disabled={readOnly}>
            <legend>OpenTelemetry 上报</legend>
            <Toggle name="enabled" label="启用 OTLP 上报" checked={config.enabled} change={value => change('enabled', value)} />
            <label>Collector 地址{config.enabled ? '（必填）' : ''}<input type="url" name="endpoint" placeholder="http://127.0.0.1:4318"
              autoCapitalize="none" spellCheck={false} required={config.enabled} value={config.endpoint ?? ''} onChange={event => change('endpoint', event.target.value || null)} /></label>
            <p className="settings-hint">填写 OTLP/HTTP 基址。服务会追加各信号的路径。</p>
            <div className="settings-actions">
              {(Object.keys(names) as Signal[]).map(key => <Toggle key={key} name={key} label={names[key]} checked={config[key]} change={value => change(key, value)} />)}
            </div>
            <details className="form-disclosure"><summary>高级上报设置</summary><div className="form-fields">
              <div className="settings-grid">
                <label>服务名称（必填）<input name="service_name" required maxLength={128} value={config.service_name} onChange={event => change('service_name', event.target.value)} /></label>
                <label>实例标识<input name="instance_id" maxLength={128} value={config.instance_id ?? ''} placeholder="启动时生成" onChange={event => change('instance_id', event.target.value || null)} /></label>
                <label>链路采样率<input name="trace_sample_ratio" type="number" min="0" max="1" step="any" required value={Number.isNaN(config.trace_sample_ratio) ? '' : config.trace_sample_ratio} onChange={event => change('trace_sample_ratio', event.target.valueAsNumber)} /></label>
                <label>指标上报间隔（毫秒）<input name="export_interval_ms" type="number" min="1000" max="300000" required value={Number.isNaN(config.export_interval_ms) ? '' : config.export_interval_ms} onChange={event => change('export_interval_ms', event.target.valueAsNumber)} /></label>
                <label>单次上报超时（毫秒）<input name="timeout_ms" type="number" min="100" max="5000" required value={Number.isNaN(config.timeout_ms) ? '' : config.timeout_ms} onChange={event => change('timeout_ms', event.target.valueAsNumber)} /></label>
              </div>
              <Toggle name="include_audit_identity" label="上报审计中的账号、模型及目标名称" checked={config.include_audit_identity} change={value => change('include_audit_identity', value)} />
              <Toggle name="allow_insecure_http" label="允许通过明文 HTTP 连接非回环 Collector" checked={config.allow_insecure_http} change={value => change('allow_insecure_http', value)} />
              {snapshot.file && <p className="settings-hint">配置文件 <code>{snapshot.file}</code></p>}
            </div></details>
          </fieldset>
          <details className="form-disclosure"><summary>Collector 请求头{headers.length > 0 && <small>{headers.length} 项</small>}</summary>
          <fieldset className="settings-fields" disabled={readOnly}>
            <legend>凭据绑定</legend>
            <p className="settings-hint">填写服务主机的环境变量名。新增凭据绑定使用 <code>GRAPHLAKE_OTLP_</code> 前缀和部署允许的接收端。</p>
            {headers.map(row => <div className="settings-header-row" key={row.id}>
              <label>请求头名称<input name={'header_' + row.id} required maxLength={128} placeholder="authorization" spellCheck={false} value={row.name} onChange={event => { const name = event.target.value; setHeaders(values => values.map(value => value.id === row.id ? { ...value, name } : value)); changed(); }} /></label>
              <label>环境变量<input name={'variable_' + row.id} required maxLength={128} pattern="[A-Za-z0-9_]+" placeholder="GRAPHLAKE_OTLP_AUTHORIZATION" spellCheck={false} value={row.variable} onChange={event => { const variable = event.target.value; setHeaders(values => values.map(value => value.id === row.id ? { ...value, variable } : value)); changed(); }} /></label>
              <button className="button" type="button" aria-label={'移除请求头 ' + (row.name || '空白项')} onClick={() => { setHeaders(values => values.filter(value => value.id !== row.id)); changed(); }}><X size={16} aria-hidden="true" /></button>
            </div>)}
            <button className="button" type="button" disabled={headers.length >= 16} onClick={() => { setHeaders(values => [...values, { id: headerId.current++, name: '', variable: '' }]); changed(); }}>
              <Plus size={16} aria-hidden="true" />添加请求头
            </button>
          </fieldset>
          </details>
        </div>
      </div></section>
    </form>}
    <section {...panel('health')}>
      <div className="settings-workspace settings-health">
        <ModelHealth models={models} openModel={openModel} />
        <section className="settings-card">
          <div className="settings-section-title"><h2>上报状态</h2><button className="button compact" type="button" disabled={!snapshot || refreshing}
            onClick={() => void refreshStatus()}><RefreshCw size={14} aria-hidden="true" />刷新状态</button></div>
          <p className="settings-hint">当前进程的上报记录。失败及拒收自进程启动累计。</p>
          {snapshot && <dl className="settings-signals" aria-live="polite">
            {snapshot.signals.map(({ name, failures, rejected, last_success_ms }) => <div key={name}>
              <dt>{names[name]}</dt><dd>{snapshot.active.enabled && snapshot.active[name] ? <>
                <span>失败 {failures} 次</span>
                <span>拒收 {rejected} 条</span>
                <span>最近完整接收 {last_success_ms ? new Date(last_success_ms).toLocaleString() : '暂无'}</span>
              </> : '已关闭'}</dd>
            </div>)}
          </dl>}
          <div className="settings-links">
            <a href="/metrics" target="_blank" rel="noreferrer">Prometheus 指标</a>
            <a href="/readyz" target="_blank" rel="noreferrer">服务就绪探针</a>
          </div>
        </section>
      </div>
    </section>
    <section {...panel('backup')}><BackupStatus visible={tab === 'backup' && !locked} /></section>
    <section {...panel('mcp')}><div className="settings-workspace">
      <McpServiceSettings locked={locked} visible={tab === 'mcp'} onDirty={onMcpDirty} />
    </div></section>
  </main>;
}
