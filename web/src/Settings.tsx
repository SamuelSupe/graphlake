import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { Activity, Download, Plus, RefreshCw, X } from 'lucide-react';
import { api, ApiError, downloadText, errorText, type ModelReadiness, type ModelSummary } from './api';
import BackupStatus from './BackupStatus';

const tabs = [
  ['telemetry', '日志与遥测'],
  ['alerts', '告警规则'],
  ['health', '健康检查'],
  ['backup', '备份与恢复'],
] as const;
type Tab = typeof tabs[number][0];

type Signal = 'metrics' | 'logs' | 'traces';
type AlertRule = { enabled: boolean; delay_seconds: number; window_seconds: number | null; threshold: number | null };
type Alerts = Record<string, AlertRule>;
type Config = {
  enabled: boolean; endpoint: string | null; service_name: string; instance_id: string | null;
  log_format: 'text' | 'json'; metrics: boolean; logs: boolean; traces: boolean;
  trace_sample_ratio: number; export_interval_ms: number; timeout_ms: number;
  allow_insecure_http: boolean; include_audit_identity: boolean; headers_env: Record<string, string>; alerts: Alerts;
};
type Snapshot = {
  active: Config; config: Config; alerts: Alerts; rules_yaml: string; revision: string;
  restart_required: boolean; role: 'coordinator' | 'standalone' | null; file: string | null;
  signals: { name: Signal; failures: number; rejected: number; last_success_ms: number | null }[];
};
type Header = { id: number; name: string; variable: string };
const names: Record<Signal, string> = { metrics: '指标', logs: '日志', traces: '链路' };
const alertNames: Record<string, string> = {
  GraphLakeMetricsUnavailable: 'Coordinator 指标不可达',
  GraphLakeRequestFailures: '持续 HTTP 5xx',
  GraphLakeAuditWriteFailures: '审计写入失败',
  GraphLakeCoordinatorNotReady: 'HA 主节点未就绪',
  GraphLakeOtlpExportFailed: 'OTLP 上报失败',
  GraphLakeOtlpPartialRejected: 'OTLP 部分拒收',
  GraphLakeMonitorUnavailable: '监控探针不可用',
  GraphLakeCandidateUnavailable: 'Coordinator 候选进程不可达',
  GraphLakeLeaderUnavailable: '主权或主节点就绪异常',
  GraphLakeWorkerUnavailable: 'Worker 不可达',
  GraphLakeIngressUnavailable: '客户端入口不可达',
  GraphLakeModelDiscoveryFailed: '已发布模型发现失败',
  GraphLakeModelUnavailable: '模型或来源不可用',
  GraphLakeBackupFailed: '备份或离线校验失败',
  GraphLakeBackupStale: '备份恢复点过期',
};
const probeLabels = {
  ready: '可用', source_unavailable: '数据源不可用', mapping_invalid: '映射校验失败', worker_unavailable: 'Worker 不可用',
};

function Toggle({ name, label, checked, change }: { name: string; label: string; checked: boolean; change: (value: boolean) => void }) {
  const id = useId();
  return <div className="settings-toggle">
    <input id={id} name={name} type="checkbox" checked={checked} onChange={event => change(event.target.checked)} />
    <label htmlFor={id}>{label}</label>
  </div>;
}

function Probe({ models }: { models: ModelSummary[] }) {
  const [selected, select] = useState('');
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<ModelReadiness>();
  const [error, setError] = useState('');
  const candidates = models.filter(model => model.active_revision);
  const id = candidates.some(model => model.id === selected) ? selected : candidates[0]?.id ?? '';
  async function check(event: FormEvent) {
    event.preventDefault();
    setRunning(true); setResult(undefined); setError('');
    try { setResult(await api<ModelReadiness>('/models/' + id + '/readiness')); }
    catch (cause) {
      if (cause instanceof ApiError && cause.readiness) setResult(cause.readiness);
      else setError(errorText(cause));
    } finally { setRunning(false); }
  }
  return <section className="settings-card">
    <h2>模型探针</h2>
    <p className="settings-hint">检查生效版本的来源、端点映射及 Worker。</p>
    {candidates.length ? <form aria-label="模型探针" method="get" onSubmit={check} className="settings-stack">
      <label>已发布模型<select name="model" value={id} disabled={running} onChange={event => {
        select(event.target.value); setResult(undefined); setError('');
      }}>{candidates.map(model => <option key={model.id} value={model.id}>{model.name}</option>)}</select></label>
      <button className="button" type="submit" disabled={running}><Activity size={16} aria-hidden="true" />{running ? '正在检查…' : '检查模型'}</button>
    </form> : <p className="settings-hint">暂无已发布模型。</p>}
    {error && <p role="alert" className="notice error">{error}</p>}
    {result && <div className={result.status === 'ready' ? 'notice' : 'notice error'} role="status">
      <strong>{probeLabels[result.status]}</strong>
      <ul className="settings-probes">
        {Object.entries(result.sources).map(([name, ok]) => <li key={name}><code>{name}</code> · {ok ? '可用' : '不可用'}</li>)}
        {result.workers?.map(({ worker, probe }) => <li key={worker}>Worker {worker} · {probe ? probeLabels[probe.status] : '不可达'}
          {probe && Object.entries(probe.sources).filter(([, available]) => !available).map(([name]) => <span key={name}> · <code>{name}</code></span>)}</li>)}
      </ul>
    </div>}
  </section>;
}

export default function Settings({ models, locked, onDirty }: { models: ModelSummary[]; locked: boolean; onDirty: (key: string, value: boolean) => void }) {
  const [tab, setTab] = useState<Tab>('telemetry');
  const tabId = useId();
  const tabButton = (value: Tab) => tabId + '-' + value;
  const tabPanel = (value: Tab) => tabButton(value) + '-panel';
  const panel = (value: Tab) => ({
    id: tabPanel(value), role: 'tabpanel', 'aria-labelledby': tabButton(value), hidden: tab !== value, 'data-settings-panel': value,
  });
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [config, setConfig] = useState<Config>();
  const [alerts, setAlerts] = useState<Alerts>({});
  const [headers, setHeaders] = useState<Header[]>([]);
  const headerId = useRef(0);
  const errorRef = useRef<HTMLParagraphElement>(null);
  const dirty = useRef(false);
  const [busy, setBusy] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [attempt, reload] = useState(0);

  function adopt(next: Snapshot) {
    setSnapshot(next); setConfig(next.config); setAlerts(next.alerts);
    setHeaders(Object.entries(next.config.headers_env).map(([name, variable]) => ({ id: headerId.current++, name, variable })));
    dirty.current = false; onDirty('/settings', false);
  }

  useEffect(() => {
    if (locked) return;
    const controller = new AbortController();
    api<Snapshot>('/settings', { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) {
        setError('');
        if (dirty.current) setSnapshot(previous => previous && { ...previous, signals: value.signals });
        else adopt(value);
      }
    }, cause => { if (!controller.signal.aborted) setError(errorText(cause)); });
    return () => controller.abort();
  }, [attempt, locked, onDirty]);
  useEffect(() => () => onDirty('/settings', false), [onDirty]);

  function changed() {
    dirty.current = true; onDirty('/settings', true); setMessage(''); setError('');
  }
  function change<K extends keyof Config>(key: K, value: Config[K]) {
    setConfig(current => current && { ...current, [key]: value }); changed();
  }
  function changeAlert(name: string, patch: Partial<AlertRule>) {
    setAlerts(current => ({ ...current, [name]: { ...current[name], ...patch } })); changed();
  }

  async function refreshStatus() {
    setRefreshing(true);
    try {
      const next = await api<Snapshot>('/settings');
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
      const next = await api<Snapshot>('/settings', {
        method: 'PUT', headers: { 'If-Match': '"' + snapshot.revision + '"' },
        body: JSON.stringify({ ...config, headers_env: Object.fromEntries(entries), alerts }),
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
  const editableTab = tab === 'telemetry' || tab === 'alerts';
  return <main className="workbench">
    <header className="workspace-header">
      <div className="workspace-title"><h1>运行配置</h1></div>
      <div className="workspace-context">
        {dirty.current && <span className="dirty"><i />未保存</span>}
        {editableTab && snapshot && <>
        <button className="button compact" type="button" disabled={busy} onClick={reloadConfig}>重新加载配置</button>
        <button className="button primary compact" type="submit" form="runtime-settings" disabled={readOnly}>{busy ? '正在保存…' : '保存配置'}</button>
        </>}
      </div>
    </header>
    <div className="settings-tabs" role="tablist" aria-label="运行配置分类">
      {tabs.map(([key, label], index) => <button key={key} id={tabButton(key)} type="button" role="tab"
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
    {tab !== 'backup' && error && <p ref={errorRef} tabIndex={-1} role="alert" className="notice error settings-feedback">{error}</p>}
    {editableTab && message && <p role="status" className="notice settings-feedback">{message}</p>}
    {(!config || !snapshot) && (['telemetry', 'alerts'] as const).map(key => <section key={key} {...panel(key)} aria-busy={!error}>
      <div className="settings-workspace">
        {error ? <button className="button" type="button" onClick={reloadConfig}>重试读取配置</button> : <p role="status">正在读取配置…</p>}
      </div>
    </section>)}
    {config && snapshot && <form id="runtime-settings" method="post" action="/api/v1/settings" onSubmit={save} onInvalidCapture={event => {
      const field = event.target;
      if (!(field instanceof HTMLInputElement)) return;
      event.preventDefault();
      if (event.currentTarget.querySelector('input:invalid') !== field) return;
      const invalidTab = field.closest<HTMLElement>('[data-settings-panel]')?.dataset.settingsPanel;
      if (invalidTab === 'telemetry' || invalidTab === 'alerts') setTab(invalidTab);
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
      <section {...panel('alerts')}><div className="settings-workspace">
        <div className="settings-card">
          <div className="settings-section-title">
            <h2>Prometheus 告警规则</h2>
            <button className="button compact" type="button" onClick={() => downloadText(snapshot.rules_yaml, 'prometheus-rules.yaml')}>
              <Download size={15} aria-hidden="true" />下载规则
            </button>
          </div>
          <p className="settings-hint">保存后下载规则文件，由监控服务器加载。通知渠道由 Alertmanager 管理。</p>
          <div className="settings-rule-list">
          {Object.entries(alerts).map(([name, rule]) => <details className="form-disclosure" key={name}>
            <summary>{alertNames[name] ?? name}{!rule.enabled && <small>已暂停</small>}</summary>
            <fieldset className="settings-fields alert-fields" disabled={readOnly}>
            <legend className="sr-only">{alertNames[name] ?? name}</legend>
            <Toggle name={name + '_enabled'} label="启用规则" checked={rule.enabled} change={value => changeAlert(name, { enabled: value })} />
            <div className="settings-grid settings-rule-grid">
              <label>触发等待（秒）<input name={name + '_delay'} type="number" min="0" max="86400" required value={Number.isNaN(rule.delay_seconds) ? '' : rule.delay_seconds}
                onChange={event => changeAlert(name, { delay_seconds: event.target.valueAsNumber })} /></label>
              {rule.window_seconds !== null && <label>统计窗口（秒）<input name={name + '_window'} type="number" min="15" max="86400" required value={Number.isNaN(rule.window_seconds) ? '' : rule.window_seconds}
                onChange={event => changeAlert(name, { window_seconds: event.target.valueAsNumber })} /></label>}
              {rule.threshold !== null && <label>{name === 'GraphLakeRequestFailures' ? '每秒错误阈值' : '窗口内错误增量阈值'}<input name={name + '_threshold'} type="number" min="0" max="1000000" step="any" required value={Number.isNaN(rule.threshold) ? '' : rule.threshold}
                onChange={event => changeAlert(name, { threshold: event.target.valueAsNumber })} /></label>}
            </div>
            </fieldset>
          </details>)}
          </div>
        </div>
      </div></section>
    </form>}
    <section {...panel('health')}>
      <div className="settings-workspace settings-health">
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
        <Probe models={models} />
      </div>
    </section>
    <section {...panel('backup')}><BackupStatus visible={tab === 'backup' && !locked} /></section>
  </main>;
}
