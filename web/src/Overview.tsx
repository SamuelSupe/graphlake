import { locale, t } from './locale.ts';
import { useEffect, useState, type ReactNode } from 'react';
import { Activity, ArrowRight, Clock3, DatabaseBackup, Network, RefreshCw, Server, Terminal, TriangleAlert } from 'lucide-react';
import type { BackupSnapshot } from './BackupStatus';
import type { ModelSummary } from './api';
import { ModelHealth } from './ModelHealth';
import { OverviewCharts } from './OverviewCharts';
import { readOverview, type OverviewSnapshot } from './overviewData';
import { historyWindowMs, type RuntimeSample } from './overviewSeries';
import type { SettingsSnapshot, SettingsTab } from './Settings';
import './overview.css';

type Tone = 'ok' | 'warning' | 'error' | 'neutral';
const readinessLabels: Record<string, [string, Tone]> = {
  ready: [t('服务就绪'), 'ok'],
  degraded: [t('部分模型不可用'), 'warning'],
  draining: [t('服务正在停止'), 'warning'],
  recovery_pending: [t('等待灾备恢复'), 'warning'],
  leadership_uncertain: [t('Coordinator 主权未确认'), 'error'],
  probe_in_progress: [t('就绪检查进行中'), 'neutral'],
  catalog_unavailable: [t('模型目录不可用'), 'error'],
  cluster_metadata_unavailable: [t('集群元数据不可用'), 'error'],
  worker_unavailable: [t('没有可用的 Worker'), 'error'],
};
const signalLabels = { metrics: t('指标'), logs: t('日志'), traces: t('链路') };
const backupJobs = [['export', t('控制状态备份')], ['offline', t('异地副本校验')]] as const;
const number = new Intl.NumberFormat(locale);

function time(ms: number | null, clock = false) {
  if (ms === null) return t('暂无');
  const date = new Date(ms);
  return <time dateTime={date.toISOString()}>{clock ? date.toLocaleTimeString(locale) : date.toLocaleString(locale)}</time>;
}

function uptime(seconds: number) {
  if (seconds < 60) return t('不足 1 分钟');
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  if (hours >= 24) return t('{0} 天 {1} 小时', Math.floor(hours / 24), (hours % 24));
  return (hours ? t('{0} 小时 ', hours) : '') + (minutes % 60) + t(' 分钟');
}

function Status({ tone = 'neutral', children }: { tone?: Tone; children: ReactNode }) {
  return <span className="overview-status" data-tone={tone}>{children}</span>;
}

function Stat({ label, value, detail }: { label: string; value?: number; detail: ReactNode }) {
  return <div className="overview-stat">
    <dt>{label}</dt><dd>{value === undefined ? '—' : number.format(value)}</dd>
    <dd className="overview-stat-detail">{detail}</dd>
  </div>;
}

function Unavailable({ error }: { error: string }) {
  return <p className="overview-error">{t('暂不可用 · {0}', error)}</p>;
}

function TelemetrySummary({ telemetry }: { telemetry: SettingsSnapshot }) {
  const signals = telemetry.signals.filter(({ name }) => telemetry.active.enabled && telemetry.active[name]);
  return <>
    <div className="overview-row">
      <span>{t('OTLP 上报')}</span>
      <Status>{telemetry.active.enabled ? t('已启用') : t('未启用')}</Status>
    </div>
    {telemetry.restart_required && <p className="overview-warning">{t('运行配置已保存，等待重启生效。')}</p>}
    {!!signals.length && <dl className="overview-rows">
      {signals.map(({ name, failures, rejected, last_success_ms }) => <div key={name}>
        <dt>{signalLabels[name]}</dt>
        <dd>{t('失败 {0} 次 · 拒收 {1} 条', number.format(failures), number.format(rejected))}</dd>
        <dd className="muted">{t('最近完整接收 {0}', time(last_success_ms))}</dd>
      </div>)}
    </dl>}
    {telemetry.active.enabled && !signals.length && <p className="muted">{t('未启用上报信号')}</p>}
  </>;
}

function BackupJob({ name, job }: { name: string; job: BackupSnapshot['export'] }) {
  const result = job.state === 'recorded' ? job.result : undefined;
  const label = result ? result.ok ? t('最近一次成功') : t('最近一次失败') : job.state === 'unavailable' ? t('状态不可读') : t('暂无记录');
  const tone = result ? result.ok ? 'ok' : 'error' : job.state === 'unavailable' ? 'warning' : 'neutral';
  return <div className="overview-backup-job">
    <dt>{name}</dt><dd><Status tone={tone}>{label}</Status></dd>
    {result && !result.ok && <dd className="muted small">{t('最近尝试 {0}', time(result.attempted_ms))}</dd>}
    {result && <dd className="muted small">{t('最近成功 {0}', time(result.last_success_ms))}</dd>}
    {result && result.checkpoint_ms !== null && <dd className="muted small">{t('数据检查点 {0}', time(result.checkpoint_ms))}</dd>}
  </div>;
}

type Props = {
  active: boolean;
  openModel: (id: string, page: 'graph' | 'catalogs') => void;
  openSettings: (tab: SettingsTab) => void;
};

export default function Overview({ active, openModel, openSettings }: Props) {
  const [snapshot, setSnapshot] = useState<OverviewSnapshot>();
  const [loadedModels, setLoadedModels] = useState<ModelSummary[]>();
  const [samples, setSamples] = useState<RuntimeSample[]>([]);
  const [loading, setLoading] = useState(true);
  const [attempt, retry] = useState(0);

  useEffect(() => {
    if (!active) return;
    let disposed = false;
    let pending: AbortController | undefined;
    async function refresh() {
      if (document.hidden || pending) return;
      const controller = new AbortController();
      pending = controller;
      setLoading(true);
      const timeout = window.setTimeout(() => controller.abort(), 10_000);
      try {
        const next = await readOverview(controller.signal);
        if (disposed) return;
        setSnapshot(next);
        if (next.models.data) setLoadedModels(next.models.data);
        const runtime = next.runtime.data;
        const at = runtime?.at ?? next.at;
        setSamples(previous => {
          const recent = previous.filter(point => point.at > at - historyWindowMs && point.at < at);
          return [...recent.slice(-119), { at, runtime }];
        });
      } finally {
        window.clearTimeout(timeout);
        pending = undefined;
        if (!disposed) setLoading(false);
      }
    }
    function onVisible() { if (!document.hidden) void refresh(); }
    void refresh();
    const timer = window.setInterval(() => void refresh(), 30_000);
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      disposed = true;
      pending?.abort();
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [active, attempt]);

  const ready = snapshot?.readiness.data;
  const readinessStatus = ready?.status === 'degraded' && ready.published_models === 0
    ? [t('发布模型不可用'), 'error'] as const : ready && readinessLabels[ready.status];
  const [label, tone]: readonly [string, Tone] = ready ? readinessStatus ?? [t('就绪状态未知'), 'neutral']
    : snapshot ? [t('就绪检查不可用'), 'error'] : [t('正在读取'), 'neutral'];
  const runtime = snapshot?.runtime.data;
  const modelError = snapshot?.models.error;
  const models = loadedModels?.filter(model => model.active_revision || model.archive_error);
  const telemetry = snapshot?.telemetry.data;
  const backups = snapshot?.backups.data;
  const issues: [target: string, message: string][] = [];
  const unavailableModels = Math.max(ready?.unavailable_models ?? 0, models?.filter(model => model.archive_error).length ?? 0);
  if (modelError) issues.push(['models', t('模型列表读取失败')]);
  else if (unavailableModels) issues.push(['models', t('{0} 个模型归档异常', unavailableModels)]);
  if (snapshot?.runtime.error) issues.push(['metrics', t('运行指标读取失败')]);
  else if (runtime?.auditFailures) issues.push(['metrics', t('审计写入累计失败 {0} 次', number.format(runtime.auditFailures))]);
  if (snapshot?.telemetry.error) issues.push(['telemetry', t('遥测状态读取失败')]);
  else if (telemetry?.restart_required) issues.push(['telemetry', t('遥测配置待重启')]);
  if (snapshot?.backups.error) issues.push(['backups', t('备份状态读取失败')]);
  for (const [key, name] of backupJobs) {
    const job = backups?.[key];
    if (job?.state === 'unavailable') issues.push(['backups', t('{0}状态不可读', name)]);
    else if (job?.state === 'recorded' && !job.result.ok) issues.push(['backups', t('{0}失败', name)]);
  }
  return <main className="workbench framed-workspace overview">
    <header className="workspace-header">
      <div className="workspace-title"><h1>{t('平台概览')}</h1></div>
      <div className="workspace-context">
        <span className="overview-updated"><Clock3 size={14} aria-hidden="true" />
          {snapshot ? <>{t('采集于 ')}{time(snapshot.at, true)}{t(' · 每 30 秒刷新')}</> : t('正在采集状态')}
        </span>
        <button type="button" className="button compact" aria-disabled={loading || !active}
          onClick={() => { if (!loading && active) retry(value => value + 1); }}>
          <RefreshCw size={15} aria-hidden="true" />{loading && snapshot ? t('正在刷新…') : t('刷新状态')}
        </button>
      </div>
    </header>
    <div className="overview-body">
      <section className="overview-service" data-tone={tone}>
        <div className="overview-service-title">
          <span className="overview-service-icon"><Server size={22} aria-hidden="true" /></span>
          <div><h2>{t('服务状态')}</h2><p className="muted">{t('当前服务入口')}</p></div>
          <div role="status"><Status tone={tone}>{label}</Status></div>
        </div>
        <div className="overview-service-details">
          {telemetry?.role && <span>{telemetry.role === 'coordinator' ? 'Coordinator' : t('单机')}</span>}
          {snapshot?.health.data && <span>v{snapshot.health.data.version}</span>}
          {runtime && <span>{t('已运行 {0}', uptime(runtime.uptime))}</span>}
        </div>
        {snapshot?.readiness.error && <Unavailable error={snapshot.readiness.error} />}
        {snapshot?.health.error && <p className="overview-error">{t('服务版本暂不可读 · {0}', snapshot.health.error)}</p>}
      </section>

      {!!issues.length && <section className="overview-attention" aria-labelledby="overview-attention-title">
        <h2 id="overview-attention-title"><TriangleAlert size={17} aria-hidden="true" />{t('需关注')}</h2>
        <ul>{issues.map(([target, message]) => <li key={message}>
          <a href={'#overview-' + target}>{message}<ArrowRight size={14} aria-hidden="true" /></a>
        </li>)}</ul>
      </section>}

      <section className="overview-load" id="overview-metrics" tabIndex={-1}>
        <div className="overview-section-heading"><h2><Activity size={17} aria-hidden="true" />{t('运行指标')}</h2><span className="muted small">{t('当前进程')}</span></div>
        {snapshot?.runtime.error && <Unavailable error={snapshot.runtime.error} />}
        <dl className="overview-stats">
          <Stat label={t('运行中查询')} value={runtime?.active} detail={runtime ? t('并发槽位 {0}', number.format(runtime.slots)) : t('并发槽位 —')} />
          <Stat label={t('等待中查询')} value={runtime?.waiting} detail={t('当前排队')} />
          <Stat label={t('可读发布版本')} value={ready?.published_models} detail={ready?.unavailable_models ? t('{0} 个模型归档异常', ready.unavailable_models) : t('就绪探针检查')} />
          <Stat label="HTTP 5xx" value={runtime?.serverErrors} detail={runtime ? t('累计请求 {0}', number.format(runtime.requests)) : t('累计请求 —')} />
        </dl>
        {!!runtime?.auditFailures && <p className="overview-warning">{t('审计写入累计失败 {0} 次', number.format(runtime.auditFailures))}</p>}
      </section>

      <OverviewCharts samples={samples} active={active} />

      <div className="overview-grid">
        <div id="overview-models" tabIndex={-1}>
          {models && <div hidden={!!modelError}><ModelHealth models={models} openModel={openModel} visible={active && !modelError} /></div>}
          {(!models || modelError) && <section className="overview-panel"><h2><Network size={17} aria-hidden="true" />{t('模型健康')}</h2>
            {modelError ? <Unavailable error={modelError} /> : <p className="muted">{t('正在读取模型…')}</p>}
          </section>}
        </div>
        <div className="overview-side">
          <section className="overview-panel" id="overview-telemetry" tabIndex={-1}>
            <div className="overview-section-heading">
              <h2><Terminal size={17} aria-hidden="true" />{t('遥测')}</h2>
              <button className="text-button" type="button" onClick={() => openSettings('telemetry')}>{t('日志与遥测')}<ArrowRight size={14} aria-hidden="true" /></button>
            </div>
            {telemetry ? <TelemetrySummary telemetry={telemetry} />
              : snapshot?.telemetry.error ? <Unavailable error={snapshot.telemetry.error} /> : <p className="muted">{t('正在读取遥测…')}</p>}
          </section>
          <section className="overview-panel" id="overview-backups" tabIndex={-1}>
            <div className="overview-section-heading">
              <h2><DatabaseBackup size={17} aria-hidden="true" />{t('备份')}</h2>
              <button className="text-button" type="button" onClick={() => openSettings('backup')}>{t('备份与恢复')}<ArrowRight size={14} aria-hidden="true" /></button>
            </div>
            {backups ? <dl className="overview-rows">
              {backupJobs.map(([key, name]) => <BackupJob key={key} name={name} job={backups[key]} />)}
              {backups.manual?.state === 'enabled' && <BackupJob name={t('手动控制状态备份')} job={backups.manual.job} />}
            </dl> : snapshot?.backups.error ? <Unavailable error={snapshot.backups.error} /> : <p className="muted">{t('正在读取备份…')}</p>}
          </section>
        </div>
      </div>
    </div>
  </main>;
}
