import { locale, t } from './locale.ts';
import { useEffect, useState } from 'react';
import { DatabaseBackup, RefreshCw, RotateCcw } from 'lucide-react';
import { api, errorText } from './api';
import RecoveryTarget from './RecoveryTarget';

type Job = { state: 'not_recorded' | 'unavailable' } | { state: 'recorded'; result: {
  ok: boolean; attempted_ms: number; last_success_ms: number | null; checkpoint_ms: number | null; artifact?: string;
} };
type Manual = { state: 'unsupported' | 'not_configured' } | { state: 'enabled'; running: boolean; job: Job };
type Exported = { file: string; status_recorded: boolean; local_directory?: string };
export type BackupSnapshot = { export: Job; offline: Job; recovery_configured: boolean; manual?: Manual };

function JobFacts({ job }: { job: Job }) {
  return job.state === 'recorded' ? <>
    <dl className="recovery-facts">
      <div><dt>{t('最近尝试')}</dt><dd>{time(job.result.attempted_ms)}</dd></div>
      {!job.result.ok && <div><dt>{t('最近成功')}</dt><dd>{time(job.result.last_success_ms)}</dd></div>}
      <div><dt>{t('检查点时间')}</dt><dd>{time(job.result.checkpoint_ms)}</dd></div>
    </dl>
    {job.result.artifact && <p className="settings-hint">{t('服务器文件 ')}<code>{job.result.artifact}</code></p>}
  </> : <p className="muted">{job.state === 'not_recorded' ? t('暂无任务记录') : t('任务状态暂不可读')}</p>;
}

function JobSummary({ title, job }: { title: string; job: Job }) {
  return <section className="settings-card">
    <div className="settings-section-title"><h2>{title}</h2>
      {job.state === 'recorded' && <span className={'status ' + (job.result.ok ? 'passed' : 'failed')}>{job.result.ok ? t('成功') : t('失败')}</span>}
    </div>
    <JobFacts job={job} />
  </section>;
}

const time = (ms: number | null) => ms === null ? '—' : new Date(ms).toLocaleString(locale);

export default function BackupStatus({ visible }: { visible: boolean }) {
  const [data, setData] = useState<BackupSnapshot>();
  const [error, setError] = useState('');
  const [revision, refresh] = useState(0);
  const [loading, setLoading] = useState(true);
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState('');
  const [exported, setExported] = useState<Exported>();
  const [recoveryOpen, setRecoveryOpen] = useState(false);
  const manual = data?.manual;
  const running = manual?.state === 'enabled' && manual.running;
  useEffect(() => {
    if (!visible) return;
    const controller = new AbortController();
    setLoading(true);
    setError('');
    api<BackupSnapshot>('/backups/status', { signal: controller.signal }).then(setData, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [visible, revision]);

  useEffect(() => {
    if (!visible || !running) return;
    const timer = window.setInterval(() => refresh(value => value + 1), 3000);
    return () => window.clearInterval(timer);
  }, [visible, running]);

  async function startBackup() {
    if (!visible || exporting || manual?.state !== 'enabled' || running) return;
    setExporting(true);
    setExportError('');
    setExported(undefined);
    try {
      setExported(await api<Exported>('/backups/export', { method: 'POST' }));
    } catch (cause) {
      setExportError(errorText(cause));
    } finally {
      setExporting(false);
      refresh(value => value + 1);
    }
  }

  return <div className="settings-workspace backup-workspace">
    <div className="settings-section-title"><h2>{t('备份任务')}</h2>
      <button type="button" className="button compact" disabled={loading} onClick={() => refresh(value => value + 1)}>
        <RefreshCw size={15} aria-hidden="true" />{t('刷新备份状态')}
      </button>
    </div>
    {loading && !data && <p role="status">{t('正在读取备份状态…')}</p>}
    {error && <p role="alert" className="notice error">{error}</p>}
    {data && !error && <section className="settings-card">
      <div className="settings-section-title"><h2>{t('手动备份')}</h2>
        {manual?.state === 'enabled' && <button type="button" className="button primary compact"
          aria-disabled={exporting || running || loading || !visible}
          onClick={() => { if (!loading) void startBackup(); }}>
          <DatabaseBackup size={16} aria-hidden="true" />{t('立即备份')}
        </button>}
      </div>
      {manual?.state === 'enabled' ? <>
        <p className={exporting || running || exported ? 'settings-hint' : 'sr-only'} role="status">
          {exporting || running ? t('正在校验并写入备份…') : exported
            ? exported.status_recorded ? t('备份已保存。') : t('备份已保存，但任务状态未能写入。') : ''}
        </p>
        {exported && !(manual.job.state === 'recorded' && manual.job.result.artifact === exported.file) &&
          <p className="settings-hint">{t('本次文件 ')}<code>{exported.file}</code></p>}
        {exported?.local_directory && <p className="settings-hint">{t('本地表目录 ')}<code>{exported.local_directory}</code></p>}
        {exportError && <p className="notice error" role="alert">{exportError}</p>}
        {manual.job.state === 'recorded' && <span className={'status ' + (manual.job.result.ok ? 'passed' : 'failed')}>
          {manual.job.result.ok ? t('最近任务成功') : t('最近任务失败')}
        </span>}
        <JobFacts job={manual.job} />
      </> : <p className="muted">{manual?.state === 'not_configured'
        ? <>{t('在 Coordinator 上配置私有备份目录 ')}<code>GRAPHLAKE_MANUAL_BACKUP_DIR</code>{t(' 后可用。')}</>
        : t('此操作适用于启用故障接管的 Coordinator。当前部署可按部署手册备份模型目录。')}</p>}
    </section>}
    <section className="settings-card">
      <div className="settings-section-title"><h2>{t('灾备恢复')}</h2>
        <button type="button" className="button compact" onClick={() => setRecoveryOpen(true)}>
          <RotateCcw size={15} aria-hidden="true" />{t('恢复备份')}
        </button>
      </div>
      <p className="settings-hint">{t('在已配置的目标站选择恢复点并完成验收。')}</p>
    </section>
    {data && !error && <>
      <JobSummary title={t('定时备份')} job={data.export} />
      <JobSummary title={t('异地副本校验')} job={data.offline} />
    </>}
    {visible && recoveryOpen && <RecoveryTarget local={!!data?.recovery_configured} close={() => setRecoveryOpen(false)} />}
  </div>;
}
