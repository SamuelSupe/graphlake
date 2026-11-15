import { useEffect, useState } from 'react';
import { ArrowUpRight, RefreshCw } from 'lucide-react';
import { api, errorText } from './api';

type Job = { state: 'not_recorded' | 'unavailable' } | { state: 'recorded'; result: {
  ok: boolean; attempted_ms: number; last_success_ms: number | null; checkpoint_ms: number | null;
} };
type Overview = { export: Job; offline: Job; recovery_configured: boolean };

function JobSummary({ title, job }: { title: string; job: Job }) {
  return <section className="settings-card">
    <div className="settings-section-title"><h2>{title}</h2>
      {job.state === 'recorded' && <span className={'status ' + (job.result.ok ? 'passed' : 'failed')}>{job.result.ok ? '成功' : '失败'}</span>}
    </div>
    {job.state === 'recorded' ? <dl className="recovery-facts">
      <div><dt>最近尝试</dt><dd>{time(job.result.attempted_ms)}</dd></div>
      {!job.result.ok && <div><dt>最近成功</dt><dd>{time(job.result.last_success_ms)}</dd></div>}
      <div><dt>检查点时间</dt><dd>{time(job.result.checkpoint_ms)}</dd></div>
    </dl> : <p className="muted">{job.state === 'not_recorded' ? '暂无任务记录' : '任务状态暂不可读'}</p>}
  </section>;
}

const time = (ms: number | null) => ms === null ? '—' : new Date(ms).toLocaleString();

export default function BackupStatus({ visible }: { visible: boolean }) {
  const [data, setData] = useState<Overview>();
  const [error, setError] = useState('');
  const [revision, refresh] = useState(0);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    if (!visible) return;
    const controller = new AbortController();
    setLoading(true);
    setError('');
    api<Overview>('/backups/status', { signal: controller.signal }).then(setData, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [visible, revision]);

  return <div className="settings-stack settings-workspace">
    <div className="settings-section-title"><h2>备份任务</h2>
      <button type="button" className="button compact" disabled={loading} onClick={() => refresh(value => value + 1)}>
        <RefreshCw size={15} aria-hidden="true" />刷新备份状态
      </button>
    </div>
    {loading && !data && <p role="status">正在读取备份状态…</p>}
    {error && <p role="alert" className="notice error">{error}</p>}
    {data && !error && <div className="settings-grid">
      <JobSummary title="控制状态备份" job={data.export} />
      <JobSummary title="异地副本校验" job={data.offline} />
    </div>}
    <section className="settings-card">
      <h2>灾备恢复</h2>
      <p className="settings-hint">从目标站 HTTPS 地址的 <code>/recovery</code> 选择异地备份，查看恢复进度并完成验收。</p>
      {data?.recovery_configured && <a href="/recovery" className="button compact">打开本节点恢复验收台<ArrowUpRight size={15} aria-hidden="true" /></a>}
    </section>
  </div>;
}
