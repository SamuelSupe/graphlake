import { useEffect, useId, useState } from 'react';
import { DatabaseBackup, RefreshCw } from 'lucide-react';
import { errorText } from './api';
import { recovery, type Phase, type RestoreJob } from './recoveryApi';
import { Modal } from './ui';

type Candidate = { id: string; modified_ms: number | null };
const phases: [Phase, string][] = [
  ['validate', '校验备份'],
  ['parquet', '恢复 Parquet'],
  ['postgres', '恢复 PostgreSQL'],
  ['control', '导入控制状态'],
];

export default function RecoveryImport({ token, job, start }: {
  token: string; job: RestoreJob | null; start: (bundle: string) => Promise<void>;
}) {
  const [candidates, setCandidates] = useState<Candidate[]>();
  const [selected, setSelected] = useState(job?.bundle ?? '');
  const [refresh, setRefresh] = useState(0);
  const [listError, setListError] = useState('');
  const [startError, setStartError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const progressLabel = useId();
  const active = phases.findIndex(([phase]) => phase === job?.phase);
  const bundle = job?.bound ? job.bundle : selected;
  const modified = candidates?.find(backup => backup.id === bundle)?.modified_ms;

  useEffect(() => {
    const controller = new AbortController();
    recovery<Candidate[]>('backups', token, { signal: controller.signal }).then(list => {
      if (controller.signal.aborted) return;
      setCandidates(list);
      setSelected(current => list.some(backup => backup.id === current) ? current : list[0]?.id ?? '');
    }, cause => {
      if (!controller.signal.aborted) setListError(errorText(cause));
    });
    return () => controller.abort();
  }, [token, refresh]);

  async function begin() {
    setConfirm(false);
    setSubmitting(true);
    setStartError('');
    try { await start(bundle); }
    catch (error) { setStartError(errorText(error)); }
    finally { setSubmitting(false); }
  }

  return <>
    <section className="settings-card">
      <div className="settings-section-title"><h2>异地备份</h2>
        <button type="button" className="button compact" onClick={() => { setListError(''); setRefresh(value => value + 1); }} disabled={job?.running || submitting}>
          <RefreshCw size={15} aria-hidden="true" />刷新备份列表
        </button>
      </div>
      {listError && <p role="alert" className="notice error">{listError}</p>}
      {(!candidates && !listError) && <p role="status">正在读取异地备份…</p>}
      {candidates && !candidates.length && !job?.bound && <p className="muted">目标站的异地备份目录中暂无检查点。</p>}
      {(!!candidates?.length || job?.bound) && <div className="recovery-select">
        <label>恢复检查点<select value={bundle} onChange={event => setSelected(event.target.value)} disabled={submitting || job?.running || job?.bound}>
          {bundle && !candidates?.some(backup => backup.id === bundle) && <option value={bundle}>{bundle}</option>}
          {candidates?.map(backup => <option key={backup.id} value={backup.id}>{backup.id}</option>)}
        </select></label>
        <button type="button" className="button primary" disabled={!bundle || submitting || job?.running} onClick={() => setConfirm(true)}>
          <DatabaseBackup size={16} aria-hidden="true" />{submitting ? '正在提交…' : job?.failure ? '重试恢复' : '开始恢复'}
        </button>
      </div>}
      {modified != null && <p className="settings-hint">副本更新时间 {new Date(modified).toLocaleString()}</p>}
      {startError && <p role="alert" className="notice error">{startError}</p>}
    </section>
    {job && <section className="settings-card">
      <div className="settings-section-title"><h2>恢复进度</h2><span className="mono">{job.bundle}</span></div>
      <p id={progressLabel} role="status">{job.running ? '正在' + phases[active]?.[1] : phases[active]?.[1] + '失败'}</p>
      <progress className="recovery-progress" aria-labelledby={progressLabel} max={phases.length} value={Math.max(active, 0)} />
      <ol className="recovery-steps">
        {phases.map(([phase, label], index) => <li key={phase} aria-current={index === active ? 'step' : undefined}>
          <span>{label}</span><span className="muted">{index < active ? '已完成' : index === active ? job.running ? '进行中' : '失败' : '等待'}</span>
        </li>)}
      </ol>
      {job.failure && <p className="notice error" role="alert">{job.failure}</p>}
    </section>}
    {confirm && <Modal title="开始灾备恢复" close={() => setConfirm(false)}><div className="dialog-form">
      <p>将从 <b>{bundle}</b> 恢复数据和控制状态。请确认目标集群和 PostgreSQL 数据库专用于本次恢复。</p>
      <div className="dialog-actions">
        <button type="button" className="button" data-initial-focus onClick={() => setConfirm(false)}>取消</button>
        <button type="button" className="button primary" onClick={() => void begin()}>确认恢复</button>
      </div>
    </div></Modal>}
  </>;
}
