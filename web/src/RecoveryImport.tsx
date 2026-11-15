import { useEffect, useId, useState } from 'react';
import { DatabaseBackup, RefreshCw } from 'lucide-react';
import { errorText } from './api';
import { recovery, type Phase, type RecoveryPointCheck, type RestoreJob } from './recoveryApi';
import { Modal } from './ui';

type Candidate = { id: string; modified_ms: number | null };
const phases: [Phase, string][] = [
  ['validate', '校验备份'],
  ['parquet', '恢复 Parquet'],
  ['postgres', '恢复 PostgreSQL'],
  ['control', '导入控制状态'],
];

export default function RecoveryImport({ token, job, start }: {
  token: string; job: RestoreJob | null; start: (point: RecoveryPointCheck) => Promise<void>;
}) {
  const [candidates, setCandidates] = useState<Candidate[]>();
  const [selected, setSelected] = useState(job?.bundle ?? '');
  const [refresh, setRefresh] = useState(0);
  const [listError, setListError] = useState('');
  const [startError, setStartError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [checking, setChecking] = useState(false);
  const [checked, setChecked] = useState<RecoveryPointCheck>();
  const [confirm, setConfirm] = useState<RecoveryPointCheck>();
  const progressLabel = useId();
  const active = phases.findIndex(([phase]) => phase === job?.phase);
  const bundle = job?.bound ? job.bundle : selected;
  const verified = checked?.bundle === bundle ? checked : undefined;
  const busy = checking || submitting || job?.running;
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

  async function check() {
    setChecked(undefined);
    setStartError('');
    setChecking(true);
    try {
      setChecked(await recovery<RecoveryPointCheck>('check', token, { body: JSON.stringify({ bundle }) }));
    } catch (cause) { setStartError(errorText(cause)); }
    finally { setChecking(false); }
  }

  async function begin(point: RecoveryPointCheck) {
    setConfirm(undefined);
    setSubmitting(true);
    setStartError('');
    try { await start(point); setChecked(undefined); }
    catch (error) { setStartError(errorText(error)); }
    finally { setSubmitting(false); }
  }

  return <>
    <section className="settings-card">
      <div className="settings-section-title"><h2>异地备份</h2>
        <button type="button" className="button compact" onClick={() => { setListError(''); setChecked(undefined); setRefresh(value => value + 1); }} disabled={busy}>
          <RefreshCw size={15} aria-hidden="true" />刷新备份列表
        </button>
      </div>
      {listError && <p role="alert" className="notice error">{listError}</p>}
      {(!candidates && !listError) && <p role="status">正在读取异地备份…</p>}
      {candidates && !candidates.length && !job?.bound && <p className="muted">目标站的异地备份目录中暂无检查点。</p>}
      {(!!candidates?.length || job?.bound) && <div className="recovery-select">
        <label>恢复检查点<select value={bundle} onChange={event => {
          setSelected(event.target.value); setChecked(undefined); setStartError('');
        }} disabled={busy || job?.bound}>
          {bundle && !candidates?.some(backup => backup.id === bundle) && <option value={bundle}>{bundle}</option>}
          {candidates?.map(backup => <option key={backup.id} value={backup.id}>{backup.id}</option>)}
        </select></label>
        <button type="button" className={verified ? 'button' : 'button primary'} disabled={!bundle || busy || !!listError} onClick={() => void check()}>
          {checking ? '正在检查…' : verified ? '重新检查' : '检查恢复点'}
        </button>
        {verified && <button type="button" className="button primary" disabled={busy || !!listError} onClick={() => setConfirm(verified)}>
          <DatabaseBackup size={16} aria-hidden="true" />{submitting ? '正在提交…' : job?.failure ? '重试恢复' : '开始恢复'}
        </button>}
      </div>}
      {modified != null && <p className="settings-hint">副本更新时间 {new Date(modified).toLocaleString()}</p>}
      {verified && <p role="status">控制备份与恢复清单匹配 · {verified.postgres_relations} 张 PostgreSQL 表 · {verified.parquet_files} 个 Parquet 文件
        {verified.checkpoint_ms != null && <> · 检查点 {new Date(verified.checkpoint_ms).toLocaleString()}</>}</p>}
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
    {confirm && <Modal title="开始灾备恢复" close={() => setConfirm(undefined)}>
      <div className="panel-layout">
        <div className="panel-body form-fields">
          <p>将从 <b>{confirm.bundle}</b> 恢复数据和控制状态。请确认目标集群和 PostgreSQL 数据库专用于本次恢复。</p>
          <dl className="recovery-facts">
            <div><dt>目标站</dt><dd>{window.location.host}</dd></div>
            <div><dt>恢复点</dt><dd>{confirm.bundle}</dd></div>
          </dl>
        </div>
        <div className="panel-footer">
          <button type="button" className="button" data-initial-focus onClick={() => setConfirm(undefined)}>取消</button>
          <button type="button" className="button primary" onClick={() => void begin(confirm)}>确认恢复</button>
        </div>
      </div>
    </Modal>}
  </>;
}
