import { diagnosticText, locale, t } from './locale.ts';
import { useEffect, useId, useState } from 'react';
import { DatabaseBackup, RefreshCw } from 'lucide-react';
import { errorText } from './api';
import { recovery, type Phase, type RecoveryPointCheck, type RestoreJob } from './recoveryApi';
import { Modal } from './ui';

type Candidate = { id: string; modified_ms: number | null };
const phases: [Phase, string][] = [
  ['validate', t('校验备份')],
  ['parquet', t('恢复文件与本地表')],
  ['postgres', t('恢复 PostgreSQL')],
  ['control', t('导入控制状态')],
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
      <div className="settings-section-title"><h2>{t('异地备份')}</h2>
        <button type="button" className="button compact" onClick={() => { setListError(''); setChecked(undefined); setRefresh(value => value + 1); }} disabled={busy}>
          <RefreshCw size={15} aria-hidden="true" />{t('刷新备份列表')}
        </button>
      </div>
      {listError && <p role="alert" className="notice error">{listError}</p>}
      {(!candidates && !listError) && <p role="status">{t('正在读取异地备份…')}</p>}
      {candidates && !candidates.length && !job?.bound && <p className="muted">{t('目标站的异地备份目录中暂无检查点。')}</p>}
      {(!!candidates?.length || job?.bound) && <div className="recovery-select">
        <label>{t('恢复检查点')}<select value={bundle} onChange={event => {
          setSelected(event.target.value); setChecked(undefined); setStartError('');
        }} disabled={busy || job?.bound}>
          {bundle && !candidates?.some(backup => backup.id === bundle) && <option value={bundle}>{bundle}</option>}
          {candidates?.map(backup => <option key={backup.id} value={backup.id}>{backup.id}</option>)}
        </select></label>
        <button type="button" className={verified ? 'button' : 'button primary'} disabled={!bundle || busy || !!listError} onClick={() => void check()}>
          {checking ? t('正在检查…') : verified ? t('重新检查') : t('检查恢复点')}
        </button>
        {verified && <button type="button" className="button primary" disabled={busy || !!listError} onClick={() => setConfirm(verified)}>
          <DatabaseBackup size={16} aria-hidden="true" />{submitting ? t('正在提交…') : job?.failure ? t('重试恢复') : t('开始恢复')}
        </button>}
      </div>}
      {modified != null && <p className="settings-hint">{t('副本更新时间 {0}', new Date(modified).toLocaleString(locale))}</p>}
      {verified && <p role="status">{t('控制备份与恢复清单匹配 · {0} 张 PostgreSQL 表 · {1} 个 Parquet 文件', verified.postgres_relations, verified.parquet_files)}
        {!!verified.local_tables && t(' · {0} 张本地表', verified.local_tables)}
        {verified.checkpoint_ms != null && <>{t(' · 检查点 {0}', new Date(verified.checkpoint_ms).toLocaleString(locale))}</>}</p>}
      {startError && <p role="alert" className="notice error">{startError}</p>}
    </section>
    {job && <section className="settings-card">
      <div className="settings-section-title"><h2>{t('恢复进度')}</h2><span className="mono">{job.bundle}</span></div>
      <p id={progressLabel} role="status">{job.running ? t('正在{0}', phases[active]?.[1]) : t('{0}失败', phases[active]?.[1])}</p>
      <progress className="recovery-progress" aria-labelledby={progressLabel} max={phases.length} value={Math.max(active, 0)} />
      <ol className="recovery-steps">
        {phases.map(([phase, label], index) => <li key={phase} aria-current={index === active ? 'step' : undefined}>
          <span>{label}</span><span className="muted">{index < active ? t('已完成') : index === active ? job.running ? t('进行中') : t('失败') : t('等待')}</span>
        </li>)}
      </ol>
      {job.failure && <p className="notice error" role="alert">{diagnosticText(job.failure)}</p>}
    </section>}
    {confirm && <Modal title={t('开始灾备恢复')} close={() => setConfirm(undefined)}>
      <div className="panel-layout">
        <div className="panel-body form-fields">
          <p>{t('将从 ')}<b>{confirm.bundle}</b>{confirm.postgres_relations ? t(' 恢复数据和控制状态。请确认目标集群和 PostgreSQL 数据库专用于本次恢复。') : t(' 恢复数据和控制状态。请确认目标集群专用于本次恢复。')}</p>
          <dl className="recovery-facts">
            <div><dt>{t('目标站')}</dt><dd>{window.location.host}</dd></div>
            <div><dt>{t('恢复点')}</dt><dd>{confirm.bundle}</dd></div>
          </dl>
        </div>
        <div className="panel-footer">
          <button type="button" className="button" data-initial-focus onClick={() => setConfirm(undefined)}>{t('取消')}</button>
          <button type="button" className="button primary" onClick={() => void begin(confirm)}>{t('确认恢复')}</button>
        </div>
      </div>
    </Modal>}
  </>;
}
