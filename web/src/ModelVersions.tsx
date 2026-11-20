import { diagnosticText, locale, t } from './locale.ts';
import { useEffect, useId, useState, type FormEvent } from 'react';
import { api, ApiError, downloadText, errorText, type ModelReadiness, type VersionHistory } from './api';
import { Modal } from './ui';
import { VersionDiff } from './VersionDiff';
import { versionsPerPage } from './modelChanges';
import { useVersionPage } from './useVersionPage';

export function Versions({ id, yaml, busy, close, rollback, offline, archiveError, restore }: {
  id: string; yaml: string; busy: boolean; close: () => void; rollback?: (revision: string) => void; offline?: () => void;
  archiveError?: string; restore?: (revision: string, yaml: string) => Promise<void>;
}) {
  const [history, setHistory] = useState<VersionHistory>();
  const [error, setError] = useState('');
  const [index, setIndex] = useState(0);
  const [compareDraft, setCompareDraft] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const { rows, selected, start } = useVersionPage(id, history, index);
  const [reading, setReading] = useState(false);
  const uploadId = useId();
  const [probing, setProbing] = useState(false);
  const [readiness, setReadiness] = useState<ModelReadiness>();
  async function probe() {
    setProbing(true);
    setError('');
    setReadiness(undefined);
    try {
      setReadiness(await api<ModelReadiness>('/models/' + id + '/readiness'));
    } catch (cause) {
      if (cause instanceof ApiError && cause.readiness) setReadiness(cause.readiness);
      else setError(errorText(cause));
    } finally { setProbing(false); }
  }
  useEffect(() => {
    const controller = new AbortController();
    api<VersionHistory>('/models/' + id + '/history', { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) setHistory(value);
    }, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [id, attempt]);
  async function upload(event: FormEvent<HTMLFormElement>, revision: string) {
    event.preventDefault();
    const file = new FormData(event.currentTarget).get('backup');
    if (!(file instanceof File)) return;
    setReading(true);
    setError('');
    try {
      if (file.size > 1024 * 1024) throw new Error(t('YAML 文件超过 1 MiB'));
      const backup = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(await file.arrayBuffer());
      await restore?.(revision, backup);
    } catch (cause) {
      setError(errorText(cause));
    } finally { setReading(false); }
  }
  const archived = selected?.after && 'yaml' in selected.after ? selected.after.yaml : undefined;
  const base = selected?.before && 'yaml' in selected.before ? selected.before.yaml : undefined;
  const failure = selected?.after && 'error' in selected.after ? selected.after
    : !compareDraft && selected?.before && 'error' in selected.before ? selected.before : undefined;
  const repairRevision = failure?.repair ?? (archiveError ? history?.active_revision ?? undefined : undefined);
  const retry = () => { setError(''); setAttempt(value => value + 1); };
  const changeLabel = { added: t('新增'), changed: t('修改'), removed: t('删除') };
  return <Modal title={t('模型版本历史')} size="wide" close={close}>
    <div className="panel-layout">
      <div className="panel-body versions-body">
        {archiveError && <p className="notice error">{diagnosticText(archiveError)}</p>}
        {error && <p className="inline-error" role="alert">{error} <button className="text-button" onClick={retry}>{t('重试')}</button></p>}
        {restore && repairRevision && <form className="dialog-form version-repair" onSubmit={event => void upload(event, repairRevision)}>
          <label htmlFor={uploadId}>{t('选择版本 {0} 的原始备份 YAML', repairRevision.slice(0, 14))}</label>
          <input id={uploadId} type="file" name="backup" accept=".yaml,.yml" required onChange={() => setError('')} />
          <button type="submit" className="button compact" disabled={reading || busy}>{t('恢复归档')}</button>
        </form>}
        <div className="versions-layout">
          <section className="versions-index" aria-label={t('发布记录')}>
            <div className="panel-section-heading"><h3>{t('发布记录')}</h3>
              <p className="muted small">{history ? t('{0} 条记录', history.versions.length) : t('正在读取版本…')}</p></div>
            {!!history?.offline_at_ms && <p className="muted small">{t('已下线 · {0}', new Date(history.offline_at_ms).toLocaleString(locale))}</p>}
            {!history ? !error && <p role="status">{t('正在加载版本…')}</p>
              : !history.versions.length ? <p className="muted">{t('尚未发布任何版本。')}</p> : <ol className="versions-choices">
              {rows.map(({entry, changes, index: i}) => <li key={i}>
                <button className="version-choice" aria-pressed={index === i} onClick={() => setIndex(i)}>
                  <strong>{{ adopt: t('既有配置'), publish: t('发布'), rollback: t('回退') }[entry.action]} · <code title={entry.revision}>{entry.revision.slice(0, 14)}</code></strong>
                  {changes && changes.length > 0 && <span className="version-operation">{changeLabel[changes[0].kind]} · {changes[0].section} {changes[0].name}
                    {changes.length > 1 && t(' 等 {0} 项变更', changes.length)}</span>}
                  {entry.at_ms > 0 && <small>{new Date(entry.at_ms).toLocaleString(locale)}</small>}
                  <span className="version-choice-action">{t('服务端已确认')}{i === 0 && history.active_revision === entry.revision && <span className="tag">{t('生效中')}</span>}</span>
                </button>
                {history.active_revision !== entry.revision && rollback && <button className="text-button" aria-label={t('回退至 {0}', entry.revision.slice(0, 14))} disabled={busy}
                  onClick={() => rollback(entry.revision)}>{t('预览回退')}</button>}
              </li>)}
            </ol>}
            {history && history.versions.length > versionsPerPage && <div className="actions">
              <button className="button compact" disabled={!start} onClick={() => setIndex(start - versionsPerPage)}>{t('上一页')}</button>
              <small>{Math.floor(start / versionsPerPage) + 1} / {Math.ceil(history.versions.length / versionsPerPage)}</small>
              <button className="button compact" disabled={start + versionsPerPage >= history.versions.length} onClick={() => setIndex(start + versionsPerPage)}>{t('下一页')}</button>
            </div>}
            {rollback && <p className="muted small">{t('回退前可预览固定场景，确认时会重新校验。工作草稿会保留。')}</p>}
            {offline && <details className="version-management"><summary>{t('生效版本操作')}</summary>
              <div className="form-fields">
                <button className="button compact" disabled={probing || busy} onClick={() => void probe()}>{t('检查生效版本数据源')}</button>
                {probing && <p role="status">{t('正在检查数据源…')}</p>}
                {readiness && <div role="status">
                  {readiness.status === 'mapping_invalid' && <p>{t('映射结构不兼容{0}', readiness.location?.kind === 'field' ? ' · ' + readiness.location.label + '.' + readiness.location.field : '')}</p>}
                  {readiness.status === 'worker_unavailable' && <p>{t('部分或全部 Worker 无法执行此模型')}</p>}
                  <ul aria-label={t('数据源检查结果')}>
                    {Object.entries(readiness.sources).map(([name, available]) => <li key={name}><code>{name}</code> · {available ? t('可用') : t('不可用')}</li>)}
                    {readiness.workers?.map(({ worker, probe }) => <li key={worker}>Worker {worker} · {
                      !probe ? t('无法完成检查') : probe.status === 'ready' ? t('可用') : probe.status === 'mapping_invalid'
                        ? t('映射结构不兼容') : t('{0} 不可用', Object.entries(probe.sources).filter(([, ok]) => !ok).map(([name]) => name).join(t('、')))
                    }</li>)}
                  </ul>
                </div>}
                <button className="button compact danger" disabled={busy} onClick={offline}>{t('下线模型')}</button>
              </div>
            </details>}
          </section>
          <section className="version-preview" aria-label={t('版本差异详情')}>
            {selected && <>
              <div className="actions version-modes">
                <button className="button compact" aria-pressed={!compareDraft} onClick={() => setCompareDraft(false)}>{t('本次变更')}</button>
                <button className="button compact" aria-pressed={compareDraft} onClick={() => setCompareDraft(true)}>{t('对照当前草稿')}</button>
              </div>
              <p className="muted small version-baseline">{compareDraft ? selected.entry.revision.slice(0, 14) : selected.previous?.revision.slice(0, 14) ?? t('空模型')} → {compareDraft ? t('当前草稿') : selected.entry.revision.slice(0, 14)}</p>
              {failure ? <p className="notice error" role="alert">{failure.error} <button className="text-button" onClick={retry}>{t('重试')}</button></p>
                : archived !== undefined && (compareDraft || base !== undefined)
                  ? <VersionDiff key={index + ':' + compareDraft} before={compareDraft ? archived : base!} after={compareDraft ? yaml : archived} release={!compareDraft} />
                  : <p role="status">{t('正在读取版本…')}</p>}
            </>}
          </section>
        </div>
      </div>
      <footer className="panel-footer">
        {selected && archived !== undefined && <button className="button" onClick={() => downloadText(archived, id + '-' + selected.entry.revision.slice(0, 14) + '.yaml')}>{t('下载历史 YAML')}</button>}
        <button className="button primary" onClick={close}>{t('完成')}</button>
      </footer>
    </div>
  </Modal>;
}
