import { useEffect, useId, useState, type FormEvent } from 'react';
import { api, ApiError, downloadText, errorText, type ModelReadiness, type VersionHistory } from './api';
import { Modal } from './ui';
import { VersionDiff } from './VersionDiff';

export function Versions({ id, yaml, busy, close, rollback, offline, archiveError, restore }: {
  id: string; yaml: string; busy: boolean; close: () => void; rollback?: (revision: string) => void; offline?: () => void;
  archiveError?: string; restore?: (revision: string, yaml: string) => Promise<void>;
}) {
  const [history, setHistory] = useState<VersionHistory>();
  const [error, setError] = useState('');
  const [comparison, setComparison] = useState<{ revision: string; yaml: string; index: number }>();
  const [reading, setReading] = useState(false);
  const [repair, setRepair] = useState<string>();
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
  async function compare(revision: string, index: number) {
    setReading(true); setError('');
    try {
      const archived = await api<{ yaml: string }>('/models/' + id + '/history/' + encodeURIComponent(revision));
      setComparison({ revision, yaml: archived.yaml, index });
    } catch (cause) {
      if (cause instanceof ApiError && cause.code === 'model_archive_unavailable') setRepair(revision);
      setError(errorText(cause));
    }
    finally { setReading(false); }
  }
  useEffect(() => {
    const controller = new AbortController();
    api<VersionHistory>('/models/' + id + '/history', { signal: controller.signal }).then(setHistory, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [id]);
  async function upload(event: FormEvent<HTMLFormElement>, revision: string) {
    event.preventDefault();
    const file = new FormData(event.currentTarget).get('backup');
    if (!(file instanceof File)) return;
    setReading(true);
    setError('');
    try {
      if (file.size > 1024 * 1024) throw new Error('YAML 文件超过 1 MiB');
      const backup = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(await file.arrayBuffer());
      await restore?.(revision, backup);
    } catch (cause) {
      setError(errorText(cause));
    } finally { setReading(false); }
  }
  const repairRevision = repair ?? (archiveError ? history?.active_revision ?? undefined : undefined);
  return <Modal title="模型版本历史" size="wide" description="选择已发布版本，查看它与当前草稿的变化。" close={close}>
    <div className="panel-layout">
      <div className="panel-body versions-body">
        {archiveError && <p className="notice error">{archiveError}</p>}
        {error && <p className="inline-error" role="alert">{error}</p>}
        {restore && repairRevision && <form className="dialog-form version-repair" onSubmit={event => void upload(event, repairRevision)}>
          <label htmlFor={uploadId}>选择版本 {repairRevision.slice(0, 14)} 的原始备份 YAML</label>
          <input id={uploadId} type="file" name="backup" accept=".yaml,.yml" required onChange={() => setError('')} />
          <button type="submit" className="button compact" disabled={reading || busy}>恢复归档</button>
        </form>}
        <div className="versions-layout">
          <section className="versions-index" aria-label="发布记录">
            <div className="panel-section-heading"><h3>发布记录</h3>
              <p className="muted small">{history ? history.versions.length + ' 条记录' : '正在读取版本…'}</p></div>
            {!!history?.offline_at_ms && <p className="muted small">已下线 · {new Date(history.offline_at_ms).toLocaleString()}</p>}
            {!history ? !error && <p role="status">正在加载版本…</p>
              : !history.versions.length ? <p className="muted">尚未发布任何版本。</p> : <ol className="versions-choices">
              {history.versions.map((entry, i) => <li key={i}>
                <button className="version-choice" aria-disabled={reading} aria-pressed={comparison?.index === i}
                  onClick={() => { if (!reading) void compare(entry.revision, i); }}>
                  <code title={entry.revision}>{entry.revision.slice(0, 14)}</code>
                  <small>{{ adopt: '既有配置', publish: '发布', rollback: '回退' }[entry.action]}
                    {entry.at_ms > 0 && ' · ' + new Date(entry.at_ms).toLocaleString()}</small>
                  <span className="version-choice-action">查看并对照{history.active_revision === entry.revision && history.versions.findIndex(version => version.revision === entry.revision) === i && <span className="tag">生效中</span>}</span>
                </button>
                {history.active_revision !== entry.revision && rollback && <button className="text-button" aria-label={'回退至 ' + entry.revision.slice(0, 14)} disabled={busy}
                  onClick={() => rollback(entry.revision)}>回退至此版本</button>}
              </li>)}
            </ol>}
            {rollback && <p className="muted small">回退前会校验当前来源；工作草稿会保留。</p>}
            {offline && <details className="version-management"><summary>生效版本操作</summary>
              <div className="form-fields">
                <button className="button compact" disabled={probing || busy} onClick={() => void probe()}>检查生效版本数据源</button>
                {probing && <p role="status">正在检查数据源…</p>}
                {readiness && <div role="status">
                  {readiness.status === 'mapping_invalid' && <p>映射结构不兼容{readiness.location?.kind === 'field' ? ' · ' + readiness.location.label + '.' + readiness.location.field : ''}</p>}
                  {readiness.status === 'worker_unavailable' && <p>部分或全部 Worker 无法执行此模型</p>}
                  <ul aria-label="数据源检查结果">
                    {Object.entries(readiness.sources).map(([name, available]) => <li key={name}><code>{name}</code> · {available ? '可用' : '不可用'}</li>)}
                    {readiness.workers?.map(({ worker, probe }) => <li key={worker}>Worker {worker} · {
                      !probe ? '无法完成检查' : probe.status === 'ready' ? '可用' : probe.status === 'mapping_invalid'
                        ? '映射结构不兼容' : Object.entries(probe.sources).filter(([, ok]) => !ok).map(([name]) => name).join('、') + ' 不可用'
                    }</li>)}
                  </ul>
                </div>}
                <button className="button compact danger" disabled={busy} onClick={offline}>下线模型</button>
              </div>
            </details>}
          </section>
          <section className="version-preview" aria-label="版本差异详情" aria-busy={reading}>
            {reading && <p className="muted small" role="status">正在读取版本…</p>}
            {comparison ? <VersionDiff before={comparison.yaml} after={yaml} />
              : <div className="panel-placeholder"><h3>查看版本变化</h3><p>选择一条发布记录，查看变更摘要和逐行差异。</p></div>}
          </section>
        </div>
      </div>
      <footer className="panel-footer">
        <span className="panel-footer-context">{comparison ? '对照版本 ' + comparison.revision.slice(0, 14) : '尚未选择对照版本'}</span>
        {comparison && <button className="button" onClick={() => downloadText(comparison.yaml, id + '-' + comparison.revision.slice(0, 14) + '.yaml')}>下载历史 YAML</button>}
        <button className="button primary" onClick={close}>完成</button>
      </footer>
    </div>
  </Modal>;
}
