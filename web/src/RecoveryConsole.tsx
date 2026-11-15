import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { ArrowLeft, ShieldCheck } from 'lucide-react';
import { errorText } from './api';
import { Mark, Modal } from './ui';
import RecoveryImport from './RecoveryImport';
import { recovery, type Gate, type RecoveryPointCheck, type RecoveryStatus, type RestoreStatus } from './recoveryApi';

type Manifest = { manifest_sha256: string; postgres_relations: number; parquet_files: number; canaries: number };
type Action = 'status' | 'check' | 'verify';

export default function RecoveryConsole() {
  const [token, setToken] = useState('');
  const [gate, setGate] = useState<Gate>();
  const [restore, setRestore] = useState<RestoreStatus>();
  const [manifest, setManifest] = useState<Manifest>();
  const [busy, setBusy] = useState<Action>();
  const [confirm, setConfirm] = useState(false);
  const [error, setError] = useState('');
  const [checkedIngress, setCheckedIngress] = useState<Gate>();
  const canCutOver = Boolean(gate?.ready && checkedIngress === gate);
  const running = !!restore?.job?.running;
  const applyStatus = useCallback((state: RecoveryStatus) => {
    if ('mode' in state) {
      setGate(undefined);
      setRestore(state);
      setManifest(undefined);
    } else {
      setGate(state);
      setRestore(undefined);
      setManifest(previous => previous?.manifest_sha256 === state.manifest_sha256 ? previous : undefined);
    }
  }, []);

  useEffect(() => {
    if (!running) return;
    const controller = new AbortController();
    let timer: number;
    let consecutiveFailures = 0;
    async function poll() {
      try {
        const state = await recovery<RecoveryStatus>('status', token, { signal: controller.signal });
        if (controller.signal.aborted) return;
        consecutiveFailures = 0;
        setError('');
        applyStatus(state);
        if (!('mode' in state) || !state.job?.running) return;
      } catch {
        if (controller.signal.aborted) return;
        // The HTTPS route may briefly be unavailable while the Coordinator loads the restored state.
        if (++consecutiveFailures >= 4) setError('暂时无法读取进度，正在重新连接目标站。');
      }
      timer = window.setTimeout(() => void poll(), 900);
    }
    timer = window.setTimeout(() => void poll(), 350);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [running, token, applyStatus]);

  async function startRestore({ bundle, manifest_sha256 }: RecoveryPointCheck) {
    await recovery<void>('import', token, { body: JSON.stringify({ bundle, manifest_sha256 }) });
    setRestore(previous => previous && {
      mode: 'restore', job: { bundle, phase: 'validate', running: true, bound: previous.job?.bound ?? false, failure: null },
    });
  }

  useEffect(() => {
    if (!gate?.ready) return;
    const controller = new AbortController();
    let retry: number | undefined;
    const stop = () => {
      controller.abort();
      window.clearTimeout(retry);
    };
    const deadline = window.setTimeout(stop, 10_000);
    async function probeIngress() {
      try {
        const response = await fetch('/readyz', { cache: 'no-store', credentials: 'omit', signal: controller.signal });
        if (response.ok && (await response.json()).status === 'ready' && !controller.signal.aborted) {
          window.clearTimeout(deadline);
          setCheckedIngress(gate);
          return;
        }
      } catch { /* The proxy may still be waiting for its first successful check. */ }
      if (!controller.signal.aborted) retry = window.setTimeout(() => void probeIngress(), 500);
    }
    void probeIngress();
    return () => { window.clearTimeout(deadline); stop(); };
  }, [gate]);

  async function perform(action: Action) {
    if (busy) return;
    setBusy(action);
    setError('');
    if (action === 'check') setManifest(undefined);
    try {
      if (action === 'check') {
        const result = await recovery<Manifest>('check', token);
        if (result.manifest_sha256 !== gate?.manifest_sha256) throw new Error('目标恢复清单已变更，请重新读取状态。');
        setManifest(result);
      }
      // Confirm the authoritative gate again after preflight and after approval.
      const state = await recovery<RecoveryStatus>(action === 'verify' ? 'verify' : 'status', token);
      applyStatus(state);
    } catch (cause) {
      setError(errorText(cause));
      try {
        if (action === 'verify' || action === 'check') {
          applyStatus(await recovery<RecoveryStatus>('status', token));
        } else if (!running) {
          setGate(undefined);
          setRestore(undefined);
          setManifest(undefined);
        }
      } catch {
        setGate(undefined);
        setManifest(undefined);
      }
    } finally { setBusy(undefined); }
  }

  const https = window.location.protocol === 'https:';

  function read(event: FormEvent) {
    event.preventDefault();
    void perform('status');
  }

  return <main className="recovery-console">
    <header className="recovery-heading">
      <span className="brand-mark"><Mark size={23} /></span>
      <div><h1>{restore ? '灾备恢复' : '灾备恢复验收'}</h1><p className="muted">{window.location.host}</p></div>
      {canCutOver && <a href="/settings" className="button compact"><ArrowLeft size={15} aria-hidden="true" />返回管理后台</a>}
    </header>
    <section className="settings-card">
      <h2>目标站</h2>
      {https ? <form className="recovery-token-form" onSubmit={read}>
        <label>恢复令牌<input type="password" autoComplete="off" spellCheck={false} required pattern="[0-9a-f]{64}" maxLength={64}
          value={token} disabled={!!busy} onChange={event => {
            setToken(event.target.value);
            setGate(undefined);
            setRestore(undefined);
            setManifest(undefined);
            setError('');
          }} /></label>
        <button className="button" disabled={!!busy} type="submit">{busy === 'status' ? '正在读取…' : '读取状态'}</button>
      </form> : <p className="notice">请从目标站 HTTPS 地址打开恢复页面。</p>}
      {restore && <p className="recovery-verdict" role="status"><strong>暂不可切流</strong><span>完成恢复后进入业务验收。</span></p>}
      {gate && <p className={'recovery-verdict ' + (canCutOver ? 'ready' : '')} role="status">
        <strong>{canCutOver ? '允许切流' : '暂不可切流'}</strong>
        <span>{canCutOver ? '恢复验收已通过，目标站业务入口就绪。'
          : gate.approved ? '验收已通过，等待目标站业务入口就绪。' : '等待恢复验收通过。'}</span>
      </p>}
      {!gate && error && <p className="notice error" role="alert">{error}</p>}
    </section>
    {restore && <RecoveryImport token={token} job={restore.job} start={startRestore} />}
    {gate && <>
      {gate.restored_backup && <p className="notice" role="status">已从 <strong>{gate.restored_backup}</strong> 完成恢复，进入验收。</p>}
      <section className="settings-card">
        <div className="settings-section-title"><h2>恢复清单</h2>
          <button type="button" className="button compact" disabled={!!busy} onClick={() => void perform('check')}>
            {busy === 'check' ? '正在校验…' : '校验恢复清单'}
          </button>
        </div>
        {manifest && <p role="status">清单与备份摘要已匹配 · {manifest.postgres_relations} 张 PostgreSQL 表 · {manifest.parquet_files} 个 Parquet 文件 · {manifest.canaries} 条业务查询</p>}
        <details className="form-disclosure"><summary>清单摘要</summary><code className="recovery-digest">{gate.manifest_sha256}</code></details>
      </section>
      <section className="settings-card">
        <div className="settings-section-title"><h2>业务查询验收</h2>
          {!gate.approved && <button type="button" className="button primary" disabled={!manifest || !!busy} onClick={() => setConfirm(true)}>
            <ShieldCheck size={16} aria-hidden="true" />{busy === 'verify' ? '正在验收…' : '执行恢复验收'}
          </button>}
        </div>
        <p className="settings-hint">{gate.approved ? '业务结果与清单中的预期结果一致。'
          : '验收会复核实际数据和业务查询。通过后开放目标站业务入口。'}</p>
      </section>
      {error && <p className="notice error" role="alert">{error}</p>}
    </>}
    {confirm && <Modal layout="center" title="执行恢复验收" close={() => setConfirm(false)}><div className="dialog-form">
      <p>验收通过后，目标站将允许业务流量。请确认来源数据已冻结，且当前站点是计划切换的目标。</p>
      <div className="dialog-actions">
        <button type="button" className="button" data-initial-focus onClick={() => setConfirm(false)}>取消</button>
        <button type="button" className="button primary" onClick={() => { setConfirm(false); void perform('verify'); }}>开始验收</button>
      </div>
    </div></Modal>}
  </main>;
}
