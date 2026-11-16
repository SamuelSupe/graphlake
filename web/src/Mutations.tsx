import { t } from './locale.ts';
import { useRef, useState } from 'react';
import { api, ApiError, errorText, type SavedModel } from './api';

type Write = { model: string; label: string; path: string; method: string; body: string; expected?: string };
type Pending = Write & { id: string; at: number };
type Receipt = { status: 'committed' | 'uncommitted'; model: string; revision: string; active_revision: string | null };
const prefix = 'graphlake-model-write-v1:';
const ttl = 24 * 60 * 60 * 1000;

export function clearPendingMutations() {
  try {
    for (let index = sessionStorage.length - 1; index >= 0; index--) {
      const key = sessionStorage.key(index);
      if (key?.startsWith(prefix)) sessionStorage.removeItem(key);
    }
  } catch { /* A private browser may disable storage. */ }
}

export function useMutations(actor: string, receipts: boolean, updated: (model?: SavedModel) => void) {
  const key = prefix + actor;
  const [pending, setPending] = useState<Pending | undefined>(() => {
    try { return JSON.parse(sessionStorage.getItem(key) ?? 'null') ?? undefined; }
    catch { return undefined; }
  });
  const current = useRef(pending);
  const running = useRef(false);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState('');
  function remember(value?: Pending) {
    current.current = value;
    setPending(value);
    try {
      if (value) sessionStorage.setItem(key, JSON.stringify(value));
      else sessionStorage.removeItem(key);
    } catch { /* The current page still retains the request. */ }
  }

  async function send(write: Pending) {
    return api<SavedModel>(write.path, {
      method: write.method,
      headers: { 'Idempotency-Key': write.id, ...(write.expected ? { 'If-Match': write.expected } : {}) },
      body: write.body,
    });
  }

  async function run(write: Write): Promise<SavedModel> {
    if (running.current || current.current) throw new Error(t('请先确认上一项模型操作的结果'));
    const request = { ...write, id: crypto.randomUUID(), at: Date.now() };
    setNotice('');
    running.current = true;
    if (receipts) remember(request);
    try {
      const result = await send(request);
      remember();
      return result;
    } catch (cause) {
      if (receipts && (!(cause instanceof ApiError) || cause.status === undefined || cause.status >= 500 || cause.status === 401)) {
        setNotice(t('{0}的结果待确认。请查询回执或用原请求重试。', t(write.label)));
      } else {
        remember();
      }
      throw cause;
    } finally { running.current = false; }
  }

  async function resolve(retry: boolean) {
    const write = current.current;
    if (!write || running.current) return;
    running.current = true;
    setBusy(true);
    try {
      const result = await api<Receipt>('/mutations/' + write.id).catch(cause => {
        if (cause instanceof ApiError && cause.response?.status === 'uncommitted') return undefined;
        throw cause;
      });
      if (result?.status === 'committed') {
        remember();
        setNotice(t('{0}已提交 · 操作版本 {1}{2}', t(write.label), result.revision.slice(0, 12), (result.active_revision ? t(' · 当时生效 {0}', result.active_revision.slice(0, 12)) : t(' · 当时无生效版本'))));
        // Show the latest model even when a later write has already advanced it.
        updated(await api<SavedModel>('/models/' + write.model).catch(() => undefined));
      } else if (retry && Date.now() - write.at < ttl) {
        const model = await send(write);
        remember();
        updated(model);
        setNotice(t('{0}已提交', t(write.label)));
      } else {
        setNotice(Date.now() - write.at >= ttl
          ? t('回执保留期已过。请查看模型版本历史，确认后清除本地记录。')
          : t('还未观察到提交。可以使用原请求重试。'));
      }
    } catch (cause) { setNotice(errorText(cause)); }
    finally { setBusy(false); running.current = false; }
  }

  const banner = pending && (!running.current || busy) ? <div className="notice mutation-notice">
    <p role="status">{notice || t('{0}的结果待确认', t(pending.label))}</p>
    <code>{pending.id}</code>
    <div className="validation-actions">
      <button className="button compact" disabled={busy} onClick={() => void resolve(false)}>{t('查询回执')}</button>
      <button className="button compact" disabled={busy || Date.now() - pending.at >= ttl} onClick={() => void resolve(true)}>{t('重试原请求')}</button>
      <button className="text-button" disabled={busy} onClick={() => {
        if (window.confirm(t('请先核对模型及版本历史。清除后将无法从此页面重试原请求。'))) {
          remember(); setNotice('');
        }
      }}>{t('清除本地记录')}</button>
    </div>
  </div> : pending ? <p role="status" className="notice mutation-notice">{t('正在处理 {0}…', t(pending.label))}</p> : notice ? <p role="status" className="notice mutation-notice">{notice} <button className="text-button" onClick={() => setNotice('')}>{t('关闭')}</button></p> : undefined;
  return { run, banner, pending: !!pending };
}
