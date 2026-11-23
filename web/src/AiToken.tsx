import { locale, t } from './locale.ts';
import { useEffect, useRef, useState } from 'react';
import { Check, Copy } from 'lucide-react';
import { api, errorText } from './api';

type TokenStatus = { owner: string; active: boolean; revision: string | null; created_at_ms: number | null };

export default function AiToken({ locked, authEnabled, canIssue, token, issued }: {
  locked: boolean; authEnabled: boolean; canIssue: boolean; token: string; issued: (token: string) => void;
}) {
  const [status, setStatus] = useState<TokenStatus>();
  const [show, setShow] = useState(false);
  const [busy, setBusy] = useState(false);
  const [confirm, setConfirm] = useState<'rotate' | 'revoke'>();
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [copied, setCopied] = useState(false);
  const [attempt, reload] = useState(0);
  const mutation = useRef<AbortController | undefined>(undefined);
  const copyTimer = useRef<number | undefined>(undefined);
  const primaryButton = useRef<HTMLButtonElement>(null);
  useEffect(() => () => window.clearTimeout(copyTimer.current), []);
  useEffect(() => {
    if (locked) {
      mutation.current?.abort(); mutation.current = undefined;
      setBusy(false); setShow(false); setConfirm(undefined); setNotice('');
      return;
    }
    if (!authEnabled) return;
    const controller = new AbortController();
    api<TokenStatus>('/mcp/token', { signal: controller.signal }).then(value => { setStatus(value); setError(''); }, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [authEnabled, locked, attempt]);
  useEffect(() => () => { mutation.current?.abort(); mutation.current = undefined; }, []);
  async function change(revoke: boolean) {
    if (!status || locked || busy) return;
    const controller = new AbortController(); mutation.current = controller;
    setBusy(true); setError(''); setNotice('');
    try {
      if (revoke) {
        await api('/mcp/token', { method: 'DELETE', signal: controller.signal, body: JSON.stringify({ revision: status.revision }) });
        if (controller.signal.aborted) return;
        setStatus({ ...status, active: false, revision: null, created_at_ms: null }); issued(''); setNotice(t('令牌已撤销，Agent 已断开访问。'));
      } else {
        const value = await api<TokenStatus & { token: string }>('/mcp/token', { method: 'POST', signal: controller.signal, body: JSON.stringify({ revision: status.revision }) });
        if (controller.signal.aborted) return;
        const { token, ...next } = value;
        setStatus(next); issued(token); setShow(false);
      }
      setConfirm(undefined);
      requestAnimationFrame(() => primaryButton.current?.focus());
    } catch (cause) { if (!controller.signal.aborted) setError(errorText(cause)); }
    finally { if (mutation.current === controller) { mutation.current = undefined; setBusy(false); } }
  }
  async function copy() {
    try {
      await navigator.clipboard.writeText(token);
      setCopied(true);
      setNotice(t('个人 AI 令牌已复制。'));
      window.clearTimeout(copyTimer.current);
      copyTimer.current = window.setTimeout(() => setCopied(false), 2200);
    } catch { setNotice(t('无法访问剪贴板，可显示令牌后手动复制。')); }
  }
  const done = Boolean(status?.active);
  return <section className="mcp-step" aria-labelledby="ai-token-title">
    <div className="mcp-step-title">
      <span className={'mcp-step-number' + (done ? ' is-done' : '')} aria-hidden="true">{done ? <Check size={15} /> : '1'}</span>
      <h3 id="ai-token-title">{t('个人令牌')}</h3>
    </div>
    <div className="mcp-step-body">
    <p className="settings-hint">{t('让 Agent 以你的身份访问 GraphLake，无需重复授权。')}</p>
    {authEnabled && !status && !error && <p className="settings-hint" role="status">{t('正在读取令牌状态…')}</p>}
    {status?.active && !token && <p>{t('已有个人令牌，可继续使用。遗失后可在「管理令牌」中重新生成。')}</p>}
    {!authEnabled ? <p className="notice">{t('启用登录认证后可生成个人 AI 令牌。')}</p>
      : !canIssue && <p className="notice">{t('暂不能生成令牌，请联系管理员检查查询、建模或发布权限。已有令牌仍可撤销。')}</p>}
    {token && !locked && <><div className="mcp-token-value">
      <code>{show ? token : '••••••••••••••••••••••••'}</code>
      <div className="settings-actions"><button ref={primaryButton} type="button" className={'button primary' + (copied ? ' copied' : '')} disabled={locked} onClick={() => void copy()}>{copied ? <Check size={15} aria-hidden="true" /> : <Copy size={15} aria-hidden="true" />}{copied ? t('已复制') : t('复制令牌')}</button><button type="button" className="text-button" disabled={locked} aria-pressed={show} onClick={() => setShow(value => !value)}>{show ? t('隐藏令牌') : t('显示令牌')}</button></div>
    </div><p className="settings-hint">{t('令牌仅在本页可见，请复制到 Agent 的认证设置。')}</p></>}
    {notice && <p className="settings-hint" role="status">{notice}</p>}
    {error && <p className="notice error" role="alert">{error}<button type="button" className="text-button" disabled={locked || busy} onClick={() => { issued(''); setConfirm(undefined); reload(value => value + 1); }}>{t('刷新令牌状态')}</button></p>}
    {status?.active ? <details className="mcp-details"><summary>{t('管理令牌')}</summary><div className="settings-stack">
      {status.created_at_ms && <p className="settings-hint">{t('生成于 {0}', new Date(status.created_at_ms).toLocaleString(locale))}</p>}
      {confirm && <p role="status">{confirm === 'revoke' ? t('撤销后，使用当前令牌的 Agent 会失去访问权限。') : t('重新生成后，旧令牌立即失效，需要更新 Agent 的配置。')}</p>}
      <div className="settings-actions">{confirm ? <><button type="button" className="button danger" disabled={locked || busy} onClick={() => void change(confirm === 'revoke')}>{busy ? t('正在处理…') : confirm === 'revoke' ? t('确认撤销') : t('确认重新生成')}</button><button type="button" className="button" disabled={busy} onClick={() => setConfirm(undefined)}>{t('取消')}</button></>
        : <><button type="button" className="button" disabled={locked || busy || !canIssue} onClick={() => setConfirm('rotate')}>{t('重新生成令牌')}</button><button type="button" className="button danger" disabled={locked || busy} onClick={() => setConfirm('revoke')}>{t('撤销令牌')}</button></>}</div>
    </div></details> : authEnabled && canIssue && <button ref={primaryButton} type="button" className="button primary" disabled={locked || busy || !status} onClick={() => void change(false)}>{busy ? t('正在生成…') : t('生成个人令牌')}</button>}
    </div>
  </section>;
}
