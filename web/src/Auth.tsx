import { t } from './locale.ts';
import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { api, authRequired, errorText } from './api';
import { clearPendingQueries } from './Query';
import { clearPendingMutations } from './Mutations';
import { Mark, WorkspaceLocked } from './ui';
import { LanguageSelect } from './LanguageSelect';

export type Session = { authenticated: boolean; enabled: boolean; username: string | null; administrator: boolean; subject?: string | null; authorization?: string | null; proxy_sign_in?: boolean };

function Login({ expired, username, proxySignIn, signedIn }: { expired: boolean; username?: string; proxySignIn?: boolean; signedIn: (session: Session) => void }) {
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => { document.title = expired ? t('GraphLake · 会话已过期') : t('GraphLake · 登录'); }, [expired]);
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const data = new FormData(form);
    setBusy(true);
    setError('');
    try {
      const session = await api<Session>('/auth/login', {
        method: 'POST',
        body: JSON.stringify({ username: data.get('username'), password: data.get('password') }),
      });
      form.reset();
      signedIn(session);
    } catch (cause) {
      setError(errorText(cause));
    } finally {
      setBusy(false);
    }
  }

  async function sso() {
    setBusy(true);
    setError('');
    try {
      signedIn(await api<Session>('/auth/sso', { method: 'POST' }));
    } catch (cause) {
      setError(errorText(cause));
    } finally {
      setBusy(false);
    }
  }

  return <main className="auth-page">
    <section className="auth-card" aria-labelledby="login-heading">
      <div className="auth-card-heading">
        <div className="auth-brand"><span className="model-icon"><Mark size={22} /></span>GraphLake</div>
        <LanguageSelect />
      </div>
      <h1 id="login-heading">{expired ? t('会话已过期') : t('登录管理后台')}</h1>
      {expired && <p className="muted">{t('重新登录后可继续编辑本页草稿。')}</p>}
      {proxySignIn && <button className="button primary" type="button" disabled={busy} onClick={() => void sso()}>{t('使用企业账号登录')}</button>}
      <form onSubmit={submit} aria-busy={busy}>
        {proxySignIn && <p className="muted">{t('管理员恢复登录')}</p>}
        <label>{t('用户名')}<input name="username" autoComplete="username" defaultValue={username ?? 'admin'} maxLength={128} required /></label>
        <label>{t('密码')}<input name="password" autoComplete="current-password" type="password" maxLength={256} required autoFocus /></label>
        {error && <p className="notice error" role="alert">{error}</p>}
        <button className="button primary" type="submit" disabled={busy}>{busy ? t('正在登录…') : t('登录')}</button>
      </form>
    </section>
  </main>;
}

export function AuthGate({ workspace }: {
  workspace: (logout: (() => Promise<void>) | undefined, locked: boolean, identity: Session) => ReactNode;
}) {
  const [session, setSession] = useState<Session>();
  const [identity, setIdentity] = useState<Session>();
  const opened = !!identity;
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  const unlocked = session?.authenticated === true;

  const signedIn = useCallback((value: Session) => {
    setSession(value);
    if (value.authenticated) setIdentity(value);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    api<Session>('/auth/session', { signal: controller.signal }).then(signedIn, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [attempt, signedIn]);

  useEffect(() => {
    if (!unlocked) return;
    const controller = new AbortController();
    const lock = () => setSession(current => current?.authenticated
      ? { enabled: true, authenticated: false, username: null, administrator: false } : current);
    let checking = false;
    async function check() {
      if (checking || document.visibilityState !== 'visible') return;
      checking = true;
      try {
        const value = await api<Session>('/auth/session', { signal: controller.signal });
        if (controller.signal.aborted) return;
        if (!value.authenticated) lock();
        else if (value.username !== identity?.username || value.authorization !== identity?.authorization || value.administrator !== identity?.administrator) signedIn(value);
      } catch { /* The next request or status check can confirm expiry. */ }
      finally { checking = false; }
    }
    const timer = window.setInterval(() => void check(), 15_000);
    window.addEventListener(authRequired, lock);
    window.addEventListener('focus', check);
    document.addEventListener('visibilitychange', check);
    return () => {
      controller.abort();
      window.clearInterval(timer);
      window.removeEventListener(authRequired, lock);
      window.removeEventListener('focus', check);
      document.removeEventListener('visibilitychange', check);
    };
  }, [unlocked, identity, signedIn]);

  async function logout() {
    await api('/auth/logout', { method: 'POST' });
    clearPendingQueries();
    clearPendingMutations();
    setIdentity(undefined);
    setSession({ authenticated: false, enabled: true, username: null, administrator: false });
  }

  if (!session) return <main className="auth-page"><section className="auth-card">
    <div className="auth-card-heading"><LanguageSelect /></div>
    {error ? <><p role="alert">{error}</p><button className="button" onClick={() => {
      setError(''); setAttempt(value => value + 1);
    }}>{t('重试')}</button></> : <p role="status">{t('正在检查登录状态…')}</p>}
  </section></main>;

  return <>
    {identity && <WorkspaceLocked.Provider value={!unlocked}>
      <div hidden={!unlocked} inert={!unlocked}>{workspace(session.enabled ? logout : undefined, !unlocked, identity)}</div>
    </WorkspaceLocked.Provider>}
    {!unlocked && <Login expired={opened} username={identity?.username ?? undefined} proxySignIn={session.proxy_sign_in || identity?.proxy_sign_in} signedIn={signedIn} />}
  </>;
}
