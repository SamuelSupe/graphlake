import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { api, authRequired, errorText } from './api';
import { Mark, WorkspaceLocked } from './ui';

type Session = { authenticated: boolean; enabled: boolean; username: string | null };

function Login({ expired, signedIn }: { expired: boolean; signedIn: (session: Session) => void }) {
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => { document.title = expired ? 'GraphLake · 会话已过期' : 'GraphLake · 登录'; }, [expired]);
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

  return <main className="auth-page">
    <section className="auth-card" aria-labelledby="login-heading">
      <div className="auth-brand"><span className="model-icon"><Mark size={22} /></span>GraphLake</div>
      <h1 id="login-heading">{expired ? '会话已过期' : '登录管理后台'}</h1>
      {expired && <p className="muted">重新登录后可继续编辑本页草稿。</p>}
      <form onSubmit={submit} aria-busy={busy}>
        <label>用户名<input name="username" autoComplete="username" defaultValue="admin" maxLength={128} required /></label>
        <label>密码<input name="password" autoComplete="current-password" type="password" maxLength={256} required autoFocus /></label>
        {error && <p className="notice error" role="alert">{error}</p>}
        <button className="button primary" type="submit" disabled={busy}>{busy ? '正在登录…' : '登录'}</button>
      </form>
    </section>
  </main>;
}

export function AuthGate({ workspace }: {
  workspace: (logout: (() => Promise<void>) | undefined, locked: boolean) => ReactNode;
}) {
  const [session, setSession] = useState<Session>();
  const [opened, setOpened] = useState(false);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  const unlocked = session?.authenticated === true;

  const signedIn = useCallback((value: Session) => {
    setSession(value);
    if (value.authenticated) setOpened(true);
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
      ? { enabled: true, authenticated: false, username: null } : current);
    let checking = false;
    async function check() {
      if (checking || document.visibilityState !== 'visible') return;
      checking = true;
      try {
        const value = await api<Session>('/auth/session', { signal: controller.signal });
        if (!controller.signal.aborted && !value.authenticated) lock();
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
  }, [unlocked]);

  async function logout() {
    await api('/auth/logout', { method: 'POST' });
    setOpened(false);
    setSession({ authenticated: false, enabled: true, username: null });
  }

  if (!session) return <main className="auth-page"><section className="auth-card">
    {error ? <><p role="alert">{error}</p><button className="button" onClick={() => {
      setError(''); setAttempt(value => value + 1);
    }}>重试</button></> : <p role="status">正在检查登录状态…</p>}
  </section></main>;

  return <>
    {opened && <WorkspaceLocked.Provider value={!unlocked}>
      <div hidden={!unlocked} inert={!unlocked}>{workspace(session.enabled ? logout : undefined, !unlocked)}</div>
    </WorkspaceLocked.Provider>}
    {!unlocked && <Login expired={opened} signedIn={signedIn} />}
  </>;
}
