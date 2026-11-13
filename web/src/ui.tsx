import { createContext, useContext, useId, useLayoutEffect, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { AlertTriangle, CheckCircle2, CircleMinus, Share2, X } from 'lucide-react';
import type { Status } from './api';
import { modelIdPattern } from './model';

export const WorkspaceLocked = createContext(false);

export function Mark({ size = 25 }: { size?: number }) {
  return <Share2 className="brand-mark" size={size} strokeWidth={2.25} aria-hidden="true" />;
}

export function StatusLabel({ status }: { status: Status }) {
  const [Icon, label] = status === 'passed' ? [CheckCircle2, '已通过']
    : status === 'failed' ? [AlertTriangle, '校验失败'] : [CircleMinus, '未校验'];
  return <span className={'status ' + status}><Icon size={16} aria-hidden="true" />{label}</span>;
}

export function Modal({ title, close, children }: { title: string; close: () => void; children: ReactNode }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const heading = useId();
  const locked = useContext(WorkspaceLocked);
  useLayoutEffect(() => {
    if (locked) return;
    const element = dialog.current;
    element?.querySelector('[data-initial-focus]')?.setAttribute('autofocus', '');
    element?.showModal();
    return () => element?.close();
  }, [locked]);
  return <dialog ref={dialog} aria-labelledby={heading} onClose={event => { if (!event.currentTarget.open && !locked) close(); }}>
    <div className="dialog-heading"><h2 id={heading}>{title}</h2><form method="dialog"><button className="icon-button" aria-label="关闭"><X size={18} /></button></form></div>
    {children}
  </dialog>;
}

export function NameDialog({ title, close, submit, initial = '', withId = false, choices }: {
  title: string;
  close: () => void;
  submit: (name: string, extra: string) => void;
  initial?: string;
  withId?: boolean;
  choices?: [string, string][];
}) {
  const [name, setName] = useState(initial);
  const [extra, setExtra] = useState(choices?.[0]?.[0] ?? '');
  const [error, setError] = useState('');
  function send(event: FormEvent) {
    event.preventDefault();
    try {
      if (!name.trim()) throw new Error('请输入名称');
      submit(name.trim(), extra.trim());
      close();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法保存');
    }
  }
  return <Modal title={title} close={close}>
    <form onSubmit={send} className="dialog-form">
      <label>名称<input data-initial-focus required maxLength={128} value={name} onChange={event => setName(event.target.value)} /></label>
      {withId && <label>模型标识<input className="mono" required pattern={modelIdPattern} maxLength={64}
        placeholder="payments" value={extra} onChange={event => setExtra(event.target.value)} /></label>}
      {choices && <label>连接器<select value={extra} onChange={event => setExtra(event.target.value)}>
        {choices.map(([id, label]) => <option key={id} value={id}>{label}</option>)}
      </select></label>}
      {error && <p role="alert" className="inline-error">{error}</p>}
      <div className="dialog-actions"><button type="button" className="button" onClick={close}>取消</button><button className="button primary" type="submit">继续</button></div>
    </form>
  </Modal>;
}
