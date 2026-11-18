import { t } from './locale.ts';
import { createContext, useContext, useId, useLayoutEffect, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { AlertTriangle, CheckCircle2, CircleMinus, Share2, X } from 'lucide-react';
import type { Status } from './api';
import { modelIdPattern } from './model';
import graphlakeLogo from './assets/graphlake-logo.png';

export const WorkspaceLocked = createContext(false);

export function BrandLogo({ size = 36 }: { size?: number }) {
  return <img className="brand-logo" src={graphlakeLogo} alt="" width={size} height={size} />;
}

export function Mark({ size = 25 }: { size?: number }) {
  return <Share2 className="brand-mark" size={size} strokeWidth={2.25} aria-hidden="true" />;
}

export function StatusLabel({ status, text }: { status: Status; text?: string }) {
  const [Icon, label] = status === 'passed' ? [CheckCircle2, t('已通过')]
    : status === 'failed' ? [AlertTriangle, t('校验失败')] : [CircleMinus, t('未校验')];
  return <span className={'status ' + status}><Icon size={16} aria-hidden="true" />{text ?? label}</span>;
}

export function Modal({ title, description, close, beforeClose, children, layout = 'drawer', size = 'standard' }: {
  title: string; description?: string; close: () => void; beforeClose?: () => boolean; children: ReactNode;
  layout?: 'drawer' | 'center'; size?: 'standard' | 'medium' | 'wide';
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const heading = useId();
  const descriptionId = useId();
  const locked = useContext(WorkspaceLocked);
  useLayoutEffect(() => {
    if (locked) return;
    const element = dialog.current;
    (element?.querySelector('[data-initial-focus]') ?? element?.querySelector('h2'))?.setAttribute('autofocus', '');
    element?.showModal();
    return () => element?.close();
  }, [locked]);
  return <dialog ref={dialog} className={'dialog-' + layout + ' dialog-' + size} aria-labelledby={heading} aria-describedby={description ? descriptionId : undefined}
    onCancel={event => { if (beforeClose && !beforeClose()) event.preventDefault(); }}
    onClose={event => { if (!event.currentTarget.open && !locked) close(); }}>
    <header className="dialog-heading"><div><h2 id={heading} tabIndex={-1}>{title}</h2>{description && <p id={descriptionId}>{description}</p>}</div>
      <form method="dialog" onSubmit={event => { if (beforeClose && !beforeClose()) event.preventDefault(); }}>
        <button className="icon-button" aria-label={t('关闭')}><X size={18} /></button></form></header>
    <div className="dialog-content">{children}</div>
  </dialog>;
}

export function NameDialog({ title, close, submit, initial = '', withId = false, choices, label = t('名称'), placeholder, action = t('继续') }: {
  title: string;
  close: () => void;
  submit: (name: string, extra: string) => void;
  initial?: string;
  withId?: boolean;
  choices?: [string, string][];
  label?: string;
  placeholder?: string;
  action?: string;
}) {
  const [name, setName] = useState(initial);
  const [extra, setExtra] = useState(choices?.[0]?.[0] ?? '');
  const [error, setError] = useState('');
  const idHint = useId();
  function send(event: FormEvent) {
    event.preventDefault();
    try {
      if (!name.trim()) throw new Error(t('请输入名称'));
      submit(name.trim(), extra.trim());
      close();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t('无法保存'));
    }
  }
  return <Modal layout="center" title={title} description={withId ? t('先填写基本信息，随后配置数据来源与图映射。') : choices ? t('选择连接器，下一步填写连接和表映射。') : undefined} close={close}>
    <form onSubmit={send} className="panel-layout">
      <div className="panel-body form-fields">
      <label>{label}<input data-initial-focus required maxLength={128} placeholder={placeholder} value={name} onChange={event => { setName(event.target.value); setError(''); }} /></label>
      {withId && <div className="form-fields"><label>{t('模型标识')}<input className="mono" required pattern={modelIdPattern} maxLength={64} aria-describedby={idHint}
        placeholder="payments" value={extra} onChange={event => setExtra(event.target.value)} /></label>
        <p id={idHint} className="field-help">{t('用于访问模型，可包含英文字母、数字、下划线和连字符。')}</p></div>}
      {choices && <label>{t('连接器')}<select value={extra} onChange={event => setExtra(event.target.value)}>
        {choices.map(([id, label]) => <option key={id} value={id}>{label}</option>)}
      </select></label>}
      {error && <p role="alert" className="inline-error">{error}</p>}
      </div>
      <footer className="panel-footer"><button type="button" className="button" onClick={close}>{t('取消')}</button><button className="button primary" type="submit">{action}</button></footer>
    </form>
  </Modal>;
}
