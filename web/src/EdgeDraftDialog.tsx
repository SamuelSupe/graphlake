import { useId, useRef, useState, type FormEvent } from 'react';
import { ArrowRightLeft } from 'lucide-react';
import { errorText } from './api';
import { edgeNameIssue, type Model } from './model';
import { Modal } from './ui';

export type EdgeEnds = { from: string; to: string };

export function EdgeDraftDialog({ model, ends, close, create }: {
  model: Model;
  ends?: EdgeEnds;
  close: () => void;
  create: (name: string, ends: EdgeEnds) => void;
}) {
  const [name, setName] = useState('');
  const [from, setFrom] = useState(ends?.from ?? '');
  const [to, setTo] = useState(ends?.to ?? '');
  const [error, setError] = useState('');
  const [nameError, setNameError] = useState('');
  const errorId = useId();
  const nameField = useRef<HTMLInputElement>(null);
  const nodes = Object.keys(model.nodes);
  function submit(event: FormEvent) {
    event.preventDefault();
    const draftName = name.trim();
    const issue = edgeNameIssue(model, draftName);
    if (issue) {
      setNameError(issue);
      requestAnimationFrame(() => nameField.current?.focus());
      return;
    }
    try {
      create(draftName, { from, to });
      close();
    } catch (cause) { setError(errorText(cause)); }
  }

  return <Modal title="新建边类型" description="确认连接方向，创建后可配置来源表和端点字段。" close={close}>
    <form className="panel-layout" onSubmit={submit}>
      <div className="panel-body form-fields">
        <div className="edge-draft-name">
          <label>边类型名称<input ref={nameField} required data-initial-focus placeholder="例如 Transfer" value={name}
            aria-invalid={!!nameError} aria-describedby={nameError ? errorId : undefined}
            onChange={event => { setName(event.target.value); setNameError(''); setError(''); }} /></label>
          <p id={errorId} className={nameError ? 'inline-error' : 'sr-only'} aria-live="polite">{nameError}</p>
        </div>
        <label>起点<select required value={from} onChange={event => { setFrom(event.target.value); setError(''); }}>
          <option value="">选择起点节点</option>{nodes.map(node => <option key={node}>{node}</option>)}
        </select></label>
        <label>终点<select required value={to} onChange={event => { setTo(event.target.value); setError(''); }}>
          <option value="">选择终点节点</option>{nodes.map(node => <option key={node}>{node}</option>)}
        </select></label>
        <div className="actions"><button type="button" className="button compact" disabled={!from || !to || from === to}
          onClick={() => { setFrom(to); setTo(from); setError(''); }}><ArrowRightLeft size={15} aria-hidden="true" />交换起点与终点</button></div>
        {error && <p className="inline-error" role="alert">{error}</p>}
      </div>
      <footer className="panel-footer">
        <button type="button" className="button" onClick={close}>取消</button>
        <button type="submit" className="button primary">创建边</button>
      </footer>
    </form>
  </Modal>;
}
