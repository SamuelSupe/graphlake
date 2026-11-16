import { t } from './locale.ts';
import { useRef, useState, type FormEvent } from 'react';
import { Plus, X } from 'lucide-react';
import { errorText } from './api';
import { comparisons, nullCheck, scopeDraft, scopeJson, type PartitionDraft, type ScopeDraft } from './scopeOptions';
import { Modal } from './ui';

function initialScope(raw: string) {
  try { return { draft: scopeDraft(raw), json: false }; }
  catch { return { draft: scopeDraft(''), json: true }; }
}

export default function QueryScope({ value, save, close }: { value: string; save: (value: string) => void; close: () => void }) {
  const [initial] = useState(() => initialScope(value));
  const [draft, setDraft] = useState(initial.draft);
  const [raw, setRaw] = useState(value);
  const [json, setJson] = useState(initial.json);
  const [error, setError] = useState('');
  const feedback = useRef<HTMLParagraphElement>(null);

  function update(change: Partial<ScopeDraft>) {
    setDraft(previous => ({ ...previous, ...change }));
    setError('');
  }
  function filter(index: number, change: Partial<PartitionDraft>) {
    update({ filters: draft.filters.map((rule, i) => i === index ? { ...rule, ...change } : rule) });
  }
  function report(cause: unknown) {
    setError(errorText(cause));
    requestAnimationFrame(() => feedback.current?.focus());
  }
  function mode() {
    try {
      if (json) setDraft(scopeDraft(raw));
      else setRaw(scopeJson(draft));
      setJson(!json);
      setError('');
    } catch (cause) { report(cause); }
  }
  function apply(event: FormEvent) {
    event.preventDefault();
    try {
      save(json ? scopeJson(scopeDraft(raw)) : scopeJson(draft));
      close();
    } catch (cause) { report(cause); }
  }

  return <Modal title={t('查询范围')} size="medium" description={t('限定本次查询读取的数据；应用后运行查询才会生效。')} close={close}>
    <form className="panel-layout query-scope-form" onSubmit={apply}>
      <div className="panel-body dialog-form">
      {json ? <label>{t('查询范围 JSON')}<textarea rows={8} className="mono" spellCheck={false}
        value={raw} onChange={event => { setRaw(event.target.value); setError(''); }} data-initial-focus /></label>
      : <>
        <section className="scope-section"><h3>{t('时间与一致性')}</h3>
        <label>{t('业务时间')}<input value={draft.time} onChange={event => update({ time: event.target.value })}
          placeholder={t('例如 2025-01-01 或 current')} data-initial-focus /></label>
        <label className="check-label"><input type="checkbox" checked={draft.consistent}
          onChange={event => update({ consistent: event.target.checked })} />{t('要求来源快照一致')}</label></section>
        <fieldset className="form-fields mapping-options">
          <legend>{t('逻辑分区')}</legend>
          {!draft.filters.length && <p className="muted small">{t('未设置条件，将读取当前权限允许的全部分区。')}</p>}
          {draft.filters.map(({ column, op, value }, index) => <div className="scope-rule" key={index}>
            <label>{t('列名')}<input value={column} required onChange={event => filter(index, { column: event.target.value })} /></label>
            <label>{t('条件')}<select value={op} onChange={event => filter(index, { op: event.target.value })}>
              {comparisons.map(([code, label]) => <option value={code} key={code}>{label}</option>)}
            </select></label>
            {!nullCheck(op) && <label>{t('值')}<input value={value} required className="mono" placeholder={t('例如 1、true、"华东"')}
              onChange={event => filter(index, { value: event.target.value })} /></label>}
            <button type="button" className="icon-button" aria-label={t('移除条件 {0}', (index + 1))}
              onClick={() => update({ filters: draft.filters.filter((_, i) => i !== index) })}><X size={16} /></button>
          </div>)}
          <button type="button" className="button compact" onClick={() => update({ filters: [...draft.filters, { column: '', op: 'eq', value: '' }] })}>
            <Plus size={16} />{t('添加条件')}
          </button>
        </fieldset>
      </>}
      {error && <p ref={feedback} tabIndex={-1} role="alert" className="inline-error">{error}</p>}
      </div><footer className="panel-footer">
        <button type="button" className="text-button scope-mode" onClick={mode}>{json ? t('使用表单') : t('编辑 JSON')}</button>
        <button type="button" className="button" onClick={close}>{t('取消')}</button>
        <button type="submit" className="button primary">{t('应用')}</button>
      </footer>
    </form>
  </Modal>;
}
