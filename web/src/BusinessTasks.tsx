import { t } from './locale';
import { useEffect, useState, type ChangeEvent } from 'react';
import exampleSuite from '../../examples/security-tasks/fixture.json?raw';
import { api, errorText, type TaskSuite } from './api';
import { caseName, emptySuite, joinSuite, splitSuite, taskName, tasks, type SuiteDraft } from './taskSuite';
import { Modal } from './ui';

const suiteLimit = 1024 * 1024;

export function BusinessTasks({ id, selected, toggle, saveSuite, close }: {
  id: string;
  selected: string[];
  toggle?: (id: string, enabled: boolean) => void;
  saveSuite?: (revision: string | null, content: string) => Promise<void>;
  close: () => void;
}) {
  return <Modal title={t('业务任务与固定场景')} size="wide" close={close}>
    <div className="panel-layout">
      <div className="panel-body form-fields business-config">
        <fieldset className="form-fields mapping-options" disabled={!toggle}>
          <legend>{t('内置任务')}</legend>
          {tasks.map(task => <div className="business-task-choice" key={task.id}>
            <label><input type="checkbox" checked={selected.includes(task.id)}
              onChange={event => toggle?.(task.id, event.target.checked)} />{task.name}</label>
            <p className="field-help">{task.description}</p>
          </div>)}
          <p className="field-help">{t('任务选择随模型草稿保存。任务使用模型中已映射的逻辑标签和证据字段。')}</p>
        </fieldset>
        {saveSuite ? <FixedSuite id={id} save={saveSuite} /> : <p className="muted small">{t('固定数据和预期证据由管理员维护。')}</p>}
      </div>
      <footer className="panel-footer"><button type="button" className="button primary" onClick={close}>{t('完成')}</button></footer>
    </div>
  </Modal>;
}

function FixedSuite({ id, save }: { id: string; save: (revision: string | null, content: string) => Promise<void> }) {
  const [suite, setSuite] = useState<TaskSuite>();
  const [draft, setDraft] = useState<SuiteDraft>(emptySuite);
  const [index, setIndex] = useState(0);
  const [task, setTask] = useState<string>(tasks[0].id);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const path = '/models/' + id + '/task-suite';

  function loaded(value: TaskSuite) {
    setSuite(value);
    setDirty(false);
    setIndex(0);
    try {
      setDraft(value.content === null ? emptySuite : splitSuite(value.content));
    } catch (cause) {
      // A new revision must never be attached to the previous editor contents.
      setDraft(emptySuite);
      throw new Error(t('当前固定场景无法解析。编辑区已清空，请导入修正后的 JSON 或载入示例。'), { cause });
    }
  }

  useEffect(() => {
    const controller = new AbortController();
    api<TaskSuite>(path, { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) loaded(value);
    }).catch(cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [path]);

  function update(value: SuiteDraft) {
    setDraft(value);
    setDirty(true);
    setMessage('');
    setError('');
  }

  function replace(content: string) {
    try {
      update(splitSuite(content));
      setIndex(0);
    } catch (cause) { setError(errorText(cause)); }
  }

  async function importFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.currentTarget.files?.[0];
    event.currentTarget.value = '';
    if (!file) return;
    try {
      if (file.size > suiteLimit) throw new Error(t('固定场景文件超过 1 MiB。'));
      replace(await file.text());
    } catch (cause) { setError(errorText(cause)); }
  }

  async function submit() {
    if (!suite || !dirty || saving) return;
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const content = joinSuite(draft);
      if (new TextEncoder().encode(content).byteLength > suiteLimit) throw new Error(t('固定场景文件超过 1 MiB。'));
      await save(suite.revision, content);
      loaded(await api<TaskSuite>(path));
      setMessage(t('固定场景已保存。请重新预览业务影响。'));
    } catch (cause) { setError(errorText(cause)); }
    finally { setSaving(false); }
  }

  async function reloadSuite() {
    setLoading(true);
    setError('');
    setMessage('');
    try { loaded(await api<TaskSuite>(path)); }
    catch (cause) { setError(errorText(cause)); }
    finally { setLoading(false); }
  }

  function addCase() {
    const base = 'case-' + (draft.cases.length + 1);
    const taken = new Set(draft.cases.map(caseName));
    let id = base;
    for (let suffix = 2; taken.has(id); suffix++) id = base + '-' + suffix;
    const parameters = task === tasks[0].id ? { namespace: '', person_id: '', as_of: '' } : { alert_id: '' };
    const raw = JSON.stringify({
      id, task, tenant_id: '', parameters: { ...parameters, window_start: '', window_end: '' },
      expected: { subject: 'found', anchor: null, findings: [] },
    }, null, 2);
    update({ ...draft, cases: [...draft.cases, raw] });
    setIndex(draft.cases.length);
  }

  return <section className="form-fields" aria-label={t('固定回归场景')}>
    <div>
      <h3>{t('固定回归场景')}</h3>
      <p className="field-help">{suite?.source === 'deployment'
        ? t('当前使用部署文件。保存后，由控制台管理的套件优先。')
        : suite?.source === 'console' ? t('当前由控制台管理。') : t('使用合成数据配置固定场景和预期证据。')}</p>
    </div>
    {loading && <p role="status">{t('正在读取固定场景…')}</p>}
    {error && <p role="alert" className="notice error">{error}</p>}
    {message && <p role="status" className="notice">{message}</p>}
    <fieldset disabled={loading || saving} className="form-fields business-suite-fields">
      <div className="business-actions">
        <button type="button" className="button compact" onClick={() => replace(exampleSuite)}>{t('载入合成示例')}</button>
        <label>{t('导入固定场景 JSON')}<input type="file" accept=".json,application/json" onChange={event => void importFile(event)} /></label>
        <button type="button" className="button compact" onClick={() => {
          if (!dirty || window.confirm(t('重新读取会丢弃未保存的场景修改。继续？'))) void reloadSuite();
        }}>{t('重新读取套件')}</button>
      </div>
      <details className="form-disclosure">
        <summary>{t('固定表数据')}</summary>
        <label>{t('表结构与记录（JSON）')}<textarea className="mono business-json" rows={9} spellCheck={false} value={draft.tables}
          onChange={event => update({ ...draft, tables: event.target.value })} /></label>
      </details>
      <div className="business-actions">
        {draft.cases.length > 0 && <label>{t('选择场景')}<select value={index} onChange={event => setIndex(Number(event.target.value))}>
          {draft.cases.map((raw, position) => <option key={position} value={position}>{caseName(raw, position)}</option>)}
        </select></label>}
        <label>{t('新增场景的任务')}<select value={task} onChange={event => setTask(event.target.value)}>
          {tasks.map(option => <option key={option.id} value={option.id}>{taskName(option.id)}</option>)}
        </select></label>
        <button type="button" className="button compact" disabled={draft.cases.length >= 16} onClick={addCase}>{t('添加场景')}</button>
      </div>
      {draft.cases[index] !== undefined && <>
        <label>{t('参数与预期证据（JSON）')}<textarea className="mono business-json" rows={12} spellCheck={false} value={draft.cases[index]}
          onChange={event => update({ ...draft, cases: draft.cases.map((raw, position) => position === index ? event.target.value : raw) })} /></label>
        <button type="button" className="button compact" onClick={() => {
          update({ ...draft, cases: draft.cases.filter((_, position) => position !== index) });
          setIndex(Math.max(0, Math.min(index, draft.cases.length - 2)));
        }}>{t('移除此场景')}</button>
      </>}
      <div className="business-actions">
        <button type="button" className="button" disabled={!suite || !dirty} onClick={() => void submit()}>
          {saving ? t('正在保存…') : t('保存固定场景')}
        </button>
      </div>
    </fieldset>
  </section>;
}
