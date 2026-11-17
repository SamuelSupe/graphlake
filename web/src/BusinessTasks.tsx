import { t } from './locale';
import { useEffect, useId, useRef, useState, type ChangeEvent, type ReactNode } from 'react';
import { FlaskConical, Plus, Radar, RefreshCw, Upload, UserMinus } from 'lucide-react';
import exampleSuite from '../../examples/security-tasks/fixture.json?raw';
import { api, errorText, type TaskSuite } from './api';
import { BusinessScenario } from './BusinessScenario';
import { caseSummary, emptySuite, joinSuite, splitSuite, taskName, tasks, type SuiteDraft } from './taskSuite';
import { Modal } from './ui';

const suiteLimit = 1024 * 1024;
const maxScenarios = 16;
const title = t('业务任务与固定场景');

export function BusinessTasks({ id, selected, toggle, saveSuite, close }: {
  id: string;
  selected: string[];
  toggle?: (id: string, enabled: boolean) => void;
  saveSuite?: (revision: string | null, content: string) => Promise<void>;
  close: () => void;
}) {
  const taskDescriptionId = useId();
  const options = <fieldset className="form-fields business-task-options" disabled={!toggle}>
    <legend>{t('内置任务')}</legend>
    <div className="business-task-grid">
      {tasks.map((task, index) => {
        const Icon = index === 0 ? UserMinus : Radar;
        const checked = selected.includes(task.id);
        return <label className="business-task-choice" key={task.id}>
          <span className="business-task-icon"><Icon size={19} aria-hidden="true" /></span>
          <span><strong>{task.name}</strong><span className="field-help" id={taskDescriptionId + task.id}>{task.description}</span></span>
          <input type="checkbox" checked={checked} aria-label={task.name} aria-describedby={taskDescriptionId + task.id} onChange={event => toggle?.(task.id, event.target.checked)} />
        </label>;
      })}
    </div>
    <p className="field-help">{t('任务选择随模型草稿保存。任务使用模型中已映射的逻辑标签和证据字段。')}</p>
  </fieldset>;

  if (saveSuite) return <FixedSuite id={id} save={saveSuite} close={close}>{options}</FixedSuite>;
  return <Modal title={title} size="wide" close={close}>
    <div className="panel-layout">
      <div className="panel-body form-fields business-config">
        {options}
        <p className="muted small">{t('固定数据和预期证据由管理员维护。')}</p>
      </div>
      <footer className="panel-footer"><button type="button" className="button primary" onClick={close}>{t('完成')}</button></footer>
    </div>
  </Modal>;
}

function FixedSuite({ id, save, close, children }: {
  id: string;
  save: (revision: string | null, content: string) => Promise<void>;
  close: () => void;
  children: ReactNode;
}) {
  const [suite, setSuite] = useState<TaskSuite>();
  const [draft, setDraft] = useState<SuiteDraft>(emptySuite);
  const [index, setIndex] = useState(0);
  const [task, setTask] = useState<string>(tasks[0].id);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const fileInput = useRef<HTMLInputElement>(null);
  const path = '/models/' + id + '/task-suite';
  const scenarios = draft.cases.map(caseSummary);

  function loaded(value: TaskSuite, position = 0) {
    setSuite(value);
    setDirty(false);
    try {
      const next = value.content === null ? emptySuite : splitSuite(value.content);
      setDraft(next);
      setIndex(Math.max(0, Math.min(position, next.cases.length - 1)));
    } catch (cause) {
      // A new revision must never be attached to the previous editor contents.
      setDraft(emptySuite);
      setIndex(0);
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
      const next = splitSuite(content);
      if (!discardChanges()) return;
      update(next);
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
      loaded(await api<TaskSuite>(path), index);
      setMessage(t('固定场景已保存。请重新预览业务影响。'));
    } catch (cause) { setError(errorText(cause)); }
    finally { setSaving(false); }
  }

  async function reloadSuite() {
    if (!discardChanges()) return;
    setLoading(true);
    setError('');
    setMessage('');
    try { loaded(await api<TaskSuite>(path)); }
    catch (cause) { setError(errorText(cause)); }
    finally { setLoading(false); }
  }

  function addCase() {
    const base = 'case-' + (draft.cases.length + 1);
    const taken = new Set(scenarios.map(scenario => scenario.name));
    let id = base;
    for (let suffix = 2; taken.has(id); suffix++) id = base + '-' + suffix;
    const definition = tasks.find(item => item.id === task);
    if (!definition) return;
    const parameters = Object.fromEntries(definition.parameters.map(parameter => [parameter.key, '']));
    const raw = JSON.stringify({
      id, task, tenant_id: '', parameters,
      expected: { subject: 'found', anchor: null, findings: [] },
    }, null, 2);
    update({ ...draft, cases: [...draft.cases, raw] });
    setIndex(draft.cases.length);
  }

  function discardChanges() {
    return !saving && (!dirty || window.confirm(t('放弃未保存的场景修改？')));
  }

  return <Modal title={title} size="wide" close={close} beforeClose={discardChanges}>
    <div className="panel-layout">
      <div className="panel-body form-fields business-config">
        {children}
        <section className="form-fields business-suite" aria-label={t('固定回归场景')}>
          <header className="scenario-heading">
            <div><h3>{t('固定回归场景')}</h3>
              <p className="field-help">{suite?.source === 'deployment'
                ? t('当前使用部署文件。保存后，由控制台管理的套件优先。')
                : suite?.source === 'console' ? t('当前由控制台管理。') : t('使用合成数据配置固定场景和预期证据。')}</p>
            </div>
            {draft.cases.length > 0 && <span className="muted small">{t('{0} / {1} 个场景', draft.cases.length, maxScenarios)}</span>}
          </header>
          {loading && <p role="status">{t('正在读取固定场景…')}</p>}
          <fieldset disabled={loading || saving} className="form-fields business-suite-fields">
            <div className="business-actions">
              <button type="button" className="button compact" onClick={() => replace(exampleSuite)}><FlaskConical size={15} aria-hidden="true" />{t('载入合成示例')}</button>
              <button type="button" className="button compact" onClick={() => fileInput.current?.click()}><Upload size={15} aria-hidden="true" />{t('导入固定场景 JSON')}</button>
              <input hidden ref={fileInput} type="file" accept=".json,application/json" onChange={event => void importFile(event)} />
              <button type="button" className="button compact" onClick={() => void reloadSuite()}><RefreshCw size={15} aria-hidden="true" />{t('重新读取套件')}</button>
            </div>
            <div className="scenario-layout">
              <div className="scenario-list">
                <div className="form-fields scenario-add">
                  <label>{t('新增场景的任务')}<select value={task} onChange={event => setTask(event.target.value)}>
                    {tasks.map(option => <option key={option.id} value={option.id}>{option.name}</option>)}
                  </select></label>
                  <button type="button" className="button compact" disabled={draft.cases.length >= maxScenarios} onClick={addCase}><Plus size={15} aria-hidden="true" />{t('添加场景')}</button>
                </div>
                {scenarios.length > 0 && <ul className="versions-choices scenario-choices" aria-label={t('选择场景')}>
                  {scenarios.map((scenario, position) => <li key={position}>
                    <button type="button" className="version-choice" aria-pressed={position === index} onClick={() => setIndex(position)}>
                      <code>{scenario.name}</code>
                      <small>{taskName(scenario.task) || t('任务未配置')}</small>
                    </button>
                  </li>)}
                </ul>}
              </div>
              {draft.cases[index] === undefined ? <div className="panel-placeholder">
                <FlaskConical size={26} aria-hidden="true" />
                <h4>{t('尚未配置固定场景')}</h4>
                <p className="field-help">{t('可载入合成示例，也可以导入 JSON 或手动添加场景。')}</p>
              </div> : <BusinessScenario key={index} raw={draft.cases[index]}
                change={raw => update({ ...draft, cases: draft.cases.map((value, position) => position === index ? raw : value) })}
                remove={() => {
                  update({ ...draft, cases: draft.cases.filter((_, position) => position !== index) });
                  setIndex(Math.max(0, Math.min(index, draft.cases.length - 2)));
                }} />}
            </div>
            <details className="form-disclosure">
              <summary>{t('固定表数据')}</summary>
              <label>{t('表结构与记录（JSON）')}<textarea className="mono business-json" rows={9} spellCheck={false} value={draft.tables}
                onChange={event => update({ ...draft, tables: event.target.value })} /></label>
            </details>
          </fieldset>
        </section>
      </div>
      <footer className="panel-footer">
        <p className={'panel-footer-context' + (error ? ' inline-error' : '')} role={error ? 'alert' : 'status'}>
          {error || message || (dirty ? t('有未保存的场景修改') : '')}
        </p>
        <button type="button" className="button" disabled={saving} onClick={() => { if (discardChanges()) close(); }}>{t('完成')}</button>
        <button type="button" className="button primary" disabled={!suite || !dirty || saving || loading} onClick={() => void submit()}>
          {saving ? t('正在保存…') : t('保存固定场景')}
        </button>
      </footer>
    </div>
  </Modal>;
}
