import { t } from './locale';
import { useEffect, useId, useRef, useState, type ChangeEvent, type ReactNode } from 'react';
import { ArrowLeft, Database, FlaskConical, Radar, RefreshCw, Upload, UserMinus } from 'lucide-react';
import exampleSuite from '../../examples/security-tasks/fixture.json?raw';
import { api, errorText, type TaskSuite } from './api';
import { BusinessScenario } from './BusinessScenario';
import { BusinessTabs } from './BusinessTabs';
import { caseSummary, emptySuite, joinSuite, splitSuite, taskName, tasks, type SuiteDraft } from './taskSuite';
import { Modal } from './ui';
import { WorkspaceTools } from './WorkspaceTools';

const suiteLimit = 1024 * 1024;
const maxScenarios = 16;
const title = t('业务任务与固定场景');
const taskViews = [
  { id: 'tasks', label: t('内置任务') },
  { id: 'scenarios', label: t('固定场景') },
] as const;

export function BusinessTasks({ id, selected, toggle, saveSuite, close }: {
  id: string;
  selected: string[];
  toggle?: (id: string, enabled: boolean) => void;
  saveSuite?: (revision: string | null, content: string) => Promise<void>;
  close: () => void;
}) {
  const taskDescriptionId = useId();
  const options = <fieldset className="form-fields business-task-options" disabled={!toggle}>
    <legend className="sr-only">{t('内置任务')}</legend>
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
  const [view, setView] = useState<typeof taskViews[number]['id']>('tasks');
  const [showTables, setShowTables] = useState(false);
  const tabsId = useId();
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const fileInput = useRef<HTMLInputElement>(null);
  const path = '/models/' + id + '/task-suite';
  const scenarios = draft.cases.map(caseSummary);
  const editingSuite = view === 'scenarios';

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

  function addCase(definition: typeof tasks[number]) {
    const base = 'case-' + (draft.cases.length + 1);
    const taken = new Set(scenarios.map(scenario => scenario.name));
    let id = base;
    for (let suffix = 2; taken.has(id); suffix++) id = base + '-' + suffix;
    const parameters = Object.fromEntries(definition.parameters.map(parameter => [parameter.key, '']));
    const raw = JSON.stringify({
      id, task: definition.id, tenant_id: '', parameters,
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
      <BusinessTabs id={tabsId} label={title} selected={view} choices={taskViews} change={setView} />
      <div className="panel-body form-fields business-config" id={tabsId + '-panel'} role="tabpanel" aria-labelledby={tabsId + '-' + view}>
        {!editingSuite ? children : <fieldset disabled={loading || saving} className="form-fields business-suite-fields">
          <div className="suite-toolbar">
            {showTables
              ? <button type="button" className="button compact" onClick={() => setShowTables(false)}>
                <ArrowLeft size={16} aria-hidden="true" />{t('返回场景')}
              </button>
              : <div className="suite-summary">
                <strong>{t('{0} / {1} 个场景', draft.cases.length, maxScenarios)}</strong>
                <span className="field-help">{loading ? t('正在读取固定场景…') : suite?.source === 'deployment'
                  ? t('当前使用部署文件。保存后，由控制台管理的套件优先。')
                  : suite?.source === 'console' ? t('当前由控制台管理。') : t('使用合成数据配置固定场景和预期证据。')}</span>
              </div>}
            {!showTables && !loading && !saving && <WorkspaceTools label={t('套件操作')}>
              <button type="button" className="tool-button" onClick={() => replace(exampleSuite)}><FlaskConical size={15} aria-hidden="true" />{t('载入合成示例')}</button>
              <button type="button" className="tool-button" onClick={() => fileInput.current?.click()}><Upload size={15} aria-hidden="true" />{t('导入固定场景 JSON')}</button>
              <button type="button" className="tool-button" onClick={() => setShowTables(true)}><Database size={15} aria-hidden="true" />{t('固定表数据')}</button>
              <button type="button" className="tool-button" onClick={() => void reloadSuite()}><RefreshCw size={15} aria-hidden="true" />{t('重新读取套件')}</button>
            </WorkspaceTools>}
            <input hidden ref={fileInput} type="file" accept=".json,application/json" onChange={event => void importFile(event)} />
          </div>
          {showTables ? <section className="form-fields scenario-editor">
            <h3>{t('固定表数据')}</h3>
            <label>{t('表结构与记录（JSON）')}<textarea className="mono business-json" rows={18} spellCheck={false} value={draft.tables}
              onChange={event => update({ ...draft, tables: event.target.value })} /></label>
          </section> : !loading && <div className="scenario-layout">
            <div className="scenario-list">
              <div className="scenario-list-heading">
                <strong>{t('场景列表')}</strong>
                {draft.cases.length < maxScenarios && !saving && <WorkspaceTools label={t('添加场景')}>
                  {tasks.map(task => <button type="button" className="tool-button" key={task.id} onClick={() => addCase(task)}>{task.name}</button>)}
                </WorkspaceTools>}
              </div>
              {scenarios.length > 0 && <>
                <label className="scenario-select"><span className="sr-only">{t('选择场景')}</span>
                  <select value={index} onChange={event => setIndex(Number(event.target.value))}>
                    {scenarios.map((scenario, position) => <option key={position} value={position}>{scenario.name}</option>)}
                  </select>
                </label>
                <ul className="scenario-choices" aria-label={t('选择场景')}>
                  {scenarios.map((scenario, position) => <li key={position}>
                    <button type="button" aria-pressed={position === index} onClick={() => setIndex(position)}>
                      <code>{scenario.name}</code>
                      <small>{taskName(scenario.task) || t('任务未配置')}</small>
                    </button>
                  </li>)}
                </ul>
              </>}
            </div>
            {draft.cases[index] === undefined ? <div className="panel-placeholder scenario-empty">
              <FlaskConical size={26} aria-hidden="true" />
              <h4>{t('尚未配置固定场景')}</h4>
              <button type="button" className="button compact" onClick={() => replace(exampleSuite)}>{t('载入合成示例')}</button>
            </div> : <BusinessScenario key={index} name={scenarios[index].name} raw={draft.cases[index]}
              change={raw => update({ ...draft, cases: draft.cases.map((value, position) => position === index ? raw : value) })}
              remove={() => {
                update({ ...draft, cases: draft.cases.filter((_, position) => position !== index) });
                setIndex(Math.max(0, Math.min(index, draft.cases.length - 2)));
              }} />}
          </div>}
        </fieldset>}
      </div>
      <footer className="panel-footer">
        <p className={'panel-footer-context' + (error ? ' inline-error' : '')} role={error ? 'alert' : 'status'}>
          {error || message || (dirty ? t('有未保存的场景修改') : '')}
        </p>
        <button type="button" className={'button' + (editingSuite ? '' : ' primary')} disabled={saving}
          onClick={() => { if (discardChanges()) close(); }}>{t('完成')}</button>
        {editingSuite && <button type="button" className="button primary" disabled={!suite || !dirty || saving || loading} onClick={() => void submit()}>
          {saving ? t('正在保存…') : t('保存固定场景')}
        </button>}
      </footer>
    </div>
  </Modal>;
}
