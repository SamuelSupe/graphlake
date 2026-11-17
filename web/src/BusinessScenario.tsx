import { useId, useMemo, useState } from 'react';
import { Trash2 } from 'lucide-react';
import { BusinessTabs } from './BusinessTabs';
import { t } from './locale';
import { record, text } from './model';
import { editScenarioField, findingName, scenarioDocument, subjects, tasks } from './taskSuite';

const timeExample = '2026-01-15T09:30:00Z';
const views = [
  { id: 'parameters', label: t('参数') },
  { id: 'expected', label: t('预期结果') },
  { id: 'json', label: 'JSON' },
] as const;

export function BusinessScenario({ name, raw, change, remove }: {
  name: string;
  raw: string;
  change: (raw: string) => void;
  remove: () => void;
}) {
  const timeHint = useId();
  const viewId = useId();
  const [view, setView] = useState<typeof views[number]['id']>('parameters');
  const doc = useMemo(() => scenarioDocument(raw), [raw]);
  const scenario = record(doc?.toJS());
  const task = tasks.find(item => item.id === scenario.task);
  const parameters = record(scenario.parameters);
  const expected = record(scenario.expected);
  const subject = text(expected.subject);
  const findings = Array.isArray(expected.findings) ? expected.findings : [];

  function edit(path: string[], value: string) {
    if (doc) change(editScenarioField(raw, doc, path, value));
  }

  return <section className="scenario-editor scenario-with-tabs">
    <header className="scenario-editor-heading">
      <div><h3>{name}</h3>{task && <p className="field-help">{task.name}</p>}</div>
      <button type="button" className="icon-button scenario-remove" aria-label={t('移除此场景')} title={t('移除此场景')} onClick={remove}>
        <Trash2 size={17} aria-hidden="true" />
      </button>
    </header>
    <BusinessTabs id={viewId} label={t('场景详情')} selected={view} choices={views} change={setView} />
    <div className="form-fields scenario-content" id={viewId + '-panel'} role="tabpanel" aria-labelledby={viewId + '-' + view}>
    {view === 'json' ? <label>{t('参数与预期证据（JSON）')}<textarea className="mono business-json" rows={16} spellCheck={false} value={raw}
      onChange={event => change(event.target.value)} /></label>
    : !doc || !task ? <p className="notice error" role="status">{t('此场景无法用表单编辑。请检查场景 JSON 和任务标识。')}</p>
    : view === 'parameters' ? <>
      <div className="scenario-fields">
        <label>{t('场景标识')}<input required value={text(scenario.id)} onChange={event => edit(['id'], event.target.value)} /></label>
        <label>{t('租户标识')}<input required value={text(scenario.tenant_id)} onChange={event => edit(['tenant_id'], event.target.value)} /></label>
      </div>
      <fieldset className="form-fields scenario-parameters" disabled={!scenario.parameters || typeof scenario.parameters !== 'object' || Array.isArray(scenario.parameters)}>
        <legend>{t('运行参数')}</legend>
        <div className="scenario-fields">
          {task.parameters.map(parameter => {
            const time = 'time' in parameter && parameter.time;
            return <label key={parameter.key}>{parameter.label}<input required value={text(parameters[parameter.key])}
              placeholder={time ? timeExample : undefined} aria-describedby={time ? timeHint : undefined}
              onChange={event => edit(['parameters', parameter.key], event.target.value)} /></label>;
          })}
        </div>
        <p id={timeHint} className="field-help">{t('使用含时区的 RFC3339 时间，例如 {0}。', timeExample)}</p>
      </fieldset>
    </> : <>
        <h4>{t('预期发现数 · {0}', findings.length)}</h4>
        <label>{t('主体状态')}<select value={subject} disabled={!scenario.expected || typeof scenario.expected !== 'object' || Array.isArray(scenario.expected)}
          onChange={event => edit(['expected', 'subject'], event.target.value)}>
          {!Object.hasOwn(subjects, subject) && <option value={subject} disabled>{t('选择主体状态')}</option>}
          {Object.entries(subjects).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
        </select></label>
        {expected.anchor != null && <div className="form-fields"><h5>{t('主体证据')}</h5><ScenarioEvidence value={expected.anchor} /></div>}
        {findings.length > 0 && <ul className="scenario-findings">
          {findings.map((item, index) => {
            const finding = record(item);
            const evidence = Array.isArray(finding.evidence) ? finding.evidence : [];
            return <li key={index}><details className="form-disclosure">
              <summary>{findingName(text(finding.kind)) || t('发现 {0}', index + 1)} <span className="muted small">{t('证据数 · {0}', evidence.length)}</span></summary>
              <dl className="scenario-evidence">{Object.entries(record(finding.values)).map(([key, value]) => <div key={key}>
                <dt>{key}</dt><dd>{text(value) || '—'}</dd>
              </div>)}</dl>
              {evidence.map((value, position) => <ScenarioEvidence key={position} value={value} />)}
            </details></li>;
          })}
        </ul>}
    </>}
    </div>
  </section>;
}

function ScenarioEvidence({ value }: { value: unknown }) {
  const evidence = record(value);
  const key = [text(evidence.tenant_id), text(evidence.record_id)].filter(Boolean).join(' / ');
  const fields = [
    [t('证据标签'), evidence.label], [t('来源系统'), evidence.source_system], [t('记录键'), key],
    [t('观测时间（ms）'), evidence.observed_at_ms], [t('来源版本'), evidence.source_version],
  ];
  return <dl className="scenario-evidence">
    {fields.map(([label, field]) => <div key={String(label)}><dt>{String(label)}</dt><dd>
      {typeof field === 'bigint' ? field.toString() : text(field) || '—'}
    </dd></div>)}
  </dl>;
}
