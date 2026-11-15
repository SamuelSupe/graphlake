import { useState } from 'react';
import { Braces, Plus, X } from 'lucide-react';
import { configText, connectorFields, integerInput, setOptional, type ConnectorField } from './connectors';
import type { Edit } from './Inspector';
import { record, text } from './model';
import { NameDialog } from './ui';

export function SourceConfig({ type, item, path, edit, openYaml }: {
  type: string;
  item: Record<string, unknown>;
  path: string[];
  edit: Edit;
  openYaml: () => void;
}) {
  const [adding, setAdding] = useState(false);
  if (!Object.hasOwn(connectorFields, type)) {
    return <button type="button" className="button" onClick={openYaml}><Braces size={16} />查看 YAML</button>;
  }
  const fields = connectorFields[type];
  function input(field: ConnectorField) {
    if (field.kind === 'toggle') {
      return <label className="check-label" key={field.key}><input type="checkbox" checked={item[field.key] !== false}
        onChange={event => edit(doc => doc.setIn([...path, field.key], event.target.checked))} />{field.label}</label>;
    }
    return <label key={field.key}>{field.label}<input className="mono" value={configText(item[field.key])}
      spellCheck={false} autoCapitalize="none" autoComplete="off"
      inputMode={field.kind === 'integer' ? 'numeric' : undefined}
      placeholder={field.key.endsWith('_env') ? '环境变量名' : field.key === 'database' ? 'projects/…/instances/…/databases/…' : undefined}
      onChange={event => {
        const value = event.target.value;
        edit(doc => {
          const next = field.kind === 'integer' ? integerInput(value) : value;
          if (field.optional) setOptional(doc, [...path, field.key], next);
          else doc.setIn([...path, field.key], next);
        });
      }} /></label>;
  }

  const delta = type === 'delta';
  const tableGroup = delta ? 'tables' : 'snapshots';
  const entries = record(item[tableGroup]);
  const advanced = fields.filter(field => field.advanced);
  return <>
    {fields.filter(field => !field.advanced).map(input)}
    {(advanced.length > 0 || delta || type === 'iceberg') && <details className="form-disclosure" open={delta || undefined}>
      <summary>{delta ? '表映射' : type === 'iceberg' ? '快照设置' : '高级连接设置'}</summary>
      <div className="form-fields">
        {advanced.map(input)}
        {(delta || type === 'iceberg') && <>
          {Object.entries(entries).map(([table, value]) => {
            const entry = [...path, tableGroup, table];
            return <div className="source-table form-fields" key={table}>
              <div className="field-title"><code>{table}</code><button className="icon-button" type="button"
                aria-label={'移除 ' + table} onClick={() => edit(doc => doc.deleteIn(entry))}><X size={16} /></button></div>
              {delta ? <>
                <label>表路径<input className="mono" value={text(record(value).path)}
                  onChange={event => edit(doc => doc.setIn([...entry, 'path'], event.target.value))} /></label>
                <label>表版本<input className="mono" inputMode="numeric" placeholder="最新版本" value={configText(record(value).version)}
                  onChange={event => edit(doc => setOptional(doc, [...entry, 'version'], integerInput(event.target.value)))} /></label>
              </> : <label>快照 ID<input className="mono" inputMode="numeric" value={configText(value)}
                onChange={event => edit(doc => doc.setIn(entry, integerInput(event.target.value)))} /></label>}
            </div>;
          })}
          <button className="button" type="button" onClick={() => setAdding(true)}><Plus size={16} />{delta ? '添加表映射' : '固定表快照'}</button>
        </>}
      </div>
    </details>}
    {adding && <NameDialog title={delta ? '添加 Delta 表映射' : '固定 Iceberg 表快照'} label="表名" placeholder="schema.table"
      close={() => setAdding(false)} submit={table => {
        if (Object.hasOwn(entries, table)) throw new Error('表已配置');
        if (type === 'iceberg' && !table.includes('.')) throw new Error('请输入包含命名空间的表名');
        edit(doc => doc.setIn([...path, tableGroup, table], delta ? { path: '' } : 0n));
      }} />}
  </>;
}
