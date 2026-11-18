import { t } from './locale.ts';
import { useId, useState } from 'react';
import { Braces, Plus, X } from 'lucide-react';
import { configText, connectorFields, integerInput, setOptional, type ConnectorField } from './connectors';
import { ListField, type Edit } from './Inspector';
import { record, strings, text } from './model';
import { NameDialog } from './ui';
import { TextTables } from './TextTables';
import { StorageConfig } from './StorageConfig';

export function SourceConfig({ type, item, path, edit, openYaml, form, modelId }: {
  type: string;
  item: Record<string, unknown>;
  path: string[];
  edit: Edit;
  openYaml?: () => void;
  form?: string;
  modelId?: string;
}) {
  const [adding, setAdding] = useState(false);
  const [invalid, setInvalid] = useState<Record<string, boolean>>({});
  const fieldId = useId();
  if (type === 'object_storage') return <StorageConfig item={item} path={path} edit={edit} form={form} modelId={modelId} />;
  if (type === 'csv' || type === 'jsonl') {
    return <TextTables type={type} item={item} path={path} edit={edit} form={form} />;
  }
  if (type === 'parquet') {
    const tables = record(item.tables);
    return <>
      <div className="field-title">{t('表映射')}<button type="button" className="icon-button" aria-label={t('添加 Parquet 表')} onClick={() => setAdding(true)}><Plus size={17} /></button></div>
      {Object.entries(tables).map(([table, paths]) => <div className="parquet-table" key={table}>
        <div className="field-title"><code>{table}</code><button type="button" className="icon-button" aria-label={t('移除表 {0}', table)}
          onClick={() => edit(doc => doc.deleteIn([...path, 'tables', table]))}><X size={16} /></button></div>
        <ListField label={t('文件路径')} form={form} placeholder={t('文件路径或 URL')} items={strings(paths)} change={next => edit(doc => doc.setIn([...path, 'tables', table], next))} />
      </div>)}
      {adding && <NameDialog title={t('添加 Parquet 表')} label={t('表名')} placeholder="schema.table" action={t('添加')} close={() => setAdding(false)} submit={name => {
        if (Object.hasOwn(tables, name)) throw new Error(t('表映射已存在'));
        edit(doc => doc.setIn([...path, 'tables', name], ['']));
      }} />}
    </>;
  }
  if (!Object.hasOwn(connectorFields, type)) {
    return <button type="button" className="button" onClick={openYaml}><Braces size={16} />{t('查看 YAML')}</button>;
  }
  const fields = connectorFields[type];
  function input(field: ConnectorField) {
    if (field.kind === 'toggle') {
      return <label className="check-label" key={field.key}><input type="checkbox" checked={item[field.key] !== false}
        onChange={event => edit(doc => doc.setIn([...path, field.key], event.target.checked))} />{field.label}</label>;
    }
    const id = fieldId + '-' + field.key;
    const message = field.kind === 'integer' ? t('请输入大于 0 的整数。') : field.key === 'uri' ? t('请输入完整的服务地址，例如 https://example.com。') : t('请填写{0}。', field.label);
    return <div className="source-field" key={field.key}>
      <label htmlFor={id}>{field.label}<span className="field-requirement">{field.optional ? t('选填') : t('必填')}</span></label>
      <input id={id} name={field.key} form={form} required={!!form && !field.optional} className="mono" value={configText(item[field.key])}
      type={field.key === 'uri' ? 'url' : 'text'}
      pattern={field.kind === 'integer' ? '0*[1-9][0-9]*' : !field.optional ? '.*\\S.*' : undefined}
      aria-invalid={!!invalid[field.key]} aria-describedby={[field.hint ? id + '-hint' : '', invalid[field.key] ? id + '-error' : ''].filter(Boolean).join(' ') || undefined}
      onInvalid={() => setInvalid(current => ({ ...current, [field.key]: true }))}
      spellCheck={false} autoCapitalize="none" autoCorrect="off" autoComplete="off"
      inputMode={field.kind === 'integer' ? 'numeric' : undefined}
      placeholder={field.placeholder ?? (field.key.endsWith('_env') ? t('环境变量名') : field.key === 'database' ? 'projects/…/instances/…/databases/…' : undefined)}
      onChange={event => {
        const value = event.target.value;
        if (invalid[field.key]) setInvalid(current => ({ ...current, [field.key]: false }));
        edit(doc => {
          const next = field.kind === 'integer' ? integerInput(value) : value;
          if (field.optional) setOptional(doc, [...path, field.key], next);
          else doc.setIn([...path, field.key], next);
        });
      }} />
      {field.hint && <p id={id + '-hint'} className="muted small">{field.hint}</p>}
      {invalid[field.key] && <p id={id + '-error'} className="inline-error">{message}</p>}
    </div>;
  }

  const delta = type === 'delta';
  const tableGroup = delta ? 'tables' : 'snapshots';
  const entries = record(item[tableGroup]);
  const advanced = fields.filter(field => field.advanced);
  return <>
    <div className="connection-fields">{fields.filter(field => !field.advanced).map(input)}</div>
    {(advanced.length > 0 || delta || type === 'iceberg') && <details className="form-disclosure" open={delta || undefined}>
      <summary>{delta ? t('表映射') : type === 'iceberg' ? t('快照设置') : t('高级连接设置')}</summary>
      <div className="form-fields">
        {advanced.length > 0 && <div className="connection-fields">{advanced.map(input)}</div>}
        {(delta || type === 'iceberg') && <>
          {Object.entries(entries).map(([table, value]) => {
            const entry = [...path, tableGroup, table];
            return <div className="source-table form-fields" key={table}>
              <div className="field-title"><code>{table}</code><button className="icon-button" type="button"
                aria-label={t('移除 {0}', table)} onClick={() => edit(doc => doc.deleteIn(entry))}><X size={16} /></button></div>
              {delta ? <>
                <label>{t('表路径')}<input form={form} required={!!form} className="mono" value={text(record(value).path)}
                  onChange={event => edit(doc => doc.setIn([...entry, 'path'], event.target.value))} /></label>
                <label>{t('表版本')}<input className="mono" inputMode="numeric" placeholder={t('最新版本')} value={configText(record(value).version)}
                  onChange={event => edit(doc => setOptional(doc, [...entry, 'version'], integerInput(event.target.value)))} /></label>
              </> : <label>{t('快照 ID')}<input className="mono" inputMode="numeric" value={configText(value)}
                onChange={event => edit(doc => doc.setIn(entry, integerInput(event.target.value)))} /></label>}
            </div>;
          })}
          <button className="button" type="button" onClick={() => setAdding(true)}><Plus size={16} />{delta ? t('添加表映射') : t('固定表快照')}</button>
        </>}
      </div>
    </details>}
    {adding && <NameDialog title={delta ? t('添加 Delta 表映射') : t('固定 Iceberg 表快照')} label={t('表名')} placeholder="schema.table"
      close={() => setAdding(false)} submit={table => {
        if (Object.hasOwn(entries, table)) throw new Error(t('表已配置'));
        if (type === 'iceberg' && !table.includes('.')) throw new Error(t('请输入包含命名空间的表名'));
        edit(doc => doc.setIn([...path, tableGroup, table], delta ? { path: '' } : 0n));
      }} />}
  </>;
}
