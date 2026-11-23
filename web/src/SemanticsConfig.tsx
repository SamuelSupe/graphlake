import { t } from './locale';
import { useState } from 'react';
import { isMap } from 'yaml';
import { setOptional } from './connectors';
import type { Edit } from './Inspector';
import { mappingControl } from './MappingColumn';
import { record, strings, text } from './model';

const relations = [
  ['unspecified', t('未分类')],
  ['identity_link', t('身份归属')],
  ['correlation', t('关联线索')],
  ['entitlement', t('授权记录')],
  ['observed_use', t('实际使用记录')],
  ['custom', t('自定义')],
];

export function SemanticsConfig({ id, item, path, edit, invalid, edge }: {
  id: string;
  item: Record<string, unknown>;
  path: string[];
  edit: Edit;
  invalid: ReadonlySet<string>;
  edge: boolean;
}) {
  const semantics = record(item.semantics);
  const relation = record(semantics.relation);
  const temporal = record(semantics.temporal);
  const properties = record(semantics.properties);
  const evidence = record(semantics.evidence);
  const root = [...path, 'semantics'];
  const columns = [...new Set([...strings(item.key), ...strings(item.properties)].filter(Boolean))];
  const [selected, setSelected] = useState('');
  const propertyNames = [...new Set([...columns, ...Object.keys(properties)])];
  const field = propertyNames.includes(selected) ? selected : propertyNames[0];
  const property = record(field ? properties[field] : undefined);
  const recordKey = strings(evidence.record_key);

  function set(keys: string[], value: string | string[]) {
    edit(doc => {
      setOptional(doc, [...root, ...keys], value);
      for (let depth = keys.length - 1; depth >= 0; depth--) {
        const parent = [...root, ...keys.slice(0, depth)];
        const node = doc.getIn(parent, true);
        if (isMap(node) && !node.items.length) doc.deleteIn(parent);
      }
    });
  }

  return <details className="form-disclosure">
    <summary {...mappingControl(invalid, id, 'semantics')}>{t('业务语义')}</summary>
    <div className="form-fields">
      <label>{t('业务说明')}<textarea rows={2} value={text(semantics.description)}
        onChange={event => set(['description'], event.target.value)} /></label>
      {edge && <fieldset className="form-fields mapping-options">
        <legend>{t('关系含义')}</legend>
        <label>{t('关系类别')}<select value={text(relation.kind) || 'unspecified'}
          onChange={event => set(['relation', 'kind'], event.target.value === 'unspecified' ? '' : event.target.value)}>
          {relations.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
        </select></label>
        <label>{t('从起点到终点的含义')}<textarea rows={2} value={text(relation.statement)}
          onChange={event => set(['relation', 'statement'], event.target.value)} /></label>
      </fieldset>}
      {(item.scd2 != null || semantics.temporal != null) &&
        <label>{t('有效期说明')}<textarea rows={2} value={text(temporal.description)}
          onChange={event => set(['temporal', 'description'], event.target.value)} /></label>}
      {field && <fieldset className="form-fields mapping-options">
        <legend>{t('字段说明')}</legend>
        <label>{t('字段')}<select value={field} onChange={event => setSelected(event.target.value)}>
          {propertyNames.map(name => <option key={name} value={name}>{name}{columns.includes(name) ? '' : t('（已移除）')}</option>)}
        </select></label>
        <label>{t('字段含义')}<textarea rows={2} value={text(property.description)}
          onChange={event => set(['properties', field, 'description'], event.target.value)} /></label>
        <label>{t('单位')}<input value={text(property.unit)}
          onChange={event => set(['properties', field, 'unit'], event.target.value)} /></label>
        {!columns.includes(field) && <button type="button" className="button compact"
          onClick={() => set(['properties', field], '')}>{t('移除字段说明')}</button>}
      </fieldset>}
      <details className="form-disclosure" open={semantics.evidence != null || undefined}>
        <summary>{t('证据字段（高级）')}</summary>
        <fieldset className="form-fields mapping-options">
        <legend className="sr-only">{t('证据字段')}</legend>
        {semantics.evidence == null ? <button type="button" className="button compact"
          onClick={() => edit(doc => doc.setIn([...root, 'evidence'], { source_system: '', record_key: [], observed_at: '' }))}>{t('配置证据字段')}</button> : <>
          {([['source_system', t('来源系统字段')], ['observed_at', t('观测时间字段')], ['source_version', t('来源版本字段（可选）')]] as const).map(([key, label]) => {
            const value = text(evidence[key]);
            return <label key={key}>{label}<select value={value} onChange={event => {
              const value = event.target.value;
              if (key === 'source_version') set(['evidence', key], value);
              else edit(doc => doc.setIn([...root, 'evidence', key], value));
            }}>
              <option value="">{t('选择字段')}</option>
              {value && !columns.includes(value) && <option value={value}>{value}{t('（已移除）')}</option>}
              {columns.map(name => <option key={name}>{name}</option>)}
            </select></label>;
          })}
          {recordKey.map((field, index) => <div className="form-fields" key={index}>
            <label>{t('记录键字段 {0}', index + 1)}<select value={field}
              onChange={event => set(['evidence', 'record_key'], recordKey.map((name, i) => i === index ? event.target.value : name))}>
              <option value="">{t('选择字段')}</option>
              {field && !columns.includes(field) && <option value={field}>{field}{t('（已移除）')}</option>}
              {columns.filter(name => name === field || !recordKey.includes(name)).map(name => <option key={name}>{name}</option>)}
            </select></label>
            <button type="button" className="button compact"
              onClick={() => set(['evidence', 'record_key'], recordKey.filter((_, i) => i !== index))}>{t('移除记录键字段 {0}', index + 1)}</button>
          </div>)}
          <button type="button" className="button compact" disabled={recordKey.length >= columns.length || recordKey.includes('')}
            onClick={() => set(['evidence', 'record_key'], [...recordKey, ''])}>{t('添加记录键字段')}</button>
          <button type="button" className="button compact" onClick={() => set(['evidence'], '')}>{t('移除证据配置')}</button>
        </>}
        </fieldset>
      </details>
    </div>
  </details>;
}
