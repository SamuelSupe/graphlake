import { t } from './locale.ts';
import type { Column } from './api';
import { setOptional } from './connectors';
import type { Edit } from './Inspector';
import { MappingColumn, mappingControl } from './MappingColumn';
import { ScalarField } from './MappingInputs';
import { record, temporalFields, text } from './model';

export function TemporalConfig({ id, item, path, columns, edit, invalid }: {
  id: string;
  item: Record<string, unknown>;
  path: string[];
  columns?: Column[];
  edit: Edit;
  invalid: ReadonlySet<string>;
}) {
  const scd2 = record(item.scd2);
  const enabled = item.scd2 != null;
  const temporal = [...path, 'scd2'];
  const saveScalar = (key: string, value: unknown) => edit(doc => {
    const at = [...temporal, key];
    if (value === undefined) {
      if (doc.hasIn(at)) doc.deleteIn(at);
    } else doc.setIn(at, value);
  });
  return <details className="form-disclosure">
    <summary {...mappingControl(invalid, id, 'scd2')} tabIndex={0}>{t('时态与分区')}
      {(enabled || item.unpartitioned === true) && <small>{t('已配置')}</small>}
    </summary>
    <div className="form-fields">
      <label className="check-label"><input type="checkbox" checked={enabled} onChange={event => {
        if (event.target.checked) edit(doc => doc.setIn(temporal, {}));
        else if (!Object.keys(scd2).length || window.confirm(t('移除此标签的历史版本配置？'))) {
          edit(doc => doc.deleteIn(temporal));
        }
      }} />{t('启用历史版本（SCD2）')}</label>
      {enabled && <fieldset className="form-fields mapping-options">
        <legend>{t('历史版本列')}</legend>
        <p className="muted small">{t('填写生效与失效时间列，或使用当前记录标记列。')}</p>
        {temporalFields.map(([field, label]) => <label key={field}>{label}<MappingColumn label={label}
          columns={columns} value={text(scd2[field])} location={mappingControl(invalid, id, 'scd2', field)}
          change={value => edit(doc => setOptional(doc, [...temporal, field], value))} /></label>)}
        {(scd2.current_flag || scd2.current_value !== undefined) && <ScalarField name={t('当前标记值（默认 true）')} optional
          key={'flag-' + String(scd2.current_value)} value={scd2.current_value}
          save={value => saveScalar('current_value', value)} />}
        {(scd2.valid_to || scd2.open_end !== undefined) && <ScalarField name={t('开放结束值（默认 null）')} optional
          key={'end-' + String(scd2.open_end)} value={scd2.open_end}
          save={value => saveScalar('open_end', value)} />}
      </fieldset>}
      <label className="check-label"><input type="checkbox" checked={item.unpartitioned === true} onChange={event => edit(doc => {
        setOptional(doc, [...path, 'unpartitioned'], event.target.checked ? true : '');
      })} />{t('忽略查询分区')}</label>
    </div>
  </details>;
}
