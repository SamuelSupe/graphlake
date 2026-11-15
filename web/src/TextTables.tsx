import { useId, useState } from 'react';
import { Plus, X } from 'lucide-react';
import { ListField, type Edit } from './Inspector';
import { record, sourceLabel, strings, text } from './model';
import { NameDialog } from './ui';

export function TextTables({ type, item, path, edit, form, objectKeys = false }: {
  type: string; item: Record<string, unknown>; path: string[]; edit: Edit; form?: string; objectKeys?: boolean;
}) {
  const [adding, setAdding] = useState(false);
  const id = useId();
  const tables = record(item.tables);
  return <>
    <p className="muted small" id={id + '-help'}>{type === 'parquet' ? '从 Parquet 元数据读取列名和类型。' : type === 'csv'
      ? 'UTF-8、逗号分隔，首行为表头。按文件中的顺序声明所有列及类型。'
      : 'UTF-8，每条记录为一个 JSON 对象。声明需要读取的顶层列及类型。'}</p>
    <div className="field-title">表映射<button type="button" className="icon-button"
      aria-label={'添加 ' + sourceLabel(type) + ' 表'} onClick={() => setAdding(true)}><Plus size={17} /></button></div>
    {Object.entries(tables).map(([name, value], tableIndex) => {
      const table = record(value);
      const entry = [...path, 'tables', name];
      const columns = Array.isArray(table.columns) ? table.columns.map(record) : [];
      return <div className="source-table form-fields" key={name}>
        <div className="field-title"><code>{name}</code><button type="button" className="icon-button"
          aria-label={'移除表 ' + name} onClick={() => edit(doc => doc.deleteIn(entry))}><X size={16} /></button></div>
        <ListField form={form} label={objectKeys ? "对象路径" : "文件路径"} placeholder={objectKeys ? "例如 exports/accounts.parquet" : "文件路径或 URL"} items={strings(table.files)} change={files => edit(doc => doc.setIn([...entry, 'files'], files))} />
        {type !== 'parquet' && columns.map((column, index) => {
          const columnPath = [...entry, 'columns', index];
          const columnId = `${id}-${tableIndex}-${index}`;
          return <div className="source-table form-fields" key={index}>
            <div className="field-title">列 {index + 1}<button type="button" className="icon-button"
              aria-label={`移除 ${name} 列 ${index + 1}`} onClick={() => edit(doc => doc.deleteIn(columnPath))}><X size={16} /></button></div>
            <label htmlFor={columnId + '-name'}>列名<input form={form} required={!!form} id={columnId + '-name'} className="mono"
              spellCheck={false} autoCapitalize="none" autoCorrect="off" value={text(column.name)}
              aria-describedby={id + '-help'} onChange={event => edit(doc => doc.setIn([...columnPath, 'name'], event.target.value))} /></label>
            <label htmlFor={columnId + '-type'}>Arrow 类型<input form={form} required={!!form} id={columnId + '-type'} className="mono"
              list={id + '-types'} value={text(column.data_type)} spellCheck={false} autoCapitalize="none" autoCorrect="off"
              onChange={event => edit(doc => doc.setIn([...columnPath, 'data_type'], event.target.value))} /></label>
            <label className="check-label" htmlFor={columnId + '-null'}><input id={columnId + '-null'} type="checkbox"
              checked={column.nullable !== false} onChange={event => edit(doc => doc.setIn([...columnPath, 'nullable'], event.target.checked))} />允许空值</label>
          </div>;
        })}
        {type !== 'parquet' && <button type="button" className="button" onClick={() => edit(doc => doc.setIn([...entry, 'columns', columns.length],
          { name: '', data_type: 'Utf8', nullable: true }))}><Plus size={16} />添加列</button>}
      </div>;
    })}
    <datalist id={id + '-types'}>{['Utf8', 'Int64', 'UInt64', 'Float64', 'Boolean', 'Date32', 'Timestamp(Microsecond, None)', 'Decimal128(18, 2)']
      .map(kind => <option key={kind} value={kind} />)}</datalist>
    {adding && <NameDialog title={'添加 ' + sourceLabel(type) + ' 表'} label="表名" action="添加"
      close={() => setAdding(false)} submit={name => {
        if (Object.hasOwn(tables, name)) throw new Error('表映射已存在');
        edit(doc => doc.setIn([...path, 'tables', name], { files: [''], ...(type === 'parquet' ? {} : { columns: [{ name: 'id', data_type: 'Int64', nullable: false }] }) }));
      }} />}
  </>;
}
