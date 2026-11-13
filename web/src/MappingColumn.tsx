import type { Column } from './api';
import { validationControl } from './model';

export function mappingControl(active: ReadonlySet<string>, model: string, ...parts: (string | number)[]) {
  const key = validationControl(...parts);
  const invalid = active.has(key);
  return { 'data-mapping-control': key, 'aria-invalid': invalid || undefined, 'aria-describedby': invalid ? model + '-model-validation-error' : undefined };
}

export function MappingColumn({ label, value, columns, change, placeholder = '选择来源列', location }: {
  label: string; value: string; columns?: Column[]; change: (value: string) => void; placeholder?: string;
  location?: ReturnType<typeof mappingControl>;
}) {
  if (!columns) return <input className="mono" aria-label={label} value={value} placeholder={placeholder} {...location}
    onChange={event => change(event.target.value)} />;
  return <select className="mono" aria-label={label} value={value} {...location} onChange={event => change(event.target.value)}>
    <option value="">{placeholder}</option>
    {value && !columns.some(column => column.name === value) && <option value={value}>{value}（未在表中）</option>}
    {columns.map(column => <option value={column.name} key={column.name}>{column.name} · {column.data_type}</option>)}
  </select>;
}
