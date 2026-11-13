import { useState } from 'react';
import { ArrowDown, ArrowUp, Plus, X } from 'lucide-react';
import { isScalar, isSeq, parseDocument } from 'yaml';
import type { Edit } from './Inspector';
import { appendMappingInput, record, text, type Model } from './model';
import { SourceBrowser, type TableSelection } from './SourceBrowser';
import type { Column } from './api';
import { MappingColumn, mappingControl } from './MappingColumn';

const operations = [['eq', '='], ['ne', '≠'], ['lt', '<'], ['le', '≤'], ['gt', '>'], ['ge', '≥'], ['is_null', '为空'], ['is_not_null', '非空']];
const literal = (value: unknown): string => typeof value === 'bigint' ? value.toString() : JSON.stringify(value) ?? 'null';

function FilterValue({ name, value, save }: { name: string; value: unknown; save: (value: unknown) => void }) {
  const before = literal(value);
  const [input, setInput] = useState(before);
  const [invalid, setInvalid] = useState(false);
  return <label>值（YAML 标量）<input className="mono" aria-label={name} value={input} aria-invalid={invalid}
    onChange={event => { setInput(event.target.value); setInvalid(false); }} onBlur={() => {
      if (input === before) return;
      const parsed = parseDocument(input, { intAsBigInt: true });
      if (parsed.errors.length || !isScalar(parsed.contents)) { setInvalid(true); return; }
      save(parsed.contents.value);
    }} />{invalid && <small className="inline-error">请使用数字、布尔值、null 或带引号的字符串。</small>}</label>;
}

export function MappingInputs({ id, yaml, model, item, path, logical, edit, getColumns, onColumns, invalid }: {
  id: string; yaml: string; model: Model; item: Record<string, unknown>; path: string[]; logical: string[];
  edit: Edit; getColumns: (table: string) => Column[] | undefined; onColumns: (columns: TableSelection) => void;
  invalid: ReadonlySet<string>;
}) {
  const inputs = Array.isArray(item.inputs) ? item.inputs.map(record) : [];
  const [browse, setBrowse] = useState<number>();
  const inputPath = [...path, 'inputs'];
  const set = (at: (string | number)[], value: unknown) => edit(doc => doc.setIn(at, value));
  function move(index: number, to: number) {
    edit(doc => {
      const seq = doc.getIn(inputPath, true);
      if (!isSeq(seq)) return;
      [seq.items[index], seq.items[to]] = [seq.items[to], seq.items[index]];
    });
  }
  const selection = browse === undefined ? undefined : text(browse < 0 ? item.source : inputs[browse]?.table);
  return <>
    {!inputs.length ? <div className="mapping-source">
      <label>来源表<input className="mono" value={text(item.source)} {...mappingControl(invalid, id, 'table')} onChange={event => set([...path, 'source'], event.target.value)}
        placeholder="source.schema.table" list={id + '-known-tables'} /></label>
      <button className="button" onClick={() => setBrowse(-1)}>浏览表与字段</button>
    </div> : <>
      <label>重复键处理<select value={text(item.on_conflict) || 'error'} onChange={event => set([...path, 'on_conflict'], event.target.value)}>
        <option value="error">报错</option><option value="prefer_first">保留前一来源</option>
      </select></label>
      {inputs.map((input, i) => {
        const here = [...inputPath, i];
        const active = getColumns(text(input.table));
        const maps = record(input.columns);
        const casts = record(input.casts);
        const fields = [...new Set([...logical, ...Object.keys(maps), ...Object.keys(casts)])].filter(Boolean);
        const filters = Array.isArray(input.where) ? input.where.map(record) : [];
        return <details className="mapping-card" key={i} open={i === 0 || undefined}>
          <summary><strong>来源 {i + 1}</strong> <code>{text(input.table) || '未选择'}</code></summary>
          <div className="mapping-source">
            <label>来源 {i + 1} 表<input className="mono" value={text(input.table)} {...mappingControl(invalid, id, 'table', i)} onChange={event => set([...here, 'table'], event.target.value)} list={id + '-known-tables'} /></label>
            <button className="button" onClick={() => setBrowse(i)}>浏览表与字段</button>
          </div>
          <div className="mapping-order">
            <button className="icon-button" aria-label={'上移来源 ' + (i + 1)} disabled={i === 0} onClick={() => move(i, i - 1)}><ArrowUp size={15} /></button>
            <button className="icon-button" aria-label={'下移来源 ' + (i + 1)} disabled={i === inputs.length - 1} onClick={() => move(i, i + 1)}><ArrowDown size={15} /></button>
            <button className="icon-button" aria-label={'移除来源 ' + (i + 1)} disabled={inputs.length < 2} onClick={() => {
              edit(doc => {
                doc.deleteIn(here);
                if (inputs.length === 2 && item.on_conflict === 'prefer_first') doc.deleteIn([...path, 'on_conflict']);
              });
            }}><X size={16} /></button>
          </div>
          <div className="field-title">逻辑字段 → 来源列</div>
          {!fields.length && <p className="muted small">配置业务键、属性或端点后可为各来源映射字段。</p>}
          {fields.map(field => <div className="mapping-field" key={field}>
            <label><code>{field}</code><MappingColumn label={'来源 ' + (i + 1) + ' 列 ' + field} placeholder={'默认：' + field}
              value={text(maps[field])} columns={active} location={mappingControl(invalid, id, 'column', i, field)} change={value => {
                edit(doc => { if (value) doc.setIn([...here, 'columns', field], value); else if (doc.hasIn([...here, 'columns', field])) doc.deleteIn([...here, 'columns', field]); });
              }} /></label>
            <label>转换类型<input aria-label={'来源 ' + (i + 1) + ' 转换 ' + field} placeholder="原类型" className="mono"
              value={text(casts[field])} onChange={event => {
                const value = event.target.value;
                edit(doc => { if (value) doc.setIn([...here, 'casts', field], value); else if (doc.hasIn([...here, 'casts', field])) doc.deleteIn([...here, 'casts', field]); });
              }} /></label>
          </div>)}
          <div className="field-title">行过滤<button className="icon-button" aria-label={'来源 ' + (i + 1) + ' 添加行过滤'} onClick={() => {
            edit(doc => {
              const rule = { column: '', op: 'eq', value: null };
              if (filters.length) doc.addIn([...here, 'where'], doc.createNode(rule)); else doc.setIn([...here, 'where'], [rule]);
            });
          }}><Plus size={16} /></button></div>
          {filters.map((filter, j) => <div className="mapping-filter" key={j}>
            <label>来源列<MappingColumn label={'过滤列 ' + (j + 1)} columns={active} value={text(filter.column)}
              location={mappingControl(invalid, id, 'filter', i, j)} change={value => set([...here, 'where', j, 'column'], value)} /></label>
            <label>条件<select value={text(filter.op)} onChange={event => set([...here, 'where', j, 'op'], event.target.value)}>
              {operations.map(([op, label]) => <option key={op} value={op}>{label}</option>)}
            </select></label>
            {!['is_null', 'is_not_null'].includes(text(filter.op)) && <FilterValue key={literal(filter.value)} name={'过滤值 ' + (j + 1)}
              value={filter.value} save={value => set([...here, 'where', j, 'value'], value)} />}
            <button className="icon-button" aria-label={'移除过滤 ' + (j + 1)} onClick={() => edit(doc => { doc.deleteIn([...here, 'where', j]); })}><X size={15} /></button>
          </div>)}
        </details>;
      })}
    </>}
    <button className="button" onClick={() => edit(doc => appendMappingInput(doc, path))}><Plus size={16} />添加来源表</button>
    {browse !== undefined && <SourceBrowser id={id} yaml={yaml} model={model} current={selection} close={() => setBrowse(undefined)}
      choose={picked => {
        if (browse < 0) {
          if (picked.table !== item.source) set([...path, 'source'], picked.table);
        } else {
          if (picked.table !== inputs[browse]?.table) set([...inputPath, browse, 'table'], picked.table);
        }
        onColumns(picked);
      }} />}
  </>;
}
