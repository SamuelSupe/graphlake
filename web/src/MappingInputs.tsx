import { t } from './locale.ts';
import { useState } from 'react';
import { ArrowDown, ArrowUp, Plus, X } from 'lucide-react';
import { isScalar, isSeq, parseDocument } from 'yaml';
import type { Edit } from './Inspector';
import { appendMappingInput, record, text, type Model } from './model';
import { SourceBrowser, type TableSelection } from './SourceBrowser';
import type { Column } from './api';
import { MappingColumn, mappingControl } from './MappingColumn';

import { comparisons, nullCheck } from './scopeOptions';
const literal = (value: unknown): string => typeof value === 'bigint' ? value.toString() : JSON.stringify(value) ?? 'null';

export function ScalarField({ name, value, save, optional = false }: { name: string; value: unknown; save: (value: unknown) => void; optional?: boolean }) {
  const before = optional && value === undefined ? '' : literal(value);
  const [input, setInput] = useState(before);
  const [invalid, setInvalid] = useState(false);
  return <label>{name}<input className="mono" value={input} aria-invalid={invalid}
    onChange={event => { setInput(event.target.value); setInvalid(false); }} onBlur={() => {
      if (input === before) return;
      if (optional && !input.trim()) { save(undefined); return; }
      const parsed = parseDocument(input, { intAsBigInt: true });
      if (parsed.errors.length || !isScalar(parsed.contents)) { setInvalid(true); return; }
      const value = parsed.contents.value;
      if (typeof value === 'number' && !Number.isFinite(value)) { setInvalid(true); return; }
      save(value);
    }} />{invalid && <small className="inline-error">{t('请使用数字、布尔值、null 或带引号的字符串。')}</small>}</label>;
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
      <label>{t('来源表')}<input className="mono" value={text(item.source)} {...mappingControl(invalid, id, 'table')} onChange={event => set([...path, 'source'], event.target.value)}
        placeholder="source.schema.table" list={id + '-known-tables'} /></label>
      <button className="button" onClick={() => setBrowse(-1)}>{t('浏览表与字段')}</button>
    </div> : <>
      <label>{t('重复键处理')}<select value={text(item.on_conflict) || 'error'} onChange={event => set([...path, 'on_conflict'], event.target.value)}>
        <option value="error">{t('报错')}</option><option value="prefer_first">{t('保留前一来源')}</option>
      </select></label>
      {inputs.map((input, i) => {
        const here = [...inputPath, i];
        const active = getColumns(text(input.table));
        const maps = record(input.columns);
        const casts = record(input.casts);
        const fields = [...new Set([...logical, ...Object.keys(maps), ...Object.keys(casts)])].filter(Boolean);
        const filters = Array.isArray(input.where) ? input.where.map(record) : [];
        return <details className="mapping-card" key={i} open={i === 0 || undefined}>
          <summary><strong>{t('来源 {0}', i + 1)}</strong> <code>{text(input.table) || t('未选择')}</code></summary>
          <div className="mapping-source">
            <label>{t('来源 {0} 表', i + 1)}<input className="mono" value={text(input.table)} {...mappingControl(invalid, id, 'table', i)} onChange={event => set([...here, 'table'], event.target.value)} list={id + '-known-tables'} /></label>
            <button className="button" onClick={() => setBrowse(i)}>{t('浏览表与字段')}</button>
          </div>
          <div className="mapping-order">
            <button className="icon-button" aria-label={t('上移来源 {0}', (i + 1))} disabled={i === 0} onClick={() => move(i, i - 1)}><ArrowUp size={15} /></button>
            <button className="icon-button" aria-label={t('下移来源 {0}', (i + 1))} disabled={i === inputs.length - 1} onClick={() => move(i, i + 1)}><ArrowDown size={15} /></button>
            <button className="icon-button" aria-label={t('移除来源 {0}', (i + 1))} disabled={inputs.length < 2} onClick={() => {
              edit(doc => {
                doc.deleteIn(here);
                if (inputs.length === 2 && item.on_conflict === 'prefer_first') doc.deleteIn([...path, 'on_conflict']);
              });
            }}><X size={16} /></button>
          </div>
          <div className="field-title">{t('逻辑字段 → 来源列')}</div>
          {!fields.length && <p className="muted small">{t('配置业务键、属性或端点后可为各来源映射字段。')}</p>}
          {fields.map(field => <div className="mapping-field" key={field}>
            <label><code>{field}</code><MappingColumn label={t('来源 {0} 列 {1}', (i + 1), field)} placeholder={t('默认：{0}', field)}
              value={text(maps[field])} columns={active} location={mappingControl(invalid, id, 'column', i, field)} change={value => {
                edit(doc => { if (value) doc.setIn([...here, 'columns', field], value); else if (doc.hasIn([...here, 'columns', field])) doc.deleteIn([...here, 'columns', field]); });
              }} /></label>
            <label>{t('转换类型')}<input aria-label={t('来源 {0} 转换 {1}', (i + 1), field)} placeholder={t('原类型')} className="mono"
              value={text(casts[field])} onChange={event => {
                const value = event.target.value;
                edit(doc => { if (value) doc.setIn([...here, 'casts', field], value); else if (doc.hasIn([...here, 'casts', field])) doc.deleteIn([...here, 'casts', field]); });
              }} /></label>
          </div>)}
          <div className="field-title">{t('行过滤')}<button className="icon-button" aria-label={t('来源 {0} 添加行过滤', (i + 1))} onClick={() => {
            edit(doc => {
              const rule = { column: '', op: 'eq', value: null };
              if (filters.length) doc.addIn([...here, 'where'], doc.createNode(rule)); else doc.setIn([...here, 'where'], [rule]);
            });
          }}><Plus size={16} /></button></div>
          {filters.map((filter, j) => <div className="mapping-filter" key={j}>
            <label>{t('来源列')}<MappingColumn label={t('过滤列 {0}', (j + 1))} columns={active} value={text(filter.column)}
              location={mappingControl(invalid, id, 'filter', i, j)} change={value => set([...here, 'where', j, 'column'], value)} /></label>
            <label>{t('条件')}<select value={text(filter.op)} onChange={event => {
              const op = event.target.value;
              edit(doc => {
                doc.setIn([...here, 'where', j, 'op'], op);
                if (nullCheck(op)) doc.setIn([...here, 'where', j, 'value'], null);
              });
            }}>
              {comparisons.map(([op, label]) => <option key={op} value={op}>{label}</option>)}
            </select></label>
            {!nullCheck(text(filter.op)) && <ScalarField key={literal(filter.value)} name={t('过滤值 {0}', (j + 1))}
              value={filter.value} save={value => set([...here, 'where', j, 'value'], value)} />}
            <button className="icon-button" aria-label={t('移除过滤 {0}', (j + 1))} onClick={() => edit(doc => { doc.deleteIn([...here, 'where', j]); })}><X size={15} /></button>
          </div>)}
        </details>;
      })}
    </>}
    <button className="button" onClick={() => edit(doc => appendMappingInput(doc, path))}><Plus size={16} />{t('添加来源表')}</button>
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
