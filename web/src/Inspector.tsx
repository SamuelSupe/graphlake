import { useState } from 'react';
import { ArrowDown, ArrowRight, ArrowUp, Braces, Pencil, Plus, Trash2, X } from 'lucide-react';
import { endpoint, record, rename, sourceLabel, strings, text, type Model, type Selection, type YamlDocument } from './model';
import { NameDialog } from './ui';
import type { Column } from './api';
import { MappingInputs } from './MappingInputs';
import { SourceBrowser, type TableSelection } from './SourceBrowser';
import { MappingColumn, mappingControl } from './MappingColumn';

export type Edit = (change: (doc: YamlDocument) => void) => void;

export function ListField({ label, items, change, reorder = false, columns, controls }: {
  label: string;
  items: string[];
  change: (next: string[], previousIndexes: number[]) => void;
  reorder?: boolean;
  columns?: Column[];
  controls?: (index?: number) => ReturnType<typeof mappingControl>;
}) {
  function move(from: number, to: number) {
    const order = items.map((_, index) => index);
    [order[from], order[to]] = [order[to], order[from]];
    change(order.map(index => items[index]), order);
  }
  return <div className="list-field">
    <div className="field-title">{label}<button className="icon-button" {...controls?.()} aria-label={'添加' + label} onClick={() => change([...items, ''], [...items.keys(), -1])}><Plus size={16} /></button></div>
    {!items.length && <p className="muted small">未配置</p>}
    {items.map((item, index) => <div className="list-field-row" key={index}>
      <MappingColumn label={label + ' ' + (index + 1)} value={item} columns={columns} location={controls?.(index)} change={value => {
        const next = items.slice();
        next[index] = value;
        change(next, [...items.keys()]);
      }} />
      {reorder && items.length > 1 && <>
        <button className="icon-button" aria-label={'上移' + label + ' ' + (index + 1)} disabled={index === 0} onClick={() => move(index, index - 1)}><ArrowUp size={14} /></button>
        <button className="icon-button" aria-label={'下移' + label + ' ' + (index + 1)} disabled={index === items.length - 1} onClick={() => move(index, index + 1)}><ArrowDown size={14} /></button>
      </>}
      <button className="icon-button" aria-label={'移除' + label + ' ' + (index + 1)} onClick={() => {
        const indexes = [...items.keys()].filter(i => i !== index);
        change(indexes.map(i => items[i]), indexes);
      }}><X size={15} /></button>
    </div>)}
  </div>;
}

export function Inspector({ model, selected, edit, select, openYaml, columns, id, yaml, invalid, onRemove, getColumns, rememberColumns }: {
  model: Model;
  id: string;
  yaml: string;
  selected: Selection;
  edit: Edit;
  select: (selection: Selection) => void;
  openYaml: () => void;
  columns?: Column[];
  invalid: ReadonlySet<string>;
  onRemove: () => void;
  getColumns: (table: string) => Column[] | undefined;
  rememberColumns: (selection: TableSelection) => void;
}) {
  const { section, name } = selected;
  const item = model[section][name];
  const [renaming, setRenaming] = useState(false);
  const [addingTable, setAddingTable] = useState(false);
  const kind = section === 'sources' ? '数据源' : section === 'nodes' ? '节点标签' : '边类型';
  const path = [section, name];
  const set = (field: string, value: unknown) => edit(doc => doc.setIn([...path, field], value));
  const source = text(item.type);
  const tables = record(item.tables);
  const [browseSource, setBrowseSource] = useState(false);
  const physical = getColumns(text(item.source));
  const multipleInputs = Array.isArray(item.inputs) && item.inputs.length > 0;
  const logical = [...strings(item.key), ...strings(item.properties), ...endpoint(item.from).columns, ...endpoint(item.to).columns];
  const control = (key: string) => (index?: number) => index === undefined ? mappingControl(invalid, id, key) : mappingControl(invalid, id, key, index);

  function editKeys(keys: string[], previousIndexes: number[]) {
    edit(doc => {
      doc.setIn([...path, 'key'], keys);
      if (section !== 'nodes') return;
      for (const [edgeName, edge] of Object.entries(model.edges)) {
        for (const end of ['from', 'to']) {
          const current = endpoint(edge[end]);
          if (current.node === name) {
            doc.setIn(['edges', edgeName, end, 'columns'], previousIndexes.map(index => current.columns[index] ?? ''));
          }
        }
      }
    });
  }

  function editEndpoint(side: 'from' | 'to', label: string) {
    const current = endpoint(item[side]);
    const keys = strings(model.nodes[current.node]?.key);
    const target = strings(model.nodes[label]?.key);
    edit(doc => {
      doc.setIn([...path, side, 'node'], label);
      doc.setIn([...path, side, 'columns'], target.map(key => current.columns[keys.indexOf(key)] ?? ''));
    });
  }

  return <>
    <div className="inspector-title"><small>{kind}</small>
      <h2>{name}{section !== 'sources' && <button className="icon-button" aria-label={'重命名' + kind} onClick={() => setRenaming(true)}><Pencil size={16} /></button>}</h2>
    </div>
    {section === 'sources' ? <div className="inspector-section">
      <label>连接器<input readOnly value={sourceLabel(item.type)} /></label>
      {source === 'postgres' ? <label>连接串环境变量<input className="mono" value={text(item.dsn_env)} onChange={event => set('dsn_env', event.target.value)} placeholder="GRAPHLAKE_POSTGRES_DSN" /></label>
        : source === 'parquet' ? <>
          <div className="field-title">表映射<button className="icon-button" aria-label="添加 Parquet 表" onClick={() => setAddingTable(true)}><Plus size={17} /></button></div>
          {!Object.keys(tables).length && <p className="muted small">未配置表映射</p>}
          {Object.entries(tables).map(([table, paths]) => <div className="parquet-table" key={table}>
            <div className="field-title"><code>{table}</code><button className="icon-button" aria-label={'移除表 ' + table} onClick={() => edit(doc => { doc.deleteIn([...path, 'tables', table]); })}><X size={16} /></button></div>
            <ListField label="文件路径" items={strings(paths)} change={next => edit(doc => doc.setIn([...path, 'tables', table], next))} />
          </div>)}
        </> : <button className="button" onClick={openYaml}><Braces size={17} />编辑数据源 YAML</button>}
      <button className="button" onClick={() => setBrowseSource(true)}>浏览表与字段</button>
      {browseSource && <SourceBrowser id={id} yaml={yaml} model={model} fixedSource={name} close={() => setBrowseSource(false)} />}
    </div> : <>
      <section className="inspector-section"><h3 tabIndex={-1} {...mappingControl(invalid, id, 'mapping')}>数据映射</h3>
        <MappingInputs id={id} yaml={yaml} model={model} item={item} path={path} logical={logical} edit={edit}
          getColumns={getColumns} onColumns={rememberColumns} invalid={invalid} />
        {physical && !multipleInputs && <p className="muted small">已读取 {physical.length} 列。可在业务键、属性和端点映射中直接选择来源列。</p>}
        <datalist id={id + '-known-tables'}>{Object.entries(model.sources).flatMap(([alias, source]) => Object.keys(record(source.tables)).map(table =>
          <option key={alias + '.' + table} value={alias + '.' + table} />,
        ))}</datalist>
        <ListField label="业务键" items={strings(item.key)} change={editKeys} reorder columns={multipleInputs ? undefined : physical} controls={control('key')} />
      </section>
      {section === 'edges' && <section className="inspector-section"><h3>端点映射</h3>
        {(['from', 'to'] as const).map((side, index) => {
          const end = endpoint(item[side]);
          const keys = strings(model.nodes[end.node]?.key);
          const label = index === 0 ? '起点' : '终点';
          return <div className="endpoint" key={side}>
            <label className="endpoint-select">{label}<select value={end.node} {...control(side)()} onChange={event => editEndpoint(side, event.target.value)}>
              <option value="">选择节点标签</option>{Object.keys(model.nodes).map(node => <option key={node}>{node}</option>)}
              {end.node && !Object.hasOwn(model.nodes, end.node) && <option value={end.node}>{end.node}（缺失）</option>}
            </select></label>
            {keys.length > 0 && <div className="endpoint-fields">
              {keys.map((key, keyIndex) => <div key={keyIndex}>
                <MappingColumn label={label + '映射 ' + (keyIndex + 1)} value={end.columns[keyIndex] ?? ''} placeholder="选择边字段"
                  columns={multipleInputs ? undefined : physical} location={control(side)(keyIndex)} change={value => {
                  const next = keys.map((_, i) => i === keyIndex ? value : end.columns[i] ?? '');
                  edit(doc => doc.setIn([...path, side, 'columns'], next));
                }} />
                <ArrowRight size={16} aria-hidden="true" /><code>{key || '业务键待配置'}</code>
              </div>)}
            </div>}
          </div>;
        })}
      </section>}
      <section className="inspector-section">
        <ListField label="属性" items={strings(item.properties)} change={next => set('properties', next)} columns={multipleInputs ? undefined : physical} controls={control('properties')} />
        {columns && <details className="schema"><summary>已验证的字段类型</summary>
          <dl>{columns.map(column => <div key={column.name}><dt><code>{column.name}</code></dt><dd>{column.data_type}</dd></div>)}</dl>
        </details>}
      </section>
      <section className="inspector-section">
        <button className="button danger" onClick={onRemove} disabled={section === 'nodes' && Object.keys(model.nodes).length <= 1}>
          <Trash2 size={16} />删除{kind}
        </button>
        {section === 'nodes' && Object.keys(model.nodes).length <= 1 && <p className="muted small">图模型至少需要一个节点标签。</p>}
      </section>
    </>}
    {renaming && <NameDialog title={'重命名' + kind} initial={name} close={() => setRenaming(false)} submit={next => {
      if (next === name) return;
      if (Object.hasOwn(model.nodes, next) || Object.hasOwn(model.edges, next)) throw new Error('标签或边类型已存在');
      if (section !== 'sources') edit(doc => rename(doc, model, section, name, next));
      select({ section, name: next });
    }} />}
    {addingTable && <NameDialog title="添加 Parquet 表" close={() => setAddingTable(false)} submit={name => {
      if (Object.hasOwn(tables, name)) throw new Error('表映射已存在');
      edit(doc => doc.setIn([...path, 'tables', name], ['']));
    }} />}
  </>;
}
