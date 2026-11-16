import { t } from './locale.ts';
import { useState } from 'react';
import { ArrowDown, ArrowRight, ArrowUp, Pencil, Plus, Trash2, X } from 'lucide-react';
import { endpoint, record, rename, sourceLabel, sourceUsers, strings, temporalFields, text, type Model, type Selection, type YamlDocument } from './model';
import { NameDialog } from './ui';
import type { Column } from './api';
import { MappingInputs } from './MappingInputs';
import { SourceBrowser, type TableSelection } from './SourceBrowser';
import { MappingColumn, mappingControl } from './MappingColumn';
import { SourceConfig } from './SourceConfig';
import { TemporalConfig } from './TemporalConfig';

export type Edit = (change: (doc: YamlDocument) => void) => void;

export function ListField({ label, items, change, reorder = false, columns, controls, placeholder, form }: {
  label: string;
  items: string[];
  change: (next: string[], previousIndexes: number[]) => void;
  reorder?: boolean;
  columns?: Column[];
  controls?: (index?: number) => ReturnType<typeof mappingControl>;
  placeholder?: string;
  form?: string;
}) {
  function move(from: number, to: number) {
    const order = items.map((_, index) => index);
    [order[from], order[to]] = [order[to], order[from]];
    change(order.map(index => items[index]), order);
  }
  return <div className="list-field">
    <div className="field-title">{label}<button className="icon-button" {...controls?.()} aria-label={t('添加{0}', label)} onClick={() => change([...items, ''], [...items.keys(), -1])}><Plus size={16} /></button></div>
    {items.map((item, index) => <div className="list-field-row" key={index}>
      <MappingColumn form={form} label={label + ' ' + (index + 1)} value={item} columns={columns} placeholder={placeholder} location={controls?.(index)} change={value => {
        const next = items.slice();
        next[index] = value;
        change(next, [...items.keys()]);
      }} />
      {reorder && items.length > 1 && <>
        <button className="icon-button" aria-label={t('上移{0} {1}', label, (index + 1))} disabled={index === 0} onClick={() => move(index, index - 1)}><ArrowUp size={14} /></button>
        <button className="icon-button" aria-label={t('下移{0} {1}', label, (index + 1))} disabled={index === items.length - 1} onClick={() => move(index, index + 1)}><ArrowDown size={14} /></button>
      </>}
      <button className="icon-button" aria-label={t('移除{0} {1}', label, (index + 1))} onClick={() => {
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
  const kind = section === 'sources' ? t('数据源') : section === 'nodes' ? t('节点标签') : t('边类型');
  const path = [section, name];
  const set = (field: string, value: unknown) => edit(doc => doc.setIn([...path, field], value));
  const source = text(item.type);
  const [browseSource, setBrowseSource] = useState(false);
  const physical = getColumns(text(item.source));
  const multipleInputs = Array.isArray(item.inputs) && item.inputs.length > 0;
  const logical = [...strings(item.key), ...strings(item.properties), ...endpoint(item.from).columns, ...endpoint(item.to).columns,
    ...temporalFields.map(([key]) => text(record(item.scd2)[key]))];
  const references = section === 'sources' ? sourceUsers(model, name) : [];
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
    <div className="inspector-title"><small>{section === 'sources' ? sourceLabel(source) : kind}</small>
      <h2>{name}<button className="icon-button" aria-label={t('重命名{0}', kind)} onClick={() => setRenaming(true)}><Pencil size={16} /></button></h2>
    </div>
    {section === 'sources' ? <div className="inspector-section">
      <SourceConfig modelId={id} type={source} item={item} path={path} edit={edit} openYaml={openYaml} />
      <button className="button" onClick={() => setBrowseSource(true)}>{t('浏览表与字段')}</button>
      <details className="form-disclosure"><summary>{t('数据源操作')}</summary><div className="form-fields">
        <button className="button danger" disabled={references.length > 0} onClick={onRemove}><Trash2 size={16} />{t('删除数据源')}</button>
        {!!references.length && <p className="muted small">{t('先替换以下类型的来源表：{0}', references.join(t('、')))}</p>}
      </div></details>
      {browseSource && <SourceBrowser id={id} yaml={yaml} model={model} fixedSource={name} close={() => setBrowseSource(false)} />}
    </div> : <>
      <section className="inspector-section"><h3 tabIndex={-1} {...mappingControl(invalid, id, 'mapping')}>{t('数据映射')}</h3>
        <MappingInputs id={id} yaml={yaml} model={model} item={item} path={path} logical={logical} edit={edit}
          getColumns={getColumns} onColumns={rememberColumns} invalid={invalid} />
        <datalist id={id + '-known-tables'}>{Object.entries(model.sources).flatMap(([alias, source]) => Object.keys(record(source.tables)).map(table =>
          <option key={alias + '.' + table} value={alias + '.' + table} />,
        ))}</datalist>
        <ListField label={t('业务键')} items={strings(item.key)} change={editKeys} reorder columns={multipleInputs ? undefined : physical} controls={control('key')} />
      </section>
      {section === 'edges' && <section className="inspector-section"><h3>{t('端点映射')}</h3>
        {(['from', 'to'] as const).map((side, index) => {
          const end = endpoint(item[side]);
          const keys = strings(model.nodes[end.node]?.key);
          const label = index === 0 ? t('起点') : t('终点');
          return <div className="endpoint" key={side}>
            <label className="endpoint-select">{label}<select value={end.node} {...control(side)()} onChange={event => editEndpoint(side, event.target.value)}>
              <option value="">{t('选择节点标签')}</option>{Object.keys(model.nodes).map(node => <option key={node}>{node}</option>)}
              {end.node && !Object.hasOwn(model.nodes, end.node) && <option value={end.node}>{end.node}{t('（缺失）')}</option>}
            </select></label>
            {keys.length > 0 && <div className="endpoint-fields">
              {keys.map((key, keyIndex) => <div key={keyIndex}>
                <MappingColumn label={t('{0}映射 {1}', label, (keyIndex + 1))} value={end.columns[keyIndex] ?? ''} placeholder={t('选择边字段')}
                  columns={multipleInputs ? undefined : physical} location={control(side)(keyIndex)} change={value => {
                  const next = keys.map((_, i) => i === keyIndex ? value : end.columns[i] ?? '');
                  edit(doc => doc.setIn([...path, side, 'columns'], next));
                }} />
                <ArrowRight size={16} aria-hidden="true" /><code>{key || t('业务键待配置')}</code>
              </div>)}
            </div>}
          </div>;
        })}
      </section>}
      <section className="inspector-section">
        <ListField label={t('属性')} items={strings(item.properties)} change={next => set('properties', next)} columns={multipleInputs ? undefined : physical} controls={control('properties')} />
        {columns && <details className="schema"><summary>{t('已验证的字段类型')}</summary>
          <dl>{columns.map(column => <div key={column.name}><dt><code>{column.name}</code></dt><dd>{column.data_type}</dd></div>)}</dl>
        </details>}
      </section>
      <section className="inspector-section">
        <TemporalConfig id={id} item={item} path={path} columns={multipleInputs ? undefined : physical} edit={edit} invalid={invalid} />
        <button className="button danger" onClick={onRemove} disabled={section === 'nodes' && Object.keys(model.nodes).length <= 1}>
          <Trash2 size={16} />{t('删除{0}', kind)}
        </button>
        {section === 'nodes' && Object.keys(model.nodes).length <= 1 && <p className="muted small">{t('图模型至少需要一个节点标签。')}</p>}
      </section>
    </>}
    {renaming && <NameDialog title={t('重命名{0}', kind)} action={t('重命名')} initial={name} close={() => setRenaming(false)} submit={next => {
      if (next === name) return;
      if (section === 'sources') {
        if (next.includes('.') || next.includes('\0')) throw new Error(t('数据源名称不能包含点或空字符'));
        if (Object.hasOwn(model.sources, next)) throw new Error(t('数据源已存在'));
      } else if (Object.hasOwn(model.nodes, next) || Object.hasOwn(model.edges, next)) {
        throw new Error(t('标签或边类型已存在'));
      }
      edit(doc => rename(doc, model, section, name, next));
      select({ section, name: next });
    }} />}

  </>;
}
