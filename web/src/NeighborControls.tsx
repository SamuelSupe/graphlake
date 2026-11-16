import { t } from './locale.ts';
import { useState } from 'react';
import { Expand, Plus, X } from 'lucide-react';
import type { GraphComparison, GraphSchema, NeighborFilter } from './api';

const comparisons: Record<GraphComparison, string> = {
  eq: '=', ne: '≠', lt: '<', le: '≤', gt: '>', ge: '≥', is_null: t('为 null'), is_not_null: t('非 null'),
};
const needsValue = (op: GraphComparison) => op !== 'is_null' && op !== 'is_not_null';

export function NeighborControls({ label, schema, disabled, busy, activeFilter, next, expand, loadAll }: {
  label: string; schema?: GraphSchema; disabled: boolean; busy: boolean;
  activeFilter?: NeighborFilter; next?: 'more' | 'pending' | 'done'; expand: (filter: NeighborFilter, next: boolean) => void;
  loadAll?: (filter: NeighborFilter) => void;
}) {
  const [filter, setFilter] = useState<NeighborFilter>(() => activeFilter ?? { direction: 'both', properties: [] });
  const edges = Object.entries(schema?.edges ?? {}).filter(([, edge]) =>
    (filter.direction !== 'in' && edge.from.node === label) || (filter.direction !== 'out' && edge.to.node === label));
  const fields = filter.edge_label ? schema?.edges[filter.edge_label]?.properties ?? [] : [];
  const normalized = { ...filter, properties: filter.properties.map(term => needsValue(term.op) ? term : { column: term.column, op: term.op }) };
  const continuing = JSON.stringify(normalized) === JSON.stringify(activeFilter) && next && next !== 'done';

  function update(index: number, change: Partial<NeighborFilter['properties'][number]>) {
    setFilter({ ...filter, properties: filter.properties.map((term, i) => i === index ? { ...term, ...change } : term) });
  }

  return <div className="neighbor-controls">
    <details>
      <summary>{t('展开筛选')}</summary>
      <fieldset disabled={disabled}>
        <label>{t('方向')}<select value={filter.direction} onChange={event => {
          const direction = event.target.value as NeighborFilter['direction'];
          const edge = filter.edge_label ? schema?.edges[filter.edge_label] : undefined;
          const matches = edge && (direction !== 'in' && edge.from.node === label || direction !== 'out' && edge.to.node === label);
          setFilter(matches ? { ...filter, direction } : { direction, properties: [] });
        }}><option value="both">{t('双向')}</option><option value="out">{t('出边')}</option><option value="in">{t('入边')}</option></select></label>
        <label>{t('边类型')}<select value={filter.edge_label ?? ''} onChange={event =>
          setFilter({ ...filter, edge_label: event.target.value || undefined, properties: [] })}>
          <option value="">{t('全部边类型')}</option>{edges.map(([name]) => <option key={name}>{name}</option>)}
        </select></label>
        {filter.properties.map((term, index) => <div className="neighbor-condition" key={index}>
          <select aria-label={t('属性 {0}', (index + 1))} value={term.column} onChange={event => update(index, { column: event.target.value })}>
            {fields.map(field => <option key={field}>{field}</option>)}
          </select>
          <select aria-label={t('比较 {0}', (index + 1))} value={term.op} onChange={event => update(index, { op: event.target.value as GraphComparison })}>
            {Object.entries(comparisons).map(([value, name]) => <option key={value} value={value}>{name}</option>)}
          </select>
          {needsValue(term.op) && <input aria-label={t('属性值 {0}', (index + 1))} value={term.value ?? ''} maxLength={4096}
            onChange={event => update(index, { value: event.target.value })} />}
          <button className="icon-button" aria-label={t('移除条件 {0}', (index + 1))} onClick={() =>
            setFilter({ ...filter, properties: filter.properties.filter((_, i) => i !== index) })}><X size={14} /></button>
        </div>)}
        <button className="button compact" disabled={!fields.length || filter.properties.length === 8} onClick={() =>
          setFilter({ ...filter, properties: [...filter.properties, { column: fields[0], op: 'eq', value: '' }] })}>
          <Plus size={14} />{t('添加属性条件')}</button>
      </fieldset>
    </details>
    <button className="button primary compact" disabled={disabled} onClick={() => expand(normalized, !!continuing)}>
      <Expand size={15} />{busy ? t('正在展开…') : continuing ? next === 'pending' ? t('继续加入当前页') : t('加载下一页') : t('展开邻居')}</button>
    {loadAll && continuing && next === 'more' && <button className="button compact" disabled={disabled}
      onClick={() => loadAll(normalized)}>{t('加载其余邻居')}</button>}
  </div>;
}
