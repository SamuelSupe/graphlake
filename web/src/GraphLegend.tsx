import { t } from './locale.ts';
import { useMemo } from 'react';
import type { GraphElement, GraphSchema } from './api';
import { graphType } from './graphExploration';

export function GraphLegend({ elements, schema, hidden, labels, color, toggle, setLabel, reset }: {
  elements: GraphElement[]; schema?: GraphSchema; hidden: ReadonlySet<string>; labels: ReadonlyMap<string, string | null>;
  color: (label: string) => string; toggle: (type: string) => void; setLabel: (type: string, field: string | null | undefined) => void; reset: () => void;
}) {
  const types = useMemo(() => {
    const result = new Map<string, { type: string; label: string; kind: GraphElement['kind']; properties: Set<string>; count: number }>();
    for (const kind of ['node', 'edge'] as const) {
      const definitions = kind === 'node' ? schema?.nodes : schema?.edges;
      for (const [label, fields] of Object.entries(definitions ?? {})) {
        const type = graphType({ kind, label });
        result.set(type, { type, label, kind, properties: new Set(fields.properties), count: 0 });
      }
    }
    for (const element of elements) {
      const { kind, label } = element;
      const type = graphType(element);
      const entry = result.get(type) ?? { type, label, kind, properties: new Set<string>(), count: 0 };
      entry.count++;
      for (const field of Object.keys(element.properties ?? {})) entry.properties.add(field);
      result.set(type, entry);
    }
    return [...result.values()].sort((a, b) => a.kind === b.kind ? a.label.localeCompare(b.label) : a.kind === 'node' ? -1 : 1);
  }, [elements, schema]);
  return <details className="explorer-legend">
    <summary>{t('图例与标签')}{hidden.size > 0 && t(' · 已隐藏 {0} 种类型', hidden.size)}</summary>
    <div className="explorer-legend-body">
      {(['node', 'edge'] as const).map(kind => {
        const group = types.filter(type => type.kind === kind);
        if (!group.length) return null;
        const caption = kind === 'node' ? t('节点') : t('边');
        return <fieldset className="explorer-legend-group" key={kind}>
          <legend>{caption}{t('类型')}</legend>
          {group.map(({ type, label, properties, count }) => {
            const selected = labels.get(type);
            return <div className="explorer-legend-row" key={type}>
              <label className="check-label"><input type="checkbox" aria-label={t('显示{0} {1}', caption, label)}
                checked={!hidden.has(type)} onChange={() => toggle(type)} />
                <span className="explorer-type-dot" style={{ backgroundColor: color(label) }} aria-hidden="true" />
                <span><code>{label}</code> <small className="muted">{count}</small></span></label>
              <select aria-label={t('{0} {1} 的显示标签', caption, label)} value={selected === null ? 'type' : selected === undefined ? '' : 'property:' + selected}
                onChange={event => {
                  const value = event.target.value;
                  setLabel(type, value === 'type' ? null : value ? value.slice('property:'.length) : undefined);
                }}>
                <option value="">{t('自动选择名称')}</option>
                <option value="type">{t('类型名')}</option>
                {selected && !properties.has(selected) && <option value={'property:' + selected}>{selected}</option>}
                {[...properties].sort().map(field => <option key={field} value={'property:' + field}>{field}</option>)}
              </select>
            </div>;
          })}
        </fieldset>;
      })}
      <div className="explorer-legend-footer">
        <span className="muted small">{t('名称为空时显示类型名。隐藏节点时，其相连的边也会隐藏。')}</span>
        <button className="button compact" disabled={!hidden.size && !labels.size} onClick={reset}>{t('重置图例')}</button>
      </div>
    </div>
  </details>;
}
