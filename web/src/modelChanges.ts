import { stringify } from 'yaml';
import { documentOf, record } from './model.ts';

export type ModelChange = { kind: 'added' | 'removed' | 'changed'; section: string; name: string; fields: string[] };
const fieldNames: Record<string, string> = {
  source: '来源表', inputs: '来源映射', key: '业务键', properties: '属性', from: '起点', to: '终点',
  scd2: '时态规则', logical_partition: '逻辑分区', type: '连接器', tables: '来源表', dsn_env: '连接环境变量',
};
const canonical = (value: unknown) => stringify(value, { sortMapEntries: true });

export function modelChanges(before: string, after: string): ModelChange[] {
  const old = record(documentOf(before).toJS({ maxAliasCount: 50 }));
  const next = record(documentOf(after).toJS({ maxAliasCount: 50 }));
  const changes: ModelChange[] = [];
  for (const [key, label] of [['sources', '数据源'], ['nodes', '节点'], ['edges', '边']] as const) {
    const a = record(old[key]), b = record(next[key]);
    for (const name of new Set([...Object.keys(a), ...Object.keys(b)])) {
      const existsBefore = Object.hasOwn(a, name), existsAfter = Object.hasOwn(b, name);
      if (existsBefore && existsAfter && canonical(a[name]) === canonical(b[name])) continue;
      const fields = existsBefore && existsAfter ? [...new Set([...Object.keys(record(a[name])), ...Object.keys(record(b[name]))])]
        .filter(field => canonical(record(a[name])[field]) !== canonical(record(b[name])[field]))
        .map(field => fieldNames[field] ?? field) : [];
      changes.push({ kind: !existsBefore ? 'added' : !existsAfter ? 'removed' : 'changed', section: label, name, fields });
    }
  }
  for (const key of new Set([...Object.keys(old), ...Object.keys(next)])) {
    if (['sources', 'nodes', 'edges'].includes(key) || canonical(old[key]) === canonical(next[key])) continue;
    changes.push({ kind: !Object.hasOwn(old, key) ? 'added' : !Object.hasOwn(next, key) ? 'removed' : 'changed',
      section: '模型', name: key === 'graph' ? '名称' : key, fields: [] });
  }
  return changes;
}
