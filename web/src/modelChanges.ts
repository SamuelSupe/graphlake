import type { VersionHistory } from './api';
import { t } from './locale.ts';
import { stringify } from 'yaml';
import { documentOf, record } from './model.ts';

export type ModelChange = { kind: 'added' | 'removed' | 'changed'; section: string; name: string; fields: string[] };
const fieldNames: Record<string, string> = {
  source: t('来源表'), inputs: t('来源映射'), key: t('业务键'), properties: t('属性'), from: t('起点'), to: t('终点'),
  semantics: t('业务语义'), tasks: t('业务任务'), scd2: t('时态规则'), logical_partition: t('逻辑分区'), type: t('连接器'), tables: t('来源表'), dsn_env: t('连接环境变量'),
};
const canonical = (value: unknown) => stringify(value, { sortMapEntries: true });

export function modelChanges(before: string, after: string): ModelChange[] {
  const old = record(documentOf(before).toJS({ maxAliasCount: 50 }));
  const next = record(documentOf(after).toJS({ maxAliasCount: 50 }));
  const changes: ModelChange[] = [];
  for (const [key, label] of [['sources', t('数据源')], ['nodes', t('节点')], ['edges', t('边')]] as const) {
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
      section: t('模型'), name: key === 'graph' ? t('名称') : fieldNames[key] ?? key, fields: [] });
  }
  return changes;
}

export const versionsPerPage = 5;
export type Archive = { yaml: string } | { error: string; repair?: string };

export function releaseRows(history: VersionHistory | undefined, start: number, archives: Map<string, Archive>) {
  return history?.versions.slice(start, start + versionsPerPage).map((entry, offset) => {
    const previous = history.versions[start + offset + 1];
    const before = previous ? archives.get(previous.revision) : { yaml: '{}' };
    const after = archives.get(entry.revision);
    let changes: ModelChange[] | undefined;
    if (before && 'yaml' in before && after && 'yaml' in after) {
      try { changes = modelChanges(before.yaml, after.yaml); } catch { /* The raw revision comparison is still available. */ }
    }
    return { entry, previous, before, after, changes, index: start + offset };
  }) ?? [];
}
