import { t } from './locale.ts';
import type { GraphSchema } from './api';

export type Dialect = 'cypher' | 'sql' | 'gremlin';
export type QueryCompletion = { label: string; type: string; detail?: string; apply?: string };

export function schemaCompletions(schema: GraphSchema | undefined, language: Dialect): QueryCompletion[] {
  if (!schema) return [];
  const identifier = (name: string) => language === 'sql' ? '"' + name.replaceAll('"', '""') + '"'
    : language === 'gremlin' ? "'" + name.replaceAll('\\', '\\\\').replaceAll("'", "\\'") + "'"
      : /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : '`' + name.replaceAll('`', '``') + '`';
  const options: QueryCompletion[] = [];
  const properties = new Map<string, string[]>();
  for (const [types, kind] of [[schema.nodes, t('节点')], [schema.edges, t('边')]] as const) {
    for (const [name, type] of Object.entries(types)) {
      options.push({ label: name, type: 'class', detail: kind, apply: identifier(name) });
      for (const field of new Set([...type.key, ...type.properties])) {
        properties.set(field, [...properties.get(field) ?? [], name]);
      }
    }
  }
  for (const [name, owners] of properties) options.push({ label: name, type: 'property', detail: owners.join(' · '), apply: identifier(name) });
  return options;
}

export const keywords: Record<Dialect, string[]> = {
  cypher: ['MATCH', 'OPTIONAL MATCH', 'WHERE', 'RETURN', 'WITH', 'ORDER BY', 'LIMIT', 'SKIP', 'DISTINCT', 'UNWIND', 'AS', 'AND', 'OR', 'NOT', 'count', 'sum', 'collect'],
  sql: ['SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'GROUP BY', 'ORDER BY', 'LIMIT', 'AS', 'DISTINCT', 'AND', 'OR', 'COUNT', 'SUM'],
  gremlin: ['g.V()', 'g.E()', 'hasLabel', 'has', 'out', 'in', 'outE', 'inE', 'inV', 'outV', 'values', 'valueMap', 'count', 'limit', 'path', 'dedup', 'order', 'by'],
};
