import { isMap, isScalar, isSeq, parseDocument, stringify } from 'yaml';
import type { ValidationSite } from './api';

export type Section = 'sources' | 'nodes' | 'edges';
export type Selection = { section: Section; name: string };
export type ModelMap = Record<string, Record<string, unknown>>;
export type Model = { graph: string; sources: ModelMap; nodes: ModelMap; edges: ModelMap };
export type Endpoint = { node: string; columns: string[] };
export type YamlDocument = ReturnType<typeof parseDocument>;
export type ValidationTarget = { selection: Selection; control: string; caption: string };

export const modelIdPattern = '[A-Za-z0-9][A-Za-z0-9_\\-]{0,63}';
export const text = (value: unknown) => typeof value === 'string' ? value : '';
export const strings = (value: unknown): string[] => Array.isArray(value) ? value.map(text) : [];
export const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};

export function documentOf(yaml: string) {
  const doc = parseDocument(yaml, { intAsBigInt: true });
  if (doc.errors.length) throw doc.errors[0];
  if (!isMap(doc.contents)) throw new Error('配置根节点必须为 YAML 映射');
  return doc;
}

export function modelOf(yaml: string): Model {
  const value = record(documentOf(yaml).toJS({ maxAliasCount: 50 }));
  const section = (key: Section) => Object.fromEntries(
    Object.entries(record(value[key])).map(([name, item]) => [name, record(item)]),
  );
  return { graph: text(value.graph), sources: section('sources'), nodes: section('nodes'), edges: section('edges') };
}

export function emptyModel(name: string) {
  const doc = documentOf('graph: ""\nsources: {}\nnodes: {}\nedges: {}\n');
  doc.set('graph', name);
  return doc.toString();
}

export function endpoint(value: unknown): Endpoint {
  const item = record(value);
  return { node: text(item.node), columns: strings(item.columns) };
}

export function sourceLabel(value: unknown) {
  const source = text(value);
  return ({
    postgres: 'PostgreSQL', parquet: 'Parquet', mysql: 'MySQL', iceberg: 'Iceberg',
    delta: 'Delta Lake', bigquery: 'BigQuery', snowflake: 'Snowflake',
    clickhouse: 'ClickHouse', trino: 'Trino', mongodb: 'MongoDB',
    elasticsearch: 'Elasticsearch', redshift: 'Redshift', spanner: 'Spanner', sap_hana: 'SAP HANA',
  } as Record<string, string>)[source] ?? source;
}

export function rename(doc: YamlDocument, model: Model, section: 'nodes' | 'edges', before: string, after: string) {
  const map = doc.get(section, true);
  if (!isMap(map)) throw new Error('无法编辑此映射，请使用 YAML');
  const pair = map.items.find(item => isScalar(item.key) && item.key.value === before);
  if (!pair) throw new Error('未找到待重命名的映射');
  pair.key = doc.createNode(after);
  if (section !== 'nodes') return;
  for (const [name, edge] of Object.entries(model.edges)) {
    for (const side of ['from', 'to']) {
      if (endpoint(edge[side]).node === before) doc.setIn(['edges', name, side, 'node'], after);
    }
  }
}

export function incidentEdgeTypes(model: Model, node: string) {
  return Object.entries(model.edges).filter(([, edge]) =>
    endpoint(edge.from).node === node || endpoint(edge.to).node === node).map(([name]) => name);
}

export function removeGraphType(doc: YamlDocument, model: Model, section: 'nodes' | 'edges', name: string) {
  if (section === 'nodes') {
    if (Object.keys(model.nodes).length <= 1) throw new Error('图模型至少需要一个节点标签');
    for (const edge of incidentEdgeTypes(model, name)) doc.deleteIn(['edges', edge]);
  }
  doc.deleteIn([section, name]);
}

export const validationControl = (...parts: (string | number)[]) => JSON.stringify(parts);

export function sourceSignature(model: Model, table: string) {
  const dot = table.indexOf('.');
  if (dot < 1) return undefined;
  const source = Object.hasOwn(model.sources, table.slice(0, dot)) ? model.sources[table.slice(0, dot)] : undefined;
  return source && stringify(source);
}

export function validationTargets(model: Model, location: ValidationSite): ValidationTarget[] {
  const found: ValidationTarget[] = [];
  for (const section of ['nodes', 'edges'] as const) {
    for (const [name, item] of Object.entries(model[section])) {
      const add = (caption: string, ...control: (string | number)[]) => {
        found.push({ selection: { section, name }, control: validationControl(...control), caption: name + ' · ' + caption });
      };
      const fields = [
        { key: 'key', title: '业务键', columns: strings(item.key) },
        { key: 'properties', title: '属性', columns: strings(item.properties) },
        { key: 'from', title: '起点映射', columns: endpoint(item.from).columns },
        { key: 'to', title: '终点映射', columns: endpoint(item.to).columns },
      ];
      if (location.kind === 'input_column') {
        if (name === location.label) add('来源 ' + (location.input + 1) + ' · ' + location.column, 'column', location.input, location.column);
        continue;
      }
      if (location.kind === 'field') {
        if (name !== location.label) continue;
        const field = fields.find(value => value.key === location.field);
        if (field && location.index !== null) {
          add(field.title + ' ' + (location.index + 1), field.key, location.index);
          const logical = field.columns[location.index];
          if (logical && Array.isArray(item.inputs)) item.inputs.forEach((_, index) =>
            add('来源 ' + (index + 1) + ' · ' + logical, 'column', index, logical));
        }
        else if (location.field === 'source') {
          if (location.index === null) add('来源表', 'table');
          else add('来源表 ' + (location.index + 1), 'table', location.index);
        }
        else if (field) add(field.title, field.key);
        else add('数据映射', 'mapping');
        continue;
      }
      const { column, table } = location;
      if (item.source === table) {
        const count = found.length;
        if (column !== null) for (const field of fields) {
          field.columns.forEach((value, index) => {
            if (value === column) add(field.title + ' ' + (index + 1), field.key, index);
          });
        }
        if (found.length === count) add('来源表', 'table');
      }
      if (Array.isArray(item.inputs)) item.inputs.map(record).forEach((input, index) => {
        if (input.table !== table) return;
        const count = found.length;
        if (column !== null) {
          const maps = record(input.columns);
          const logical = new Set([...fields.flatMap(field => field.columns), ...Object.keys(maps)]);
          for (const key of logical) {
            if ((text(maps[key]) || key) === column) add('来源 ' + (index + 1) + ' · ' + key, 'column', index, key);
          }
          if (Array.isArray(input.where)) input.where.map(record).forEach((filter, rule) => {
            if (filter.column === column) add('来源 ' + (index + 1) + ' · 过滤列 ' + (rule + 1), 'filter', index, rule);
          });
        }
        if (found.length === count) add('来源表 ' + (index + 1), 'table', index);
      });
    }
  }
  return found;
}

// Keep existing YAML nodes intact when converting a single input or moving one.
export function appendMappingInput(doc: YamlDocument, path: string[]) {
  const inputs = [...path, 'inputs'];
  const sequence = doc.getIn(inputs, true);
  if (isSeq(sequence) && sequence.items.length) {
    sequence.add(doc.createNode({ table: '' }));
  } else {
    const original = doc.getIn([...path, 'source'], true);
    doc.setIn(inputs, doc.createNode([{ table: '' }, { table: '' }]));
    if (original) doc.setIn([...inputs, 0, 'table'], original);
    doc.deleteIn([...path, 'source']);
  }
}
