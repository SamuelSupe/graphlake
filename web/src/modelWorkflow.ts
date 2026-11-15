import { endpoint, record, strings, text, type Model, type Selection } from './model.ts';

export type SetupIssue = { message: string; selection?: Selection; add?: 'sources' | 'nodes' };

// These checks identify unfinished form fields; the server remains the authority on validity.
export function setupIssue(model: Model): SetupIssue | undefined {
  if (!Object.keys(model.sources).length) return { message: '先添加数据源，再选择节点对应的来源表。', add: 'sources' };
  if (!Object.keys(model.nodes).length) return { message: '添加至少一个节点标签，并设置来源表与业务键后即可保存。', add: 'nodes' };
  for (const section of ['nodes', 'edges'] as const) {
    for (const [name, item] of Object.entries(model[section])) {
      const selection = { section, name };
      const tables = Array.isArray(item.inputs) && item.inputs.length ? item.inputs.map(input => text(record(input).table)) : [text(item.source)];
      if (!tables.length || tables.some(table => !table.trim())) return { message: `为 ${name} 选择来源表。`, selection };
      if (section === 'nodes' && (!strings(item.key).length || strings(item.key).some(key => !key.trim()))) {
        return { message: `为节点 ${name} 设置业务键，用于唯一标识每条记录。`, selection };
      }
      if (section === 'edges') for (const side of ['from', 'to'] as const) {
        const end = endpoint(item[side]);
        if (!end.node || !end.columns.length || end.columns.some(column => !column.trim())) {
          return { message: `为边 ${name} 配置${side === 'from' ? '起点' : '终点'}节点及对应字段。`, selection };
        }
      }
    }
  }
}
