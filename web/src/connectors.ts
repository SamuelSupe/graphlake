import { sourceLabel, text, type YamlDocument } from './model';

export type ConnectorField = {
  key: string;
  label: string;
  optional?: boolean;
  advanced?: boolean;
  kind?: 'integer' | 'toggle';
};

const dsn = { key: 'dsn_env', label: '连接串环境变量' };
const address = { key: 'uri', label: '服务地址' };
const user = { key: 'user', label: '用户名' };
const token = { key: 'token_env', label: '令牌环境变量' };
const password = { key: 'password_env', label: '密码环境变量' };
const tls: ConnectorField[] = [
  { key: 'tls', label: '启用 TLS', kind: 'toggle', advanced: true },
  { key: 'ca_file', label: 'CA 证书路径', optional: true, advanced: true },
];
const trinoAuth = [{ ...token, optional: true }, { ...password, optional: true }];
const trinoCatalog = [address, { key: 'catalog', label: 'Trino 目录' }, user, ...trinoAuth];
const asOf = { key: 'as_of', label: '读取时间', optional: true, advanced: true };

export const connectorFields: Readonly<Record<string, readonly ConnectorField[]>> = {
  postgres: [dsn],
  parquet: [],
  mysql: [dsn, ...tls],
  clickhouse: [address, user, { ...password, optional: true }],
  trino: [address, user, ...trinoAuth],
  iceberg: [{ ...address, label: 'REST Catalog 地址' }, { key: 'warehouse', label: '仓库', optional: true }, { ...token, optional: true }],
  delta: [],
  snowflake: [address, token, { key: 'warehouse', label: '计算仓库' }, { key: 'role', label: '角色', optional: true, advanced: true }, asOf],
  bigquery: [
    { key: 'billing_project', label: '计费项目' }, { key: 'location', label: '数据位置' }, token,
    { key: 'maximum_bytes_billed', label: '单次扫描字节上限', kind: 'integer' },
    { ...address, optional: true, advanced: true }, asOf,
  ],
  mongodb: trinoCatalog,
  elasticsearch: trinoCatalog,
  redshift: trinoCatalog,
  spanner: [
    { key: 'database', label: '数据库资源路径' },
    { ...address, label: '服务地址', optional: true, advanced: true },
  ],
  sap_hana: [
    { key: 'host', label: '主机' }, { key: 'port', label: '端口', kind: 'integer' }, user, password, ...tls,
  ],
};

export const connectorChoices: [string, string][] = Object.keys(connectorFields).map(type => [type, sourceLabel(type)]);

export function newConnector(type: string) {
  const fields = connectorFields[type];
  if (!Object.hasOwn(connectorFields, type) || !fields) throw new Error('请选择连接器');
  return {
    type,
    ...Object.fromEntries(fields.filter(field => !field.optional && field.kind !== 'toggle').map(
      field => [field.key, field.kind === 'integer' ? 0n : ''],
    )),
    ...(type === 'parquet' || type === 'delta' ? { tables: {} } : {}),
  };
}

export function integerInput(raw: string) {
  return /^-?\d+$/.test(raw) ? BigInt(raw) : raw;
}

export function configText(value: unknown) {
  return typeof value === 'bigint' || typeof value === 'number' ? String(value) : text(value);
}

export function setOptional(doc: YamlDocument, path: (string | number)[], value: unknown) {
  if (value === '') {
    if (doc.hasIn(path)) doc.deleteIn(path);
  } else {
    doc.setIn(path, value);
  }
}
