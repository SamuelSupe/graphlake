import { sourceLabel, text, type YamlDocument } from './model';

export type ConnectorField = {
  key: string;
  label: string;
  optional?: boolean;
  advanced?: boolean;
  kind?: 'integer' | 'toggle';
  placeholder?: string;
  hint?: string;
};

const dsn = { key: 'dsn_env', label: '连接串环境变量', placeholder: 'GRAPHLAKE_DATABASE_DSN', hint: '填写已在 GraphLake 服务中设置的变量名，不是连接串本身。' };
const address = { key: 'uri', label: '服务地址', placeholder: 'https://example.com' };
const user = { key: 'user', label: '用户名' };
const token = { key: 'token_env', label: '令牌环境变量', placeholder: 'GRAPHLAKE_SOURCE_TOKEN' };
const password = { key: 'password_env', label: '密码环境变量', placeholder: 'GRAPHLAKE_SOURCE_PASSWORD' };
const tls: ConnectorField[] = [
  { key: 'tls', label: '启用 TLS', kind: 'toggle', advanced: true },
  { key: 'ca_file', label: 'CA 证书路径', optional: true, advanced: true },
];
const trinoAuth = [{ ...token, optional: true }, { ...password, optional: true }];
const trinoCatalog = [address, { key: 'catalog', label: 'Trino 目录' }, user, ...trinoAuth];
const asOf = { key: 'as_of', label: '读取时间', optional: true, advanced: true };

export const connectorFields: Readonly<Record<string, readonly ConnectorField[]>> = {
  duckdb: [{ key: 'path', label: '数据库文件路径', placeholder: '/data/analytics.duckdb', hint: '只读打开服务端的 DuckDB 文件。相对路径从当前模型目录解析。' }],
  doris: [dsn, ...tls],
  starrocks: [dsn, ...tls],
  tidb: [dsn, ...tls],
  oceanbase: [{ ...dsn, hint: '使用 OceanBase MySQL 模式租户的连接串环境变量。' }, ...tls],
  mariadb: [dsn, ...tls],
  sqlserver_native: [{ key: 'host', label: '主机', placeholder: 'sqlserver.example.com' }, { key: 'port', label: '端口', kind: 'integer', placeholder: '1433' }, { key: 'database', label: '数据库', placeholder: 'analytics' }, user, password, ...tls],
  oracle_native: [{ key: 'connect_string', label: '连接地址', placeholder: 'db.example.com:1521/FREEPDB1', hint: '服务端需安装 Oracle Instant Client；支持 Easy Connect 或 TNS 名称。' }, user, password],
  mongodb_native: [{ ...dsn, hint: '环境变量中保存 mongodb:// 或 mongodb+srv:// 连接串。' }, { key: 'schema_sample_size', label: '结构采样条数', kind: 'integer', optional: true, placeholder: '1000', hint: '默认 1000，最多 10000 条。嵌套对象展开为点分字段，数组保留为 JSON 文本。' }],
  databricks: [{ ...address, label: '工作区地址' }, token, { key: 'warehouse', label: 'SQL Warehouse ID' }, { key: 'catalog', label: '目录', placeholder: 'main' }],
  object_storage: [],
  postgres: [dsn],
  sqlite: [{ key: 'path', label: '数据库文件路径', placeholder: '/data/app.db', hint: '使用服务端文件路径；相对路径从当前模型目录解析。' }],
  parquet: [],
  csv: [],
  jsonl: [],
  mysql: [dsn, ...tls],
  clickhouse: [address, user, { ...password, optional: true }],
  trino: [address, user, ...trinoAuth],
  iceberg: [{ ...address, label: 'REST Catalog 地址' }, { key: 'warehouse', label: '仓库', optional: true }, { ...token, optional: true }],
  delta: [],
  snowflake: [address, token, { key: 'warehouse', label: '计算仓库' }, { key: 'role', label: '角色', optional: true, advanced: true }, asOf],
  bigquery: [
    { key: 'billing_project', label: '计费项目' }, { key: 'location', label: '数据位置' }, token,
    { key: 'maximum_bytes_billed', label: '单次扫描字节上限', kind: 'integer', placeholder: '1000000000', hint: '必须大于 0；按字节限制单次 BigQuery 扫描费用。' },
    { ...address, optional: true, advanced: true }, asOf,
  ],
  mongodb: trinoCatalog,
  elasticsearch: trinoCatalog,
  redshift: trinoCatalog,
  sqlserver: trinoCatalog,
  oracle: trinoCatalog,
  spanner: [
    { key: 'database', label: '数据库资源路径' },
    { ...address, label: '服务地址', optional: true, advanced: true },
  ],
  sap_hana: [
    { key: 'host', label: '主机' }, { key: 'port', label: '端口', kind: 'integer' }, user, password, ...tls,
  ],
};

export const connectorChoices: [string, string][] = Object.keys(connectorFields).map(type => [type, sourceLabel(type)]);

export const connectorDetails: Readonly<Record<string, { group: string; description: string }>> = {
  duckdb: { group: '数据库', description: '直接读取本地 DuckDB 分析数据库，保持只读事务。' },
  doris: { group: '数仓与分析', description: '直接连接 Apache Doris，读取分析表与视图。' },
  starrocks: { group: '数仓与分析', description: '直接连接 StarRocks，读取分析表与视图。' },
  tidb: { group: '数据库', description: '通过 MySQL 协议读取 TiDB，按语句读取数据。' },
  oceanbase: { group: '数据库', description: '连接 OceanBase 的 MySQL 模式租户。' },
  mariadb: { group: '数据库', description: '连接 MariaDB，以只读快照读取 InnoDB 表。' },
  sqlserver_native: { group: '数据库', description: '通过 TDS 直连 SQL Server，无需部署 Trino。' },
  oracle_native: { group: '数据库', description: '通过 Oracle Instant Client 直连，使用只读事务。' },
  mongodb_native: { group: '数据库', description: '通过 MongoDB 驱动直接读取集合，支持嵌套字段。' },
  databricks: { group: '数仓与分析', description: '通过 SQL Warehouse 查询 Databricks 目录中的表。' },
  object_storage: { group: '对象存储', description: '配置 S3、MinIO、OSS、TOS、COS、Azure Blob 或 GCS，浏览对象并读取 Parquet、CSV、JSON Lines。' },
  postgres: { group: '数据库', description: '连接 PostgreSQL，读取业务表与视图。' },
  sqlite: { group: '数据库', description: '直接读取服务端的 SQLite 数据库文件。' },
  mysql: { group: '数据库', description: '连接 MySQL，支持 TLS 与自定义 CA。' },
  sap_hana: { group: '数据库', description: '连接 SAP HANA，读取企业业务数据。' },
  parquet: { group: '文件与数据湖', description: '映射本地或对象存储中的 Parquet 文件。' },
  csv: { group: '文件与数据湖', description: '配置 CSV 文件、表头和列类型。' },
  jsonl: { group: '文件与数据湖', description: '读取逐行 JSON，并声明所需的顶层字段。' },
  iceberg: { group: '文件与数据湖', description: '连接 Iceberg REST Catalog，可固定表快照。' },
  delta: { group: '文件与数据湖', description: '映射 Delta Lake 表路径，可指定读取版本。' },
  clickhouse: { group: '数仓与分析', description: '通过 HTTP 连接 ClickHouse 分析数据。' },
  trino: { group: '数仓与分析', description: '连接 Trino，访问已配置的目录与表。' },
  snowflake: { group: '数仓与分析', description: '配置 Snowflake SQL API 与计算仓库。' },
  bigquery: { group: '数仓与分析', description: '连接 BigQuery，设置计费项目与扫描上限。' },
  spanner: { group: '数据库', description: '通过数据库资源路径连接 Cloud Spanner。' },
  mongodb: { group: '通过 Trino', description: '通过 Trino 的 MongoDB 目录读取集合。' },
  elasticsearch: { group: '通过 Trino', description: '通过 Trino 的 Elasticsearch 目录读取索引。' },
  redshift: { group: '通过 Trino', description: '通过 Trino 的 Redshift 目录读取数仓表。' },
  sqlserver: { group: '通过 Trino', description: '通过 Trino 的 SQL Server 目录读取关系表。' },
  oracle: { group: '通过 Trino', description: '通过 Trino 的 Oracle 目录读取关系表。' },
};

export function newConnector(type: string) {
  const fields = connectorFields[type];
  if (!Object.hasOwn(connectorFields, type) || !fields) throw new Error('请选择连接器');
  if (type === 'object_storage') return { type, storage: { provider: 's3', bucket: '' }, format: 'parquet', tables: {} };
  return {
    type,
    ...Object.fromEntries(fields.filter(field => !field.optional && field.kind !== 'toggle').map(
      field => [field.key, field.kind === 'integer' ? 0n : ''],
    )),
    ...(['parquet', 'delta', 'csv', 'jsonl'].includes(type) ? { tables: {} } : {}),
    ...(type === 'sqlserver_native' ? { port: 1433n } : {}),
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
