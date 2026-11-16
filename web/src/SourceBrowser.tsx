import { t } from './locale.ts';
import { useEffect, useState } from 'react';
import { api, errorText, type Column, type Listing } from './api';
import { sourceLabel, type Model } from './model';
import { Modal } from './ui';

export type TableSelection = { table: string; columns: Column[] };
const hasNamespace = (kind: unknown) => ['duckdb', 'doris', 'starrocks', 'tidb', 'oceanbase', 'mariadb', 'sqlserver_native', 'oracle_native', 'mongodb_native', 'databricks', 'postgres', 'mysql', 'iceberg', 'trino', 'mongodb', 'elasticsearch', 'redshift', 'sqlserver', 'oracle', 'clickhouse', 'sap_hana', 'snowflake', 'bigquery'].includes(String(kind));

function splitTable(kind: unknown, table: string) {
  if (!hasNamespace(kind)) return ['', table];
  const split = ['iceberg', 'trino', 'snowflake', 'bigquery'].includes(String(kind)) ? table.lastIndexOf('.') : table.indexOf('.');
  return split < 0 ? ['', ''] : [table.slice(0, split), table.slice(split + 1)];
}

export function SourceBrowser({ id, yaml, model, current, fixedSource, close, choose }: {
  id: string; yaml: string; model: Model; current?: string; fixedSource?: string;
  close: () => void; choose?: (selection: TableSelection) => void;
}) {
  const names = Object.keys(model.sources);
  const [source, setSource] = useState(fixedSource ?? names.find(name => current?.startsWith(name + '.')) ?? names[0] ?? '');
  const kind = model.sources[source]?.type;
  const namespaced = hasNamespace(kind);
  const namespaceLabel = kind === 'iceberg' ? t('命名空间') : kind === 'bigquery' ? t('项目与数据集') : t('模式');
  const manualNamespace = ['snowflake', 'bigquery', 'sap_hana'].includes(String(kind));
  const initial = current?.startsWith(source + '.') ? current.slice(source.length + 1) : '';
  const [initialSchema, initialTable] = splitTable(kind, initial);
  const [schema, setSchema] = useState(initialSchema);
  const [table, setTable] = useState(initialTable);
  const [typedTable, setTypedTable] = useState('');
  const [typedSchema, setTypedSchema] = useState('');
  const [catalog, setCatalog] = useState(kind === 'trino' ? initialSchema.split('.')[0] : '');
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [trinoSchemas, setTrinoSchemas] = useState<string[]>([]);
  const [columns, setColumns] = useState<Column[]>();
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);
  const [pending, setPending] = useState(false);
  const [tableAttempt, setTableAttempt] = useState(0);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [limits, setLimits] = useState<Record<string, boolean>>({});

  useEffect(() => {
    if (!source || !namespaced) return;
    const controller = new AbortController();
    setLoadingSchemas(true);
    api<Listing>('/models/' + id + '/browse', {
      method: 'POST', signal: controller.signal, body: JSON.stringify({ yaml, source }),
    }).then(data => {
      setSchemas(data.schemas);
      setLimits(old => ({ ...old, schemas: data.truncated }));
    }, cause => { if (!controller.signal.aborted) setErrors(old => ({ ...old, schemas: errorText(cause) })); })
      .finally(() => { if (!controller.signal.aborted) setLoadingSchemas(false); });
    return () => controller.abort();
  }, [id, yaml, source, namespaced]);

  useEffect(() => {
    if (!source || namespaced && !schema && !(kind === 'trino' && catalog)) return;
    const controller = new AbortController();
    setLoadingTables(true);
    api<Listing>('/models/' + id + '/browse', {
      method: 'POST', signal: controller.signal, body: JSON.stringify({ yaml, source, schema: schema || catalog || undefined }),
    }).then(data => {
      if (kind === 'trino' && !schema) setTrinoSchemas(data.tables);
      else setTables(data.tables);
      setLimits(old => ({ ...old, tables: data.truncated }));
    }, cause => { if (!controller.signal.aborted) setErrors(old => ({ ...old, tables: errorText(cause) })); })
      .finally(() => { if (!controller.signal.aborted) setLoadingTables(false); });
    return () => controller.abort();
  }, [id, yaml, source, namespaced, schema, catalog, kind]);

  useEffect(() => {
    if (!source || !table || namespaced && !schema) return;
    const controller = new AbortController();
    setPending(true);
    api<Listing>('/models/' + id + '/browse', {
      method: 'POST', signal: controller.signal, body: JSON.stringify({ yaml, source, schema: schema || undefined, table }),
    }).then(data => setColumns(data.columns), cause => {
      if (!controller.signal.aborted) setErrors(old => ({ ...old, columns: errorText(cause) }));
    }).finally(() => { if (!controller.signal.aborted) setPending(false); });
    return () => controller.abort();
  }, [id, yaml, source, namespaced, schema, table, tableAttempt]);

  const options = (values: string[], current: string) => [...new Set([...values, ...(current ? [current] : [])])].map(value => <option key={value}>{value}</option>);
  function changeTable(value: string, reread = false) {
    setErrors(old => ({ ...old, columns: '' })); setColumns(undefined); setPending(false); setTable(value);
    if (reread) setTableAttempt(attempt => attempt + 1);
  }

  const selectedTable = table ? [source, ...(namespaced ? [schema] : []), table].join('.') : '';
  return <Modal title={t('浏览来源表与字段')} size="wide" description={choose ? t('选择来源表，核对字段和类型后带入当前映射。') : t('选择来源表，查看字段名称、类型和是否允许空值。')} close={close}>
    <div className="panel-layout source-browser">
      <div className="panel-body source-browser-grid"><section className="source-picker" aria-label={t('选择来源')}>
      <h3>{t('选择来源')}</h3>
      {fixedSource ? <p className="source-browser-context"><strong>{fixedSource}</strong><span>{sourceLabel(kind)}</span></p>
        : <label>{t('数据源')}<select data-initial-focus value={source} onChange={event => {
        setSource(event.target.value); setCatalog(''); setSchema(''); changeTable(''); setTypedTable(''); setTypedSchema(''); setSchemas([]); setTables([]); setErrors({}); setLimits({}); setLoadingSchemas(false); setLoadingTables(false);
      }}><option value="">{t('选择数据源')}</option>
        {names.map(name => <option key={name} value={name}>{name} · {sourceLabel(model.sources[name].type)}</option>)}
      </select></label>}
      {kind === 'trino' && <label>{t('目录')}<select value={catalog} aria-busy={loadingSchemas} onChange={event => {
        setCatalog(event.target.value); setSchema(''); changeTable(''); setTables([]); setTrinoSchemas([]); setLoadingTables(false);
      }}><option value="">{loadingSchemas ? t('正在读取目录…') : t('选择目录')}</option>{options(schemas, catalog)}</select></label>}
      {source && namespaced && (kind !== 'trino' || catalog) && <label>{namespaceLabel}<select value={schema} onChange={event => {
        setSchema(event.target.value); changeTable(''); setTables([]); setLoadingTables(false); setErrors(old => ({ ...old, tables: '', columns: '' }));
      }} aria-busy={kind === 'trino' ? loadingTables && !schema : loadingSchemas}><option value="">{(kind === 'trino' ? loadingTables && !schema : loadingSchemas) ? t('正在读取{0}…', namespaceLabel) : t('选择{0}', namespaceLabel)}</option>{options(kind === 'trino' ? trinoSchemas : schemas, schema)}</select></label>}
      {source && (!namespaced || schema) && <label>{t('来源表')}<select value={table} aria-busy={loadingTables} onChange={event => changeTable(event.target.value)}>
        <option value="">{loadingTables ? t('正在读取来源表…') : t('选择来源表')}</option>{options(tables, table)}
      </select></label>}
      {source && <details className="form-disclosure">
        <summary>{t('手动查找')}</summary>
        <div className="form-fields">
          {manualNamespace && <form className="mapping-source" onSubmit={event => {
            event.preventDefault(); setSchema(typedSchema.trim()); changeTable(''); setTables([]);
          }}><label>{t('限定模式')}<input value={typedSchema} onChange={event => setTypedSchema(event.target.value)} placeholder="database.schema" /></label>
            <button className="button" disabled={!typedSchema.trim()}>{t('列出表')}</button></form>}
          <form className="mapping-source" onSubmit={event => {
            event.preventDefault();
            const name = typedTable.trim();
            if (!name) return;
            if (namespaced) {
              const [namespace, table] = splitTable(kind, name);
              if (!namespace || !table) { setErrors(old => ({ ...old, columns: t('请输入完整限定表名。') })); return; }
              if (kind === 'trino') setCatalog(namespace.split('.')[0]);
              setSchema(namespace); changeTable(table, true);
            } else changeTable(name, true);
          }}><label>{t('完整表名')}<input value={typedTable} onChange={event => setTypedTable(event.target.value)} placeholder={namespaced ? 'schema.table' : t('表名')} /></label>
            <button className="button" disabled={!typedTable.trim()}>{t('读取字段')}</button></form>
        </div>
      </details>}
      {!names.length && <p className="muted small">{t('添加数据源后即可浏览。')}</p>}
      {(limits.schemas || limits.tables) && <p className="muted small">{t('此列表最多显示 2000 项；其他表可手动输入。')}</p>}
      {Object.entries(errors).filter(([, error]) => error).map(([key, error]) => <p key={key} className="inline-error" role="alert">{error}</p>)}
      </section><section className="source-preview" aria-label={t('字段预览')}>
      <div className="panel-section-heading"><h3>{t('来源字段 ')}{columns && <span className="muted small">{columns.length}{t(' 个')}</span>}</h3>
        {selectedTable && <code>{selectedTable}</code>}</div>
      {pending && <p className="muted small" role="status">{t('正在读取字段…')}</p>}
      {columns ? <div className="source-column-table"><table aria-label={t('来源字段')}>
        <thead><tr><th scope="col">{t('字段')}</th><th scope="col">{t('类型')}</th><th scope="col">{t('可为空')}</th></tr></thead>
        <tbody>{columns.map(column => <tr key={column.name}><th scope="row"><code>{column.name}</code></th><td><code>{column.data_type}</code></td><td>{column.nullable ? t('是') : t('否')}</td></tr>)}</tbody>
      </table>{!columns.length && <p className="empty">{t('此表未返回字段。')}</p>}</div>
        : !pending && <p className="panel-placeholder">{errors.columns ? t('字段读取失败，请检查来源或重新选择表。') : t('选择来源表，在这里预览字段。')}</p>}
      </section></div>
      <footer className="panel-footer"><span className="panel-footer-context">{selectedTable || t('尚未选择来源表')}</span>
        <button className="button" onClick={close}>{choose ? t('取消') : t('完成')}</button>
        {choose && <button className="button primary" disabled={!columns || pending} onClick={() => {
        if (columns) choose({ table: selectedTable, columns });
        close();
      }}>{t('使用此表')}</button>}</footer>
    </div>
  </Modal>;
}
