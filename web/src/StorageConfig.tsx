import { t } from './locale.ts';
import { useEffect, useId, useRef, useState } from 'react';
import { api, errorText } from './api';
import { ConnectorIcon } from './ConnectorIcon';
import { setOptional } from './connectors';
import type { Edit } from './Inspector';
import { documentOf, record, strings, text } from './model';
import { TextTables } from './TextTables';

const providers: Record<string, string> = { s3: 'Amazon S3', minio: 'MinIO', oss: t('阿里云 OSS'), tos: t('火山引擎 TOS'), cos: t('腾讯云 COS'), azure: 'Azure Blob Storage', gcs: 'Google Cloud Storage' };

export function StorageConfig({ item, path, edit, form, modelId }: {
  item: Record<string, unknown>; path: string[]; edit: Edit; form?: string; modelId?: string;
}) {
  const id = useId();
  const storage = record(item.storage);
  const provider = text(storage.provider) || 's3';
  const format = text(item.format) || 'parquet';
  const fields: [string, string, boolean, string][] = [
    ['bucket', provider === 'azure' ? t('容器名称') : t('存储桶名称'), true, 'analytics'],
    ...(provider === 'azure' ? [['account', t('存储账号'), true, ''], ['account_key_env', t('账号密钥环境变量'), false, 'GRAPHLAKE_AZURE_KEY']] as [string, string, boolean, string][]
      : provider === 'gcs' ? [['service_account_env', t('服务账号 JSON 环境变量'), false, 'GRAPHLAKE_GCS_CREDENTIALS']] as [string, string, boolean, string][]
      : [['region', t('区域'), ['oss', 'tos', 'cos'].includes(provider), 'us-east-1'], ['access_key_env', t('Access Key 环境变量'), false, 'GRAPHLAKE_STORAGE_KEY'], ['secret_key_env', t('Secret Key 环境变量'), false, 'GRAPHLAKE_STORAGE_SECRET'], ['session_token_env', t('临时令牌环境变量'), false, '']] as [string, string, boolean, string][]),
    ...(provider === 'gcs' ? [] : [['endpoint', t('服务端点'), ['minio', 'oss', 'tos', 'cos'].includes(provider), provider === 'tos' ? 'https://tos-s3-cn-beijing.volces.com' : 'https://storage.example.com']] as [string, string, boolean, string][]),
  ];
  return <>
    <label htmlFor={id + '-provider'}>{t('存储服务')}
      <span className="storage-provider-select">
        <ConnectorIcon type="object_storage" provider={provider} />
        <select id={id + '-provider'} form={form} value={provider} onChange={event => edit(doc => {
          doc.setIn([...path, 'storage', 'provider'], event.target.value);
          if (event.target.value === 'gcs' && doc.hasIn([...path, 'storage', 'endpoint'])) doc.deleteIn([...path, 'storage', 'endpoint']);
        })}>{Object.entries(providers).map(([value, name]) => <option value={value} key={value}>{name}</option>)}</select>
      </span>
    </label>
    <p className="muted small">{t('对象路径相对于所配置的桶或容器。凭据填写服务端环境变量名，留空时使用服务端默认身份。')}</p>
    {provider === 'oss' && <p className="muted small">{t('OSS 账号需启用 S3 兼容认证；使用所在区域的 OSS 服务端点。')}</p>}
    {provider === 'tos' && <p className="muted small">{t('TOS 使用 tos-s3-区域.volces.com 端点，并填写对应区域。')}</p>}
    <div className="connection-fields">{fields.map(([key, label, required, placeholder]) => <div className="source-field" key={key}>
      <label htmlFor={id + '-' + key}>{label}<span className="field-requirement">{required ? t('必填') : t('选填')}</span></label>
      <input id={id + '-' + key} name={'storage-' + key} form={form} required={!!form && required} type={key === 'endpoint' ? 'url' : 'text'}
        className="mono" spellCheck={false} autoCapitalize="none" autoCorrect="off" autoComplete="off" placeholder={placeholder}
        value={text(storage[key])} onChange={event => edit(doc => setOptional(doc, [...path, 'storage', key], event.target.value))} />
    </div>)}</div>
    <label htmlFor={id + '-format'}>{t('文件格式')}<select id={id + '-format'} form={form} value={format} onChange={event => edit(doc => doc.setIn([...path, 'format'], event.target.value))}>
      <option value="parquet">Parquet</option><option value="csv">CSV</option><option value="jsonl">JSON Lines</option>
    </select></label>
    <TextTables type={format} item={item} path={path} edit={edit} form={form} objectKeys />
    {modelId && <ObjectList modelId={modelId} item={item} path={path} edit={edit} />}
  </>;
}

function ObjectList({ modelId, item, path, edit }: { modelId: string; item: Record<string, unknown>; path: string[]; edit: Edit }) {
  const [prefix, setPrefix] = useState('');
  const [table, setTable] = useState('');
  const [result, setResult] = useState<{ objects: string[]; truncated: boolean }>();
  const [error, setError] = useState('');
  const [pending, setPending] = useState(false);
  const request = useRef<AbortController | null>(null);
  const storage = JSON.stringify(item.storage);
  useEffect(() => { setResult(undefined); setError(''); setPending(false); return () => request.current?.abort(); }, [storage, prefix]);
  const tables = record(item.tables);
  const selected = Object.hasOwn(tables, table) ? table : Object.keys(tables)[0] ?? '';
  async function browse() {
    request.current?.abort();
    const controller = new AbortController(); request.current = controller;
    setPending(true); setError(''); setResult(undefined);
    const doc = documentOf('graph: object-browser\nsources: {}\nnodes: {}');
    doc.setIn(['sources', 'storage'], item);
    try {
      const result = await api<{ objects: string[]; truncated: boolean }>('/models/' + modelId + '/browse', { method: 'POST', signal: controller.signal,
        body: JSON.stringify({ yaml: doc.toString(), source: 'storage', object_prefix: prefix }) });
      if (!controller.signal.aborted) setResult(result);
    } catch (cause) { if (!controller.signal.aborted) setError(errorText(cause)); }
    finally { if (!controller.signal.aborted) setPending(false); }
  }
  return <details className="form-disclosure"><summary>{t('浏览桶内对象')}</summary><div className="form-fields">
    <form className="form-fields" aria-label={t('查找存储对象')} onSubmit={event => { event.preventDefault(); void browse(); }}>
      <label>{t('路径前缀')}<input value={prefix} placeholder={t('例如 exports/')} onChange={event => setPrefix(event.target.value)} /></label>
      <button className="button" type="submit" disabled={pending}>{pending ? t('正在读取…') : t('读取对象列表')}</button>
    </form>
    {error && <p role="alert" className="inline-error">{error}</p>}
    {result && <>
      <p className="muted small" role="status">{result.objects.length}{t(' 个对象{0}', result.truncated ? t('，已达到 2000 条上限，请缩小前缀范围') : '')}</p>
      {selected ? <label>{t('添加到表')}<select value={selected} onChange={event => setTable(event.target.value)}>{Object.keys(tables).map(name => <option key={name}>{name}</option>)}</select></label>
        : <p className="muted small">{t('先添加表映射，即可选择对象加入文件列表。')}</p>}
      <ul className="storage-objects">{result.objects.map(object => {
        const files = strings(record(tables[selected]).files);
        const added = files.includes(object);
        return <li key={object}><code>{object}</code><button type="button" className="button compact" disabled={!selected || added} onClick={() => edit(doc => doc.setIn([...path, 'tables', selected, 'files'], [...files.filter(Boolean), object]))}>{added ? t('已添加') : t('添加')}</button></li>;
      })}</ul>
    </>}
  </div></details>;
}
