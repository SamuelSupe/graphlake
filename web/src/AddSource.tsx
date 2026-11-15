import { useId, useRef, useState, type FormEvent } from 'react';
import { errorText } from './api';
import { ConnectorGallery } from './Catalogs';
import { connectorDetails, connectorFields, newConnector } from './connectors';
import type { Edit } from './Inspector';
import { documentOf, record, sourceLabel } from './model';
import { SourceConfig } from './SourceConfig';
import { Modal } from './ui';

export function AddSource({ initialType, close, submit, modelId }: {
  initialType?: string;
  modelId: string;
  close: () => void;
  submit: (name: string, source: Record<string, unknown>) => void;
}) {
  const [type, setType] = useState(initialType);
  return <Modal title={type ? '配置 ' + sourceLabel(type) : '选择数据源连接器'} size={type ? 'medium' : 'wide'} close={close}>
    {type ? <SourceDraft modelId={modelId} type={type} close={close} submit={submit} />
      : <div className="panel-body"><ConnectorGallery add={setType} /></div>}
  </Modal>;
}

function SourceDraft({ type, close, submit, modelId }: { modelId: string; type: string; close: () => void; submit: (name: string, source: Record<string, unknown>) => void }) {
  const form = useId();
  const nameInput = useRef<HTMLInputElement>(null);
  const [name, setName] = useState('');
  const [error, setError] = useState('');
  const [yaml, setYaml] = useState(() => {
    const doc = documentOf('source: {}');
    doc.set('source', newConnector(type));
    return doc.toString();
  });
  const source = record(documentOf(yaml).toJS().source);
  const edit: Edit = change => setYaml(current => {
    const doc = documentOf(current);
    change(doc);
    return doc.toString();
  });
  function send(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!name.trim()) return;
    try { submit(name.trim(), source); close(); }
    catch (cause) { setError(errorText(cause)); nameInput.current?.focus(); }
  }
  return <div className="panel-layout source-create">
    <div className="panel-body form-fields">
      <p className="muted">{connectorDetails[type]?.description}</p>
      <form id={form} onSubmit={send} className="form-fields" aria-label="新建数据源">
        <label htmlFor={form + '-name'}>数据源名称 <span className="field-requirement">必填</span></label>
        <p id={form + '-name-help'} className="muted small">在当前模型内唯一，不能包含点号。</p>
        <input id={form + '-name'} name="source-name" className="mono" ref={nameInput} data-initial-focus autoFocus required maxLength={128}
          pattern="(?=.*\S)[^.]+" placeholder="例如 analytics" value={name} spellCheck={false} autoCapitalize="none" autoCorrect="off" autoComplete="off"
          onInvalid={event => setError(event.currentTarget.validity.valueMissing ? '请输入数据源名称。' : '名称不能只含空格或包含点号。')}
          aria-invalid={!!error} aria-describedby={form + '-name-help' + (error ? ' ' + form + '-error' : '')}
          onChange={event => { setName(event.target.value); setError(''); }} />
      </form>
      <div className="source-create-config form-fields"><SourceConfig modelId={modelId} type={type} item={source} path={['source']} edit={edit} form={form} /></div>
      {error && <p id={form + '-error'} className="inline-error" role="alert">{error}</p>}
    </div>
    <footer className="panel-footer">
      <p className="panel-footer-context">添加后保存草稿、校验并发布以生效。{connectorFields[type].some(field => field.key.endsWith('_env')) && '凭据填写服务端环境变量名，不填写密码或令牌原文。'}</p>
      <button type="button" className="button" onClick={close}>取消</button>
      <button type="submit" form={form} className="button primary">添加到草稿</button>
    </footer>
  </div>;
}
