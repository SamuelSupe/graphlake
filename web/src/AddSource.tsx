import { t } from './locale.ts';
import { useId, useLayoutEffect, useRef, useState, type FormEvent } from 'react';
import { ArrowLeft } from 'lucide-react';
import { errorText } from './api';
import { ConnectorGallery } from './Catalogs';
import { connectorDetails, connectorFields, newConnector } from './connectors';
import type { Edit } from './Inspector';
import { documentOf, record, sourceLabel } from './model';
import { SourceConfig } from './SourceConfig';
import { Modal } from './ui';

export function AddSource({ close, submit, modelId }: {
  modelId: string;
  close: () => void;
  submit: (name: string, source: Record<string, unknown>) => void;
}) {
  const [type, setType] = useState<string>();
  const [choosing, setChoosing] = useState(true);
  const stages = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    stages.current?.querySelector<HTMLElement>('[data-initial-focus]')?.focus();
  }, [choosing, type]);

  return <Modal title={choosing || !type ? t('选择数据源连接器') : t('配置 {0}', sourceLabel(type))}
    size={choosing ? 'wide' : 'medium'} close={close}>
    <div className="panel-layout" ref={stages}>
      <div className="panel-body" hidden={!choosing}>
        <ConnectorGallery active={choosing} add={next => { setType(next); setChoosing(false); }} />
      </div>
      {type && <SourceDraft key={type} modelId={modelId} type={type} hidden={choosing}
        back={() => setChoosing(true)} close={close} submit={submit} />}
    </div>
  </Modal>;
}

function SourceDraft({ type, close, submit, modelId, hidden, back }: {
  modelId: string; type: string; hidden: boolean; back: () => void;
  close: () => void; submit: (name: string, source: Record<string, unknown>) => void;
}) {
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
  return <div className="panel-layout source-create" hidden={hidden}>
    <div className="panel-body form-fields">
      <button className="text-button source-back" type="button" onClick={back}>
        <ArrowLeft size={15} aria-hidden="true" />{t('返回连接器列表')}
      </button>
      <p className="muted">{connectorDetails[type]?.description}</p>
      <form id={form} onSubmit={send} className="form-fields" aria-label={t('新建数据源')}>
        <label htmlFor={form + '-name'}>{t('数据源名称 ')}<span className="field-requirement">{t('必填')}</span></label>
        <p id={form + '-name-help'} className="muted small">{t('在当前模型内唯一，不能包含点号。')}</p>
        <input id={form + '-name'} name="source-name" className="mono" ref={nameInput} data-initial-focus={!hidden || undefined} required maxLength={128}
          pattern="(?=.*\S)[^.]+" placeholder={t('例如 analytics')} value={name} spellCheck={false} autoCapitalize="none" autoCorrect="off" autoComplete="off"
          onInvalid={event => setError(event.currentTarget.validity.valueMissing ? t('请输入数据源名称。') : t('名称不能只含空格或包含点号。'))}
          aria-invalid={!!error} aria-describedby={form + '-name-help' + (error ? ' ' + form + '-error' : '')}
          onChange={event => { setName(event.target.value); setError(''); }} />
      </form>
      <div className="source-create-config form-fields"><SourceConfig modelId={modelId} type={type} item={source} path={['source']} edit={edit} form={form} /></div>
      {error && <p id={form + '-error'} className="inline-error" role="alert">{error}</p>}
    </div>
    <footer className="panel-footer">
      <p className="panel-footer-context">{t('添加后保存草稿、校验并发布以生效。')}{connectorFields[type].some(field => field.key.endsWith('_env')) && <> <br />{t('凭据填写服务端环境变量名，不填写密码或令牌原文。')}</>}</p>
      <button type="button" className="button" onClick={close}>{t('取消')}</button>
      <button type="submit" form={form} className="button primary">{t('添加到草稿')}</button>
    </footer>
  </div>;
}
