import { useId, useRef, useState, type ChangeEvent } from 'react';
import { Download, Upload } from 'lucide-react';
import { downloadText, errorText } from './api';
import { t } from './locale.ts';
import { modelOf } from './model';

export function ModelYamlEditor({ id, yaml, error, errorLine, editable, busy, change, close }: {
  id: string;
  yaml: string;
  error?: string;
  errorLine?: number;
  editable: boolean;
  busy: boolean;
  change: (value: string) => void;
  close?: () => void;
}) {
  const errorId = useId();
  const textarea = useRef<HTMLTextAreaElement>(null);
  const [importError, setImportError] = useState('');
  const [importing, setImporting] = useState(false);

  async function importYaml(event: ChangeEvent<HTMLInputElement>) {
    const input = event.currentTarget;
    const file = input.files?.[0];
    input.value = '';
    if (!file) return;
    setImportError('');
    setImporting(true);
    try {
      if (file.size > 1024 * 1024) throw new Error(t('YAML 文件超过 1 MiB'));
      const next = await file.text();
      modelOf(next);
      const editor = textarea.current;
      if (input.isConnected && editor && !editor.readOnly && editor.value !== next
        && window.confirm(t('将当前编辑内容替换为导入的 YAML？'))) change(next);
    } catch (cause) {
      setImportError(errorText(cause));
    } finally {
      setImporting(false);
    }
  }

  function locateError(line: number) {
    const editor = textarea.current;
    if (!editor) return;
    const lines = editor.value.split('\n');
    const index = Math.min(line, lines.length) - 1;
    const start = lines.slice(0, index).reduce((offset, value) => offset + value.length + 1, 0);
    editor.focus();
    editor.setSelectionRange(start, start + lines[index].length);
    const lineHeight = Number.parseFloat(getComputedStyle(editor).lineHeight);
    editor.scrollTop = Math.max(0, (index - 2) * lineHeight);
    editor.scrollLeft = 0;
  }

  const files = <>
    {editable && <label className="button compact"><Upload size={15} aria-hidden="true" />{t('导入 YAML 到草稿')}
      <input className="sr-only" type="file" accept=".yaml,.yml" disabled={busy || importing} onChange={importYaml} />
    </label>}
    <button type="button" className="button compact" onClick={() => downloadText(yaml, id + '.yaml')}>
      <Download size={15} aria-hidden="true" />{t('下载草稿 YAML')}
    </button>
  </>;

  return <section className="panel-layout yaml-editor" aria-label={t('模型 YAML')}>
    {!close && <header className="yaml-file-actions">{files}</header>}
    <div className="panel-body">
      <div aria-live="polite" className="yaml-diagnostic">
        {error && <p id={errorId} className="inline-error">{error}</p>}
      </div>
      {errorLine !== undefined && <button type="button" className="text-button yaml-locate"
        onClick={() => locateError(errorLine)}>{t('定位到第 {0} 行', errorLine)}</button>}
      <label><span className="sr-only">{t('YAML 配置')}</span>
        <textarea ref={textarea} value={yaml} readOnly={!editable || busy} onChange={event => {
          setImportError('');
          change(event.target.value);
        }} aria-invalid={!!error} aria-describedby={error ? errorId : undefined}
          spellCheck={false} autoCapitalize="off" autoComplete="off" wrap="off" className="mono" rows={16} data-initial-focus />
      </label>
      {importError && <p className="inline-error" role="alert">{importError}</p>}
    </div>
    {close && <footer className="panel-footer">
      {files}
      <button type="button" className="button primary" onClick={close}>{t('完成')}</button>
    </footer>}
  </section>;
}
