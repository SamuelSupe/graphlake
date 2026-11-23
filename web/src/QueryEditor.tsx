import { t } from './locale.ts';
import { useEffect, useId, useRef } from 'react';
import { Annotation, Compartment, EditorState } from '@codemirror/state';
import { EditorView, keymap, lineNumbers, highlightActiveLine } from '@codemirror/view';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { bracketMatching, HighlightStyle, StreamLanguage, syntaxHighlighting, syntaxTree } from '@codemirror/language';
import { autocompletion, completionKeymap, startCompletion, type CompletionContext } from '@codemirror/autocomplete';
import { tags } from '@lezer/highlight';
import { sql } from '@codemirror/lang-sql';
import { cypher } from '@codemirror/legacy-modes/mode/cypher';
import { groovy } from '@codemirror/legacy-modes/mode/groovy';
import { keywords, schemaCompletions, type Dialect } from './queryCompletions';
import type { GraphSchema } from './api';

const queryHighlight = HighlightStyle.define([
  { tag: [tags.keyword, tags.operatorKeyword, tags.bool, tags.null], color: '#7dd3fc', fontWeight: '600' },
  { tag: [tags.string, tags.regexp], color: '#6ee7b7' },
  { tag: [tags.number, tags.atom], color: '#fcd34d' },
  { tag: [tags.propertyName, tags.typeName, tags.className], color: '#c4b5fd' },
  { tag: tags.comment, color: '#64748b', fontStyle: 'italic' },
]);

const externalEdit = Annotation.define<boolean>();

export default function QueryEditor({ value, language, schema, change, run }: {
  value: string; language: Dialect; schema?: GraphSchema; change: (value: string) => void; run: () => void;
}) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView>(null);
  const callbacks = useRef({ change, run });
  callbacks.current = { change, run };
  const configuration = useRef(new Compartment());
  const labelId = useId();
  const hintId = useId();

  useEffect(() => {
    const editor = new EditorView({ parent: host.current!, state: EditorState.create({ doc: value, extensions: [
      lineNumbers(), highlightActiveLine(), history(), bracketMatching(), syntaxHighlighting(queryHighlight),
      EditorView.theme({}, { dark: true }),
      EditorView.lineWrapping, EditorState.tabSize.of(2),
      EditorView.contentAttributes.of({ 'aria-labelledby': labelId, 'aria-describedby': hintId, 'aria-multiline': 'true', spellcheck: 'false' }),
      keymap.of([{ key: 'Mod-Enter', run: editor => { if (!editor.composing) callbacks.current.run(); return true; } }, ...completionKeymap, ...defaultKeymap, ...historyKeymap]),
      EditorView.updateListener.of(update => { if (update.docChanged && !update.transactions.some(transaction => transaction.annotation(externalEdit))) callbacks.current.change(update.state.doc.toString()); }),
      configuration.current.of([]),
    ] }) });
    view.current = editor;
    return () => { view.current = null; editor.destroy(); };
  }, []);

  useEffect(() => {
    const editor = view.current;
    if (editor && value !== editor.state.doc.toString()) {
      editor.dispatch({ changes: { from: 0, to: editor.state.doc.length, insert: value }, annotations: externalEdit.of(true) });
    }
  }, [value]);

  useEffect(() => {
    const options = schemaCompletions(schema, language);
    const complete = (context: CompletionContext) => {
      const node = syntaxTree(context.state).resolveInner(context.pos, -1);
      if (/comment|string/i.test(node.name)) return null;
      const word = context.matchBefore(/[\w\u0080-\uffff]*/)!;
      if (!context.explicit && word.from === word.to) return null;
      const prefix = context.state.sliceDoc(Math.max(0, word.from - 1), word.from);
      const candidates = prefix === ':' && language === 'cypher' ? options.filter(item => item.type === 'class')
        : prefix === '.' && language !== 'gremlin' ? options.filter(item => item.type === 'property') : options;
      return { from: word.from, options: [...candidates, ...((prefix === ':' || prefix === '.') && language !== 'gremlin' ? [] : keywords[language].map(label => ({ label, type: 'keyword' })))], validFor: /^[\w\u0080-\uffff]*$/ };
    };
    view.current?.dispatch({ effects: configuration.current.reconfigure([
      language === 'sql' ? sql() : StreamLanguage.define(language === 'cypher' ? cypher : groovy),
      autocompletion({ override: [complete], defaultKeymap: false }),
    ]) });
  }, [language, schema]);

  return <div className="query-code-editor">
    <div className="code-editor-heading"><span id={labelId}>{t('查询语句')}</span>
      <button type="button" className="text-button" onClick={() => { view.current?.focus(); if (view.current) startCompletion(view.current); }}>{t('补全')}</button>
    </div>
    <div ref={host} />
    <p className="code-editor-hint" id={hintId}>{t('Ctrl + Space 补全 · ⌘ / Ctrl + Enter 运行 · Tab 移至下一个控件')}</p>
  </div>;
}
