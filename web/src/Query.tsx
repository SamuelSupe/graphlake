import { useEffect, useMemo, useRef, useState } from 'react';
import { CheckCircle2, Code2, Download, GitFork, Network, Play, Square, Table2 } from 'lucide-react';
import { GraphCanvas, type GraphSelection } from './GraphCanvas';
import { GraphExplorer, type GraphExplorerHandle } from './GraphExplorer';
import { api, ApiError, downloadFile, errorText, request, type GraphSchema, type QueryResult } from './api';

type Dialect = 'cypher' | 'sql' | 'gremlin';
type Statement = { language: Dialect; query: string; parameters_json?: string; timeout_ms?: number; model_revision: string; graph_options?: string; capture_result?: boolean };
type HistoryItem = Statement & { at: number; status: string; needsParameters?: boolean };
type Execution = { page: QueryResult; statement: Statement; requestId: string };
type QueryOutput = {
  result: Execution;
  canvas?: { seed: Execution; additions: number; lastIncluded: string };
};
type SavedQuery = { id: string; name: string; statement: Statement; needsParameters: boolean };

const views = [
  { key: 'graph', label: '图', Icon: Network },
  { key: 'table', label: '表格', Icon: Table2 },
  { key: 'schema', label: '图结构', Icon: GitFork },
  { key: 'raw', label: '原始响应', Icon: Code2 },
] as const;
type ResultView = typeof views[number]['key'];

const defaults = { cypher: 'MATCH (n) RETURN n LIMIT 25', sql: 'SELECT 1', gremlin: 'g.V().limit(25)' };
const historyKey = (id: string) => 'graphlake-query-history-v1:' + id;
function readHistory(id: string): HistoryItem[] {
  try { return JSON.parse(localStorage.getItem(historyKey(id)) ?? '[]'); } catch { return []; }
}
function RawResponse({ page }: { page: QueryResult }) {
  const json = useMemo(() => JSON.stringify(page, null, 2), [page]);
  return <pre className="raw-result" aria-label="原始 JSON 响应" tabIndex={0}>{json}</pre>;
}
const savedKey = (id: string) => 'graphlake-saved-queries-v1:' + id;

export function Query({ id, revision, draftPending, refresh, editSchema, visible }: {
  id: string; revision: string; draftPending: boolean; refresh: () => void; editSchema: () => void; visible: boolean;
}) {
  const [language, setLanguage] = useState<Dialect>('cypher');
  const [queries, setQueries] = useState(defaults);
  const query = queries[language];
  const [parameters, setParameters] = useState('');
  const [timeout, setTimeoutText] = useState('');
  const [defaultTimeout, setDefaultTimeout] = useState<number>();
  const [output, setOutput] = useState<QueryOutput>();
  const result = output?.result;
  const canvas = output?.canvas?.seed;
  const explorer = useRef<GraphExplorerHandle>(null);
  const resultHeading = useRef<HTMLDivElement>(null);
  const revealResult = useRef(false);
  const [graphOptions, setGraphOptions] = useState('');
  const [captureResult, setCaptureResult] = useState(false);
  const [parameterRequired, setParameterRequired] = useState(false);
  const [loadNotice, setLoadNotice] = useState('');
  const [templateName, setTemplateName] = useState('');
  const [keepParameters, setKeepParameters] = useState(false);
  const [templates, setTemplates] = useState<SavedQuery[]>(() => {
    try { return JSON.parse(localStorage.getItem(savedKey(id)) ?? '[]'); } catch { return []; }
  });
  const [history, setHistory] = useState(() => readHistory(id));
  const [view, setView] = useState<ResultView>('schema');
  const [schema, setSchema] = useState<{ revision: string; data?: GraphSchema; error?: string; stale?: boolean }>();
  const [schemaSelection, setSchemaSelection] = useState<GraphSelection>();
  const [schemaAttempt, setSchemaAttempt] = useState(0);
  const [error, setError] = useState<{ message: string; stale?: boolean }>();
  const [exported, setExported] = useState(false);
  const [running, setRunning] = useState<'query' | 'export'>();
  const [stopping, setStopping] = useState(false);
  const active = useRef<{ id: string; controller: AbortController } | null>(null);
  const timeoutValid = !timeout || /^\d+$/.test(timeout) && Number(timeout) >= 1 && Number(timeout) <= 60_000;
  const canRun = !!query.trim() && timeoutValid && (!parameterRequired || language !== 'cypher' || !!parameters.trim());
  const statementFromEditor = (): Statement => ({
    model_revision: revision, language, query,
    parameters_json: language === 'cypher' && parameters.trim() ? parameters : undefined,
    graph_options: graphOptions.trim() || undefined, capture_result: captureResult,
    timeout_ms: timeout ? Number(timeout) : undefined,
  });

  useEffect(() => {
    const controller = new AbortController();
    api<{ timeout_ms: number }>('/options', { signal: controller.signal }).then(value => setDefaultTimeout(value.timeout_ms), () => {});
    return () => {
      controller.abort();
      if (active.current) {
        const { id, controller } = active.current;
        active.current = null;
        void api('/queries/' + id, { method: 'DELETE', keepalive: true }).catch(() => {});
        controller.abort();
      }
    };
  }, []);

  useEffect(() => {
    if (!visible || schema?.revision === revision && schema.data) return;
    const controller = new AbortController();
    api<GraphSchema>('/models/' + id + '/schema/' + encodeURIComponent(revision), { signal: controller.signal })
      .then(data => setSchema({ revision, data }), cause => {
        if (!controller.signal.aborted) setSchema({ revision, error: errorText(cause), stale: cause instanceof ApiError && cause.code === 'active_revision_changed' });
      });
    return () => controller.abort();
  }, [id, revision, visible, schemaAttempt]);

  useEffect(() => {
    if (!revealResult.current) return;
    revealResult.current = false;
    if (visible && window.matchMedia('(max-width: 760px)').matches) {
      const heading = resultHeading.current;
      if (heading && heading.getBoundingClientRect().top >= window.innerHeight * .45) {
        heading.focus({ preventScroll: true });
        heading.scrollIntoView({ block: 'start' });
      }
    }
  }, [result?.requestId, visible]);

  function record(statement: Statement, status: string) {
    const item: HistoryItem = { ...statement, at: Date.now(), status, needsParameters: !!statement.parameters_json };
    setHistory(previous => {
      const next = [item, ...previous].slice(0, 20);
      try { localStorage.setItem(historyKey(id), JSON.stringify(next.map(entry => ({ ...entry, parameters_json: undefined })))); }
      catch { /* session history still works */ }
      return next;
    });
  }

  function load(statement: Statement, needsParameters?: boolean) {
    setLanguage(statement.language);
    setQueries(old => ({ ...old, [statement.language]: statement.query }));
    setParameters(statement.parameters_json ?? '');
    setGraphOptions(statement.graph_options ?? '');
    setCaptureResult(!!statement.capture_result);
    setTimeoutText(statement.timeout_ms?.toString() ?? '');
    // Older history records lack a marker. The server identifies missing names if one is used.
    setParameterRequired(!!needsParameters && !statement.parameters_json);
    setError(undefined);
    setLoadNotice(statement.model_revision !== revision ? '已载入历史语句。再次运行将使用当前生效版本。' : '');
  }

  function saveTemplates(next: SavedQuery[]) {
    try {
      localStorage.setItem(savedKey(id), JSON.stringify(next));
      setTemplates(next);
    } catch {
      setError({ message: '浏览器无法保存查询模板，请检查存储空间。' });
    }
  }

  async function run(operation: 'query' | 'export' = 'query', mode: 'replace' | 'append' | 'replay' = 'replace') {
    if (active.current || operation === 'query' && mode !== 'replay' && !canRun) return;
    const statement = operation === 'export' ? result?.statement : mode === 'replay' ? canvas?.statement : statementFromEditor();
    if (!statement || statement.model_revision !== revision) return;
    const requestId = crypto.randomUUID();
    const controller = new AbortController();
    active.current = { id: requestId, controller };
    setRunning(operation);
    setError(undefined);
    setExported(false);
    const options = { method: 'POST', signal: controller.signal, body: JSON.stringify({
      ...statement, request_id: requestId, capture_graph: operation === 'query' && statement.language !== 'sql',
    }) };
    const endpoint = '/models/' + id + '/' + operation;
    try {
      if (operation === 'query') {
        const page = await api<QueryResult>(endpoint, options);
        if (active.current?.id === requestId) {
          const execution = { page, statement, requestId };
          revealResult.current = mode === 'replace';
          let nextCanvas: QueryOutput['canvas'];
          let showGraph = page.graph.elements.length > 0 || page.graph_next !== undefined;
          if (mode === 'append' && output?.canvas && explorer.current) {
            const issue = showGraph ? explorer.current.append(page) : undefined;
            if (issue) setError({ message: issue });
            showGraph = showGraph && !issue;
            nextCanvas = showGraph ? { ...output.canvas, additions: output.canvas.additions + 1, lastIncluded: requestId } : output.canvas;
          } else if (showGraph) {
            nextCanvas = { seed: execution, additions: 0, lastIncluded: requestId };
          }
          setOutput({ result: execution, canvas: nextCanvas });
          setView(showGraph ? 'graph' : 'table');
          record(statement, page.truncated ? '预览已截断' : '完成');
        }
      } else {
        const { ticket } = await api<{ ticket: string }>(endpoint, options);
        downloadFile('/api/v1/downloads/' + encodeURIComponent(ticket), id + '.arrow');
        setExported(true);
      }
    } catch (cause) {
      if (!controller.signal.aborted && !(cause instanceof DOMException && cause.name === 'AbortError')) {
        if (cause instanceof ApiError && cause.message.startsWith('missing Cypher parameter $')) setParameterRequired(true);
        setError({ message: errorText(cause), stale: cause instanceof ApiError && cause.code === 'active_revision_changed' });
        if (operation === 'query') record(statement, cause instanceof ApiError && cause.code === 'cancelled' ? '已取消' : '失败');
      }
    } finally {
      if (active.current?.id === requestId) {
        active.current = null;
        setRunning(undefined);
        setStopping(false);
      }
    }
  }

  async function downloadSnapshot() {
    if (!result?.page.snapshot) return;
    const path = '/models/' + id + '/results/' + result.page.snapshot;
    try {
      await request(path, { method: 'HEAD' });
      downloadFile('/api/v1' + path, id + '-result.arrow');
    } catch (cause) { setError({ message: errorText(cause) }); }
  }

  async function stop() {
    const pending = active.current;
    if (!pending) return;
    setStopping(true);
    try {
      await api('/queries/' + pending.id, { method: 'DELETE' });
    } catch (cause) {
      if (active.current?.id === pending.id) {
        setStopping(false);
        setError({ message: errorText(cause) });
      }
    }
  }

  const page = result?.page;
  const shownSchema = schema?.revision === revision ? schema : undefined;
  const selectedType = schemaSelection && shownSchema?.data?.[schemaSelection.section][schemaSelection.name];

  return <section className="query-workspace" aria-label="只读查询">
    <div className="query-composer">
    <div className="query-toolbar">
      {running ? <button className="button" disabled={stopping} onClick={() => void stop()}><Square size={16} />{stopping ? '正在取消…' : '取消查询'}</button>
        : <button className="button primary" disabled={!canRun} onClick={() => void run()}><Play size={16} />运行查询</button>}
      {canvas && <button className="button compact" disabled={!!running || !canRun || canvas.page.model_revision !== revision}
        onClick={() => void run('query', 'append')}>运行并追加到画布</button>}
      <label><span className="sr-only">查询语言</span><select value={language} disabled={!!running} onChange={event => setLanguage(event.target.value as Dialect)}>
        <option value="cypher">Cypher</option><option value="sql">SQL</option><option value="gremlin">Gremlin</option>
      </select></label>
      <label className="query-timeout">超时（毫秒）<input type="number" min={1} max={60_000} step={1} value={timeout}
        disabled={!!running} aria-invalid={!timeoutValid} aria-describedby={timeoutValid ? undefined : id + '-query-timeout-error'} placeholder={defaultTimeout ? String(defaultTimeout) : '服务器默认'}
        onChange={event => setTimeoutText(event.target.value)} /></label>
      {draftPending && <span className="query-revision muted">草稿待发布 · 查询使用生效版本</span>}
    </div>
    {!timeoutValid && <p className="inline-error query-notice" id={id + '-query-timeout-error'} role="alert">请输入 1 至 60000 毫秒的整数。</p>}
    <label className="query-input"><span className="sr-only">查询语句</span>
      <textarea value={query} onChange={event => {
        setQueries(old => ({ ...old, [language]: event.target.value }));
        setParameterRequired(false);
      }}
        onKeyDown={event => { if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') { event.preventDefault(); void run(); } }}
        spellCheck={false} rows={4} className="mono" />
    </label>
    <div className="query-settings">
      <div className="query-disclosures">
        {language === 'cypher' && <details className="query-details query-parameters" open={parameterRequired || undefined}><summary>Cypher 参数</summary>
          <label>JSON 对象<textarea value={parameters} onChange={event => setParameters(event.target.value)} placeholder="{}" spellCheck={false} rows={3} className="mono" /></label>
        </details>}
        <details className="query-details query-parameters"><summary>图查询范围{graphOptions.trim() && ' · 已设置'}</summary>
          <label>图查询范围 JSON<textarea value={graphOptions} onChange={event => setGraphOptions(event.target.value)}
            placeholder={'{"snapshot_time":"2025-01-01","logical_partition":[{"column":"tenant_id","op":"eq","value":1}]}'}
            spellCheck={false} rows={3} className="mono" /></label>
          <p className="muted small">可设置业务时间、逻辑分区和一致性要求，适用于 Cypher、SQL 和 Gremlin。</p>
        </details>
      </div>
      <label className="check-label"><input type="checkbox" checked={captureResult} onChange={event => setCaptureResult(event.target.checked)} />保留本次执行的完整结果</label>
      <span className="query-shortcut"><kbd>⌘ / Ctrl</kbd> + <kbd>Enter</kbd> 运行查询</span>
    </div>
    {parameterRequired && language === 'cypher' && !parameters.trim() && <p className="notice query-notice" role="status">此查询需要参数。填写 Cypher 参数后即可运行。</p>}
    {loadNotice && <p className="notice query-notice" role="status">{loadNotice}</p>}
    </div>
    {error && <div className="notice error query-notice" role="alert">{error.message}{error.stale && <button className="text-button" onClick={refresh}>重新加载模型</button>}</div>}
    {exported && <p className="notice query-notice" role="status">完整 Arrow 文件已交给浏览器下载。</p>}
    {running && <p className="query-running" role="status">{running === 'export' ? '正在生成完整结果…' : '正在运行查询…'}</p>}
    <section className="query-output" aria-label="查询结果">
      {page && <div className="result-heading" ref={resultHeading} tabIndex={-1} aria-label="查询完成">
        <span className="result-success"><CheckCircle2 size={16} />查询完成</span>
        <span>{page.rows.length} 行</span>
        <span className="result-metrics">{page.elapsed_ms} ms · <code title={page.model_revision}>版本 {page.model_revision.slice(0, 14)}</code> · <code title={result.requestId}>执行 {result.requestId.slice(0, 8)}</code></span>
      </div>}
      <div className="result-bar">
      <div className="result-tabs" role="tablist" aria-label="查询结果视图">
        {views.map(({ key, label, Icon }, index) => <button key={key} id={id + '-result-tab-' + key}
          type="button" role="tab" aria-selected={view === key} tabIndex={view === key ? 0 : -1}
          onClick={() => setView(key)} onKeyDown={event => {
            let target: number;
            if (event.key === 'ArrowRight') target = (index + 1) % views.length;
            else if (event.key === 'ArrowLeft') target = (index + views.length - 1) % views.length;
            else if (event.key === 'Home') target = 0;
            else if (event.key === 'End') target = views.length - 1;
            else return;
            event.preventDefault();
            const next = views[target].key;
            setView(next);
            event.currentTarget.parentElement?.querySelector<HTMLButtonElement>('[id="' + id + '-result-tab-' + next + '"]')?.focus();
          }}><Icon size={17} aria-hidden="true" />{label}</button>)}
      </div>
      <div className="result-tools">
        {view === 'schema' && <button className="button compact" onClick={editSchema}><GitFork size={15} />编辑图结构</button>}
        {page?.snapshot && <button className="button compact" onClick={() => void downloadSnapshot()}><Download size={15} />
          {page.snapshot_complete ? '下载结果 Arrow' : '下载预览 Arrow'}</button>}
        {page && <button className="button compact" disabled={!!running || page.model_revision !== revision}
          title="按相同语句和模型版本重新读取数据源并导出 Arrow" onClick={() => void run('export')}><Download size={15} />重新执行并导出</button>}
      </div>
      </div>
      {page?.truncated && <p className="notice query-notice">仅显示前 {page.rows.length} 行。{page.snapshot_complete ? '本次执行的完整结果可下载。' : '可在运行前勾选保留完整结果。'}</p>}
      {page && page.model_revision !== revision && <p className="notice query-notice">当前预览来自旧模型版本。重新执行会使用当前生效版本。</p>}
      <div className={'result-panel result-' + view} role="tabpanel" aria-labelledby={id + '-result-tab-' + view}
        tabIndex={view === 'raw' && !page || view === 'graph' && !canvas || view === 'table' && !page ? 0 : undefined}>
        {view === 'schema' && (shownSchema?.data ? <div className="schema-canvas">
          <GraphCanvas model={shownSchema.data} selected={schemaSelection} select={setSchemaSelection} />
          {schemaSelection && selectedType && <aside className="schema-type" aria-label="生效类型详情">
            <strong>{schemaSelection.name}</strong>
            {'from' in selectedType && <p>{selectedType.from.node} → {selectedType.to.node}</p>}
            <p>业务键 <code>{selectedType.key.join(' · ') || '—'}</code></p>
            <p>属性 <code>{selectedType.properties.join(' · ') || '—'}</code></p>
          </aside>}
        </div> : shownSchema?.error ? <div className="empty" role="alert">{shownSchema.error}<button className="button" onClick={() => {
          if (shownSchema.stale) refresh();
          else { setSchema(undefined); setSchemaAttempt(value => value + 1); }
        }}>{shownSchema.stale ? '重新加载模型' : '重试'}</button></div>
          : <p className="empty" role="status">正在读取生效图结构…</p>)}
        {canvas && <div hidden={view !== 'graph'}>
          {output.canvas && canvas.requestId !== result?.requestId && <p className="notice query-notice">
            画布起始于执行 <code title={canvas.requestId}>{canvas.requestId.slice(0, 8)}</code>
            {output.canvas.additions > 0 && '，已追加 ' + output.canvas.additions + ' 次查询'}
            。{output.canvas.lastIncluded !== result?.requestId && '本次执行未向画布加入图元素。'}
            表格和原始响应显示最近一次执行。重跑原查询会重建起始画布。
          </p>}
          <GraphExplorer key={canvas.requestId} ref={explorer} id={id} page={canvas.page}
            schema={schema?.revision === canvas.page.model_revision ? schema.data : undefined} activeRevision={revision}
            timeout={canvas.statement.timeout_ms} busy={!!running} reload={refresh} rerun={() => void run('query', 'replay')} />
        </div>}
        {view === 'graph' && !canvas && <p className="empty">{page
          ? page.graph.truncated ? '图元素超过预览上限，请缩小查询范围。' : '当前结果没有可绘制的图元素。'
          : '运行查询后可在这里查看图元素。'}</p>}
        {view === 'table' && (page ? <>
          <div className="table-scroll" role="region" aria-label="查询结果表格" tabIndex={0}><table>
            <thead><tr>{page.columns.map((column, index) => <th scope="col" key={index}>{column.name}<small>{column.data_type}</small></th>)}</tr></thead>
            <tbody>{page.rows.map((row, index) => <tr key={index}>{row.map((cell, cellIndex) => <td key={cellIndex}>
              <code className={cell === null ? 'null-cell' : ''}>{cell ?? 'null'}</code></td>)}</tr>)}</tbody>
          </table></div>
          {!page.rows.length && <p className="empty">查询未返回行</p>}
        </> : <p className="empty">运行查询后可在这里查看结果。</p>)}
        {view === 'raw' && (page ? <RawResponse page={page} />
          : <p className="empty">运行查询后可在这里查看原始响应。</p>)}
      </div>
      {!!page?.sources.length && <details className="query-details source-versions"><summary>来源版本</summary>
        <dl>{page.sources.map((source, index) => <div key={index}><dt>{source.source}</dt><dd><code>{source.version}</code><small>{source.consistency}</small></dd></div>)}</dl>
      </details>}
    </section>
    <details className="query-details query-history-section"><summary>查询模板（{templates.length}）</summary>
      <form className="query-toolbar template-form" onSubmit={event => {
        event.preventDefault();
        if (!templateName.trim() || !query.trim() || !timeoutValid) return;
        const statement = statementFromEditor();
        const needsParameters = !!statement.parameters_json || language === 'cypher' && parameterRequired;
        if (!keepParameters) delete statement.parameters_json;
        saveTemplates([{ id: crypto.randomUUID(), name: templateName.trim(), statement, needsParameters }, ...templates]);
        setTemplateName(''); setKeepParameters(false);
      }}>
        <label>模板名称<input value={templateName} maxLength={80} onChange={event => setTemplateName(event.target.value)} /></label>
        {language === 'cypher' && <label><input type="checkbox" checked={keepParameters} onChange={event => setKeepParameters(event.target.checked)} />在此浏览器保存参数值</label>}
        <button className="button compact" disabled={!templateName.trim() || !query.trim() || !timeoutValid}>保存查询模板</button>
      </form>
      <ul className="version-list query-history">{templates.map(item => <li key={item.id}>
        <div><strong>{item.name}</strong><small>{item.statement.language} · {item.statement.query}</small></div>
        <button className="button compact" disabled={!!running} onClick={() => load(item.statement, item.needsParameters)}>载入模板</button>
        <button className="button compact" aria-label={'移除模板 ' + item.name} onClick={() => saveTemplates(templates.filter(template => template.id !== item.id))}>移除</button>
      </li>)}</ul>
    </details>
    <details className="query-details query-history-section">
      <summary>查询历史（{history.length}）</summary>
      {!history.length ? <p className="empty">暂无历史查询。</p> : <ol className="version-list query-history">
        {history.map((item, i) => <li key={i}><div><code title={item.query}>{item.query}</code>
          <small>{item.language} · {item.status} · 版本 {item.model_revision.slice(0, 10)} · {new Date(item.at).toLocaleString()}</small></div>
          <button className="button compact" disabled={!!running} onClick={() => load(item, item.needsParameters)}>载入语句</button></li>)}
      </ol>}
    </details>
  </section>;
}
