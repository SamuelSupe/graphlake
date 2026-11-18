import { locale, t } from './locale.ts';
import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { Code2, Download, GitFork, Network, Play, Square, Table2 } from 'lucide-react';
import type { GraphSelection } from './GraphCanvas';
import { queryParameterError } from './queryParameters';
import { GraphExplorer, type GraphExplorerHandle } from './GraphExplorer';
import { api, ApiError, downloadFile, errorText, request, type FreshnessPolicy, type GraphSchema, type QueryResult } from './api';

const QueryEditor = lazy(() => import('./QueryEditor'));
const QueryScope = lazy(() => import('./QueryScope'));
const GraphCanvas = lazy(() => import('./GraphCanvas'));

type Dialect = 'cypher' | 'sql' | 'gremlin';
type Statement = { language: Dialect; query: string; parameters_json?: string; timeout_ms?: number; model_revision: string; graph_options?: string; capture_result?: boolean; csr_generation?: string; freshness?: FreshnessPolicy };
type HistoryItem = Statement & { at: number; status: string; needsParameters?: boolean };
type Execution = { page: QueryResult; statement?: Statement; requestId: string };
type Recoverable = { id: string; revision: string };
type QueryMode = 'replace' | 'append' | 'replay';
type PublishedSnapshot = { generation: string; model_revision: string; graph_options: QueryResult['graph_options'] };
const recoveryPrefix = 'graphlake-pending-query-v1:';
function canRetryRequest(cause: unknown) {
  return !(cause instanceof ApiError) || ['auth_required', 'draining', 'cluster_storage'].includes(cause.code ?? '')
    || !cause.code && [502, 503, 504].includes(cause.status ?? 0);
}
function hasRestrictedScope(options?: QueryResult['graph_options']) {
  return !options || !!options.snapshot_time || !!options.require_consistent_snapshot
    || Array.isArray(options.logical_partition) && options.logical_partition.length > 0;
}
function readPending(model: string): Recoverable | undefined {
  try {
    const value = JSON.parse(sessionStorage.getItem(recoveryPrefix + model) ?? 'null');
    if (typeof value?.revision === 'string' && /^[0-9a-f-]{36}$/i.test(value.id)) return value;
  } catch { /* Recovery can still use the in-memory request ID. */ }
}
export function clearPendingQueries() {
  try {
    for (let i = sessionStorage.length - 1; i >= 0; i--) {
      const key = sessionStorage.key(i);
      if (key?.startsWith(recoveryPrefix)) sessionStorage.removeItem(key);
    }
  } catch { /* The browser may block session storage. */ }
}
type QueryOutput = {
  result: Execution;
  canvas?: { seed: Execution; additions: number; lastIncluded: string };
};
type SavedQuery = { id: string; name: string; statement: Statement; needsParameters: boolean };

const views = [
  { key: 'graph', label: t('图'), compactLabel: t('图'), Icon: Network },
  { key: 'table', label: t('表格'), compactLabel: t('表格'), Icon: Table2 },
  { key: 'schema', label: t('图结构'), compactLabel: t('结构视图'), Icon: GitFork },
  { key: 'raw', label: t('原始响应'), compactLabel: t('原始 JSON'), Icon: Code2 },
] as const;
type ResultView = typeof views[number]['key'];

const defaults = { cypher: 'MATCH (n) RETURN n LIMIT 25', sql: 'SELECT 1', gremlin: 'g.V().limit(25)' };
const actorKey = (id: string, actor: string) => actor === 'admin' || actor === '@dev' ? id : actor + ':' + id;
const historyKey = (id: string) => 'graphlake-query-history-v1:' + id;
function readHistory(id: string): HistoryItem[] {
  try { return JSON.parse(localStorage.getItem(historyKey(id)) ?? '[]'); } catch { return []; }
}
function RawResponse({ page }: { page: QueryResult }) {
  const json = useMemo(() => JSON.stringify(page, null, 2), [page]);
  return <pre className="raw-result" aria-label={t('原始 JSON 响应')} tabIndex={0}>{json}</pre>;
}
const savedKey = (id: string) => 'graphlake-saved-queries-v1:' + id;

export function Query({ id, actor, revision, draftPending, refresh, editSchema, visible, canExport }: {
  id: string; actor: string; revision: string; draftPending: boolean; refresh: () => void; editSchema?: () => void; visible: boolean; canExport: boolean;
}) {
  const scope = actorKey(id, actor);
  const [language, setLanguage] = useState<Dialect>('cypher');
  const [queries, setQueries] = useState(defaults);
  const query = queries[language];
  const [parameters, setParameters] = useState('');
  const [parameterError, setParameterError] = useState('');
  const [timeout, setTimeoutText] = useState('');
  const [defaultTimeout, setDefaultTimeout] = useState<number>();
  const [recoverable, setRecoverable] = useState(false);
  const [pending, setPending] = useState(() => readPending(scope));
  const pendingStatement = useRef<{ statement: Statement; mode: QueryMode }>(null);
  const [snapshot, setSnapshot] = useState<{ generation: string; revision: string }>();
  const generation = snapshot?.revision === revision ? snapshot.generation : undefined;
  const [output, setOutput] = useState<QueryOutput>();
  const result = output?.result;
  const canvas = output?.canvas?.seed;
  const explorer = useRef<GraphExplorerHandle>(null);
  const resultHeading = useRef<HTMLDivElement>(null);
  const revealResult = useRef(false);
  const [graphOptions, setGraphOptions] = useState('');
  const [editingScope, setEditingScope] = useState(false);
  const [captureResult, setCaptureResult] = useState(false);
  const [freshness, setFreshness] = useState<FreshnessPolicy>({});
  const [parameterRequired, setParameterRequired] = useState(false);
  const [loadNotice, setLoadNotice] = useState('');
  const [templateName, setTemplateName] = useState('');
  const [keepParameters, setKeepParameters] = useState(false);
  const [templates, setTemplates] = useState<SavedQuery[]>(() => {
    try { return JSON.parse(localStorage.getItem(savedKey(scope)) ?? '[]'); } catch { return []; }
  });
  const [history, setHistory] = useState(() => readHistory(scope));
  const [view, setView] = useState<ResultView>('schema');
  const [schema, setSchema] = useState<{ revision: string; data?: GraphSchema; error?: string; stale?: boolean }>();
  const [schemaSelection, setSchemaSelection] = useState<GraphSelection>();
  const [schemaAttempt, setSchemaAttempt] = useState(0);
  const [error, setError] = useState<{ message: string; stale?: boolean }>();
  const [exported, setExported] = useState(false);
  const [running, setRunning] = useState<'query' | 'export'>();
  const [stopping, setStopping] = useState(false);
  const active = useRef<{ id: string; controller: AbortController } | null>(null);
  const composer = useRef<HTMLDivElement>(null);
  const optionsPanel = useRef<HTMLDetailsElement>(null);
  const parameterInput = useRef<HTMLTextAreaElement>(null);
  const timeoutValid = !timeout || /^\d+$/.test(timeout) && Number(timeout) >= 1 && Number(timeout) <= 60_000;
  const agesValid = [freshness.max_observation_age_seconds, freshness.max_reconciliation_age_seconds].every(age => age === undefined || Number.isInteger(age) && age >= 1 && age <= 2592000);
  const canRun = !!query.trim() && timeoutValid && agesValid && (!parameterRequired || language !== 'cypher' || !!parameters.trim());
  const statementFromEditor = (): Statement => ({
    model_revision: revision, language, query,
    parameters_json: language === 'cypher' && parameters.trim() ? parameters : undefined,
    graph_options: generation ? undefined : graphOptions.trim() || undefined, capture_result: canExport && captureResult,
    csr_generation: language === 'cypher' ? generation : undefined,
    freshness: { ...freshness, require_current_csr: language === 'cypher' ? freshness.require_current_csr : undefined },
    timeout_ms: timeout ? Number(timeout) : undefined,
  });

  useEffect(() => {
    const controller = new AbortController();
    api<{ timeout_ms: number; query_recovery: boolean }>('/options', { signal: controller.signal }).then(value => {
      setDefaultTimeout(value.timeout_ms);
      setRecoverable(value.query_recovery);
    }, () => {});
    return () => {
      controller.abort();
      if (active.current) {
        const { controller } = active.current;
        active.current = null;
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
      try { localStorage.setItem(historyKey(scope), JSON.stringify(next.map(entry => ({ ...entry, parameters_json: undefined })))); }
      catch { /* session history still works */ }
      return next;
    });
  }

  function load(statement: Statement, needsParameters?: boolean) {
    setLanguage(statement.language);
    setQueries(old => ({ ...old, [statement.language]: statement.query }));
    setParameters(statement.parameters_json ?? '');
    setParameterError('');
    setGraphOptions(statement.graph_options ?? '');
    setFreshness(statement.freshness ?? {});
    setSnapshot(statement.csr_generation ? { generation: statement.csr_generation, revision: statement.model_revision } : undefined);
    setCaptureResult(!!statement.capture_result);
    setTimeoutText(statement.timeout_ms?.toString() ?? '');
    // Older history records lack a marker. The server identifies missing names if one is used.
    setParameterRequired(!!needsParameters && !statement.parameters_json);
    setError(undefined);
    setLoadNotice(statement.model_revision !== revision ? t('已载入历史语句。再次运行将使用当前生效版本。') : '');
    requestAnimationFrame(() => {
      const missingParameters = statement.language === 'cypher' && needsParameters && !statement.parameters_json;
      if (missingParameters && optionsPanel.current) optionsPanel.current.open = true;
      const input = missingParameters ? parameterInput.current
        : composer.current?.querySelector<HTMLElement>('[contenteditable="true"], textarea');
      input?.focus({ preventScroll: true });
      input?.scrollIntoView({ block: 'center' });
    });
  }

  function remember(value?: Recoverable) {
    setPending(value);
    if (!value) pendingStatement.current = null;
    try {
      if (value) sessionStorage.setItem(recoveryPrefix + scope, JSON.stringify(value));
      else sessionStorage.removeItem(recoveryPrefix + scope);
    } catch { /* Recovery stays available in this page. */ }
  }

  function showResult(page: QueryResult, requestId: string, statement?: Statement, mode: QueryMode = 'replace') {
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
    if (statement) record(statement, page.truncated ? t('预览已截断') : t('完成'));
  }

  async function recover() {
    if (!pending || active.current) return;
    const { id: requestId } = pending;
    const controller = new AbortController();
    active.current = { id: requestId, controller };
    setRunning('query');
    setError(undefined);
    try {
      while (!controller.signal.aborted) {
        const response = await request('/queries/' + encodeURIComponent(requestId), { signal: controller.signal });
        if (response.status === 202) {
          await new Promise(resolve => window.setTimeout(resolve, 700));
          continue;
        }
        const page: QueryResult = await response.json();
        if (active.current?.id !== requestId) return;
        if (page.model_revision !== pending.revision || !page.graph || !Array.isArray(page.rows)) {
          remember();
          setError({ message: t('找回的任务与原查询不匹配。') });
          return;
        }
        const original = pendingStatement.current;
        showResult(page, requestId, original?.statement, original?.mode);
        remember();
        return;
      }
    } catch (cause) {
      if (!controller.signal.aborted) {
        if (!canRetryRequest(cause)) remember();
        setError({ message: cause instanceof ApiError && cause.status === 404 ? t('原查询已过期或未提交。可重新运行。') : errorText(cause) });
      }
    } finally {
      if (active.current?.id === requestId) {
        active.current = null;
        setRunning(undefined);
        setStopping(false);
      }
    }
  }

  async function selectSnapshot() {
    try {
      const info = await api<PublishedSnapshot>('/models/' + id + '/csr');
      if (info.model_revision === revision) {
        setSnapshot({ generation: info.generation, revision });
        setError(undefined);
      }
    } catch (cause) {
      setError({ message: cause instanceof ApiError && cause.status === 404 ? t('当前模型没有已发布的 CSR 代次。') : errorText(cause) });
    }
  }

  function saveTemplates(next: SavedQuery[]) {
    try {
      localStorage.setItem(savedKey(scope), JSON.stringify(next));
      setTemplates(next);
    } catch {
      setError({ message: t('浏览器无法保存查询模板，请检查存储空间。') });
    }
  }

  async function run(operation: 'query' | 'export' = 'query', mode: QueryMode = 'replace') {
    if (active.current || pending || operation === 'export' && !canExport || operation === 'query' && mode !== 'replay' && !canRun) return;
    if (operation === 'query' && mode !== 'replay' && language === 'cypher') {
      const problem = queryParameterError(parameters);
      if (problem) {
        setParameterError(problem);
        if (optionsPanel.current) optionsPanel.current.open = true;
        parameterInput.current?.focus();
        return;
      }
    }
    let statement = operation === 'export' ? result?.statement : mode === 'replay' ? canvas?.statement : statementFromEditor();
    if (!statement || statement.model_revision !== revision && !(mode === 'replay' && statement.csr_generation)) return;
    const requestId = crypto.randomUUID();
    const controller = new AbortController();
    active.current = { id: requestId, controller };
    setRunning(operation);
    setError(undefined);
    setExported(false);
    const endpoint = '/models/' + id + '/' + operation;
    let submitted = false;
    try {
      if (mode === 'replay' && statement.csr_generation) {
        const published = await api<PublishedSnapshot>('/models/' + id + '/csr', { signal: controller.signal });
        if (published.model_revision !== revision || hasRestrictedScope(published.graph_options) || hasRestrictedScope(canvas?.page.graph_options)) {
          setError({ message: t('请在编辑器确认当前 CSR 的范围后运行。') });
          return;
        }
        statement = { ...statement, csr_generation: published.generation, model_revision: revision };
        setSnapshot({ generation: published.generation, revision });
      }
      if (operation === 'query' && recoverable) {
        pendingStatement.current = { statement, mode };
        remember({ id: requestId, revision: statement.model_revision });
      }
      const options = { method: 'POST', signal: controller.signal, body: JSON.stringify({
        ...statement, capture_result: canExport && statement.capture_result, request_id: requestId, capture_graph: operation === 'query' && statement.language !== 'sql',
      }) };
      submitted = true;
      if (operation === 'query') {
        const page = await api<QueryResult>(endpoint, options);
        if (active.current?.id === requestId) {
          showResult(page, requestId, statement, mode);
          remember();
        }
      } else {
        const { ticket } = await api<{ ticket: string }>(endpoint, options);
        downloadFile('/api/v1/downloads/' + encodeURIComponent(ticket), id + '.arrow');
        setExported(true);
      }
    } catch (cause) {
      if (!controller.signal.aborted && !(cause instanceof DOMException && cause.name === 'AbortError')) {
        const canRecover = submitted && operation === 'query' && recoverable
          && (canRetryRequest(cause) || cause instanceof ApiError && cause.code === 'timeout');
        if (operation === 'query' && !canRecover) remember();
        if (cause instanceof ApiError && cause.message.startsWith('missing Cypher parameter $')) setParameterRequired(true);
        setError({ message: errorText(cause), stale: cause instanceof ApiError && cause.code === 'active_revision_changed' });
        if (submitted && operation === 'query' && !canRecover) record(statement, cause instanceof ApiError && cause.code === 'cancelled' ? t('已取消') : t('失败'));
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
    if (!canExport || !result?.page.snapshot) return;
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

  return <section className="query-workspace" aria-label={t('只读查询')}>
    <div className="query-input-column">
      <div className="query-composer" ref={composer}>
        <div className="query-toolbar">
          {running ? <button className="button" disabled={stopping} onClick={() => void stop()}><Square size={16} />{stopping ? t('正在取消…') : t('取消查询')}</button>
            : <button className="button primary" disabled={!canRun || !!pending} onClick={() => void run()}><Play size={16} />{t('运行查询')}</button>}
          {canvas && <button className="button compact" disabled={!!running || !!pending || !canRun || canvas.page.model_revision !== revision}
            onClick={() => void run('query', 'append')}>{t('运行并追加到画布')}</button>}
          <label><span className="sr-only">{t('查询语言')}</span><select value={language} disabled={!!running} onChange={event => { setLanguage(event.target.value as Dialect); setSnapshot(undefined); }}>
            <option value="cypher">Cypher</option><option value="sql">SQL</option><option value="gremlin">Gremlin</option>
          </select></label>
          {draftPending && <span className="query-revision muted">{t('草稿待发布 · 查询使用生效版本')}</span>}
        </div>
        {!timeoutValid && <p className="inline-error query-notice" id={id + '-query-timeout-error'} role="alert">{t('请输入 1 至 60000 毫秒的整数。')}</p>}
        <Suspense fallback={<label className="query-input">{t('查询语句')}<textarea value={query} onChange={event => setQueries(old => ({ ...old, [language]: event.target.value }))} className="mono" rows={4} /></label>}>
          <QueryEditor key={language} value={query} language={language} schema={schema?.revision === revision ? schema.data : undefined}
            change={value => { setQueries(old => ({ ...old, [language]: value })); setParameterRequired(false); }} run={() => void run()} />
        </Suspense>
        <div className="query-settings">
          <div className="query-disclosures">
            <button className="button compact scope-button" disabled={!!generation || !!running} onClick={() => setEditingScope(true)}>{t('查询范围')}{graphOptions.trim() && !generation ? t(' · 已设置') : ''}
            </button>
            <details ref={optionsPanel} className="query-details query-parameters" open={parameterRequired && language === 'cypher' || !timeoutValid || undefined}>
              <summary>{t('查询选项')}{(timeout || captureResult || generation || language === 'cypher' && parameters.trim()) ? t(' · 已设置') : ''}</summary>
              <div className="query-options-grid">
                {language === 'cypher' && <label>{t('Cypher 参数（JSON）')}<textarea ref={parameterInput} value={parameters} onChange={event => { setParameters(event.target.value); setParameterError(''); }}
                  onBlur={event => setParameterError(queryParameterError(event.target.value))} placeholder="{}" spellCheck={false} rows={3} className="mono"
                  aria-invalid={!!parameterError || undefined} aria-describedby={parameterError ? id + '-parameter-error' : parameterRequired && !parameters.trim() ? id + '-query-parameters-hint' : undefined} />
                  {parameterError && <span className="inline-error" id={id + '-parameter-error'} role="status">{parameterError}</span>}</label>}
                <div className="form-fields execution-options">
                <label className="query-timeout">{t('超时（毫秒）')}<input type="number" min={1} max={60_000} step={1} value={timeout}
                  disabled={!!running} aria-invalid={!timeoutValid} aria-describedby={timeoutValid ? undefined : id + '-query-timeout-error'} placeholder={defaultTimeout ? String(defaultTimeout) : t('服务器默认')}
                  onChange={event => setTimeoutText(event.target.value)} /></label>
                {canExport && <label className="check-label"><input type="checkbox" checked={captureResult}
                  onChange={event => setCaptureResult(event.target.checked)} />{t('保留完整结果')}</label>}
                <label>{t('来源观察最大年龄（秒）')}<input type="number" min={1} max={2592000} placeholder={t('模型默认')} value={freshness.max_observation_age_seconds ?? ''} onChange={event => setFreshness(old => ({...old, max_observation_age_seconds: event.target.value ? event.target.valueAsNumber : undefined}))} /></label>
                <label>{t('全量对账最大年龄（秒）')}<input type="number" min={1} max={2592000} placeholder={t('模型默认')} value={freshness.max_reconciliation_age_seconds ?? ''} onChange={event => setFreshness(old => ({...old, max_reconciliation_age_seconds: event.target.value ? event.target.valueAsNumber : undefined}))} /></label>
                {!agesValid && <p role="alert">{t('有效范围为 1 至 2592000 秒。')}</p>}
                {language === 'cypher' && <label className="check-label"><input type="checkbox" checked={!!freshness.require_current_csr} onChange={event => setFreshness(old => ({...old, require_current_csr: event.target.checked}))} />{t('要求 CSR 与当前副本一致')}</label>}
                {recoverable && language === 'cypher' && (generation
                  ? <button className="button compact" disabled={!!running} onClick={() => setSnapshot(undefined)}>{t('切换至实时来源')}</button>
                  : <button className="button compact" disabled={!!running} onClick={() => void selectSnapshot()}>{t('使用已发布 CSR')}</button>)}
                </div>
              </div>
            </details>
          </div>
        </div>
        {parameterRequired && language === 'cypher' && !parameters.trim() && <p className="notice query-notice" id={id + '-query-parameters-hint'} role="status">{t('此查询需要参数。填写 Cypher 参数后即可运行。')}</p>}
        {loadNotice && <p className="notice query-notice" role="status">{loadNotice}</p>}
      </div>
      {visible && editingScope && <Suspense><QueryScope value={graphOptions} save={setGraphOptions} close={() => setEditingScope(false)} /></Suspense>}
      {error && <div className="notice error query-notice" role="alert">{error.message}{error.stale && <button className="text-button" onClick={refresh}>{t('重新加载模型')}</button>}</div>}
      {pending && !running && <div className="notice query-notice" role="status">{t('检测到未确认的查询。')}
        <button className="text-button" onClick={() => void recover()}>{t('找回原查询')}</button>
        <button className="text-button" onClick={() => remember()}>{t('忽略')}</button></div>}
      {exported && <p className="notice query-notice" role="status">{t('完整 Arrow 文件已交给浏览器下载。')}</p>}
      {running && <p className="query-running" role="status">{running === 'export' ? t('正在生成完整结果…') : t('正在运行查询…')}</p>}
    </div>
    <section className="query-output" aria-label={t('查询结果')}>
      {page && <div className="result-heading" ref={resultHeading} tabIndex={-1} aria-label={t('查询完成')}>
        <strong>{t('结果行数 {0}', page.rows.length)}</strong>
        <span>{page.elapsed_ms} ms</span>
        <code className="result-metrics" title={page.model_revision}>{t('版本 {0}', page.model_revision.slice(0, 10))}</code>
      </div>}
      <div className="result-bar">
      <div className="result-tabs" role="tablist" aria-label={t('查询结果视图')}>
        {views.map(({ key, label, compactLabel, Icon }, index) => <button key={key} id={id + '-result-tab-' + key}
          type="button" role="tab" aria-label={label} aria-controls={id + '-result-view'} aria-selected={view === key} tabIndex={view === key ? 0 : -1}
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
          }}><Icon size={17} aria-hidden="true" /><span className="label-full">{label}</span><span className="label-compact">{compactLabel}</span></button>)}
      </div>
      <div className="result-tools">
        {view === 'schema' && editSchema && <button className="button compact" onClick={editSchema}><GitFork size={15} />{t('编辑图结构')}</button>}
        {canExport && page?.snapshot && <button className="button compact" onClick={() => void downloadSnapshot()}><Download size={15} />
          {page.snapshot_complete ? t('下载结果 Arrow') : t('下载预览 Arrow')}</button>}
        {canExport && page && <button className="button compact" disabled={!result?.statement || !!running || !!pending || page.model_revision !== revision}
          title={t('按相同语句和模型版本重新读取数据源并导出 Arrow')} onClick={() => void run('export')}><Download size={15} />{t('重新执行并导出')}</button>}
      </div>
      </div>
      {page?.truncated && <p className="notice query-notice">{t('仅显示前 {0} 行。{1}', page.rows.length, page.snapshot_complete ? t('本次执行的完整结果可下载。') : t('可在运行前勾选保留完整结果。'))}</p>}
      {result && !result.statement && <p className="notice query-notice">{t('已取回原查询结果。重新运行时请在编辑器输入语句。')}</p>}
      {page && page.model_revision !== revision && <p className="notice query-notice">{t('当前预览来自旧模型版本。重新执行会使用当前生效版本。')}</p>}
      <div id={id + '-result-view'} className={'result-panel result-' + view} role="tabpanel" aria-labelledby={id + '-result-tab-' + view}
        tabIndex={view === 'raw' && !page || view === 'graph' && !canvas || view === 'table' && !page ? 0 : undefined}>
        {view === 'schema' && (shownSchema?.data ? <div className="schema-canvas">
          <Suspense fallback={<p className="empty" role="status">{t('正在打开图结构…')}</p>}>
            <GraphCanvas model={shownSchema.data} selected={schemaSelection} select={setSchemaSelection} />
          </Suspense>
          {schemaSelection && selectedType && <aside className="schema-type" aria-label={t('生效类型详情')}>
            <strong>{schemaSelection.name}</strong>
            {'from' in selectedType && <p>{selectedType.from.node} → {selectedType.to.node}</p>}
            <p>{t('业务键 ')}<code>{selectedType.key.join(' · ') || '—'}</code></p>
            <p>{t('属性 ')}<code>{selectedType.properties.join(' · ') || '—'}</code></p>
          </aside>}
        </div> : shownSchema?.error ? <div className="empty" role="alert">{shownSchema.error}<button className="button" onClick={() => {
          if (shownSchema.stale) refresh();
          else { setSchema(undefined); setSchemaAttempt(value => value + 1); }
        }}>{shownSchema.stale ? t('重新加载模型') : t('重试')}</button></div>
          : <p className="empty" role="status">{t('正在读取生效图结构…')}</p>)}
        {canvas && <div hidden={view !== 'graph'}>
          {output.canvas && canvas.requestId !== result?.requestId && <p className="notice query-notice">{t('画布起始于执行 ')}<code title={canvas.requestId}>{canvas.requestId.slice(0, 8)}</code>
            {output.canvas.additions > 0 && t('，已追加 {0} 次查询', output.canvas.additions)}
            。{output.canvas.lastIncluded !== result?.requestId && t('本次执行未向画布加入图元素。')}{t('表格和原始响应显示最近一次执行。重跑原查询会重建起始画布。')}
          </p>}
          <GraphExplorer key={canvas.requestId} ref={explorer} id={id} page={canvas.page}
            schema={schema?.revision === canvas.page.model_revision ? schema.data : undefined} activeRevision={revision}
            timeout={canvas.statement?.timeout_ms} busy={!!running || !!pending} reload={refresh}
            rerun={canvas.statement ? () => void run('query', 'replay') : undefined} />
        </div>}
        {view === 'graph' && !canvas && <p className="empty">{page
          ? page.graph.truncated ? t('图元素超过预览上限，请缩小查询范围。') : t('当前结果没有可绘制的图元素。')
          : t('运行查询后可在这里查看图元素。')}</p>}
        {view === 'table' && (page ? <>
          <div className="table-scroll" role="region" aria-label={t('查询结果表格')} tabIndex={0}><table>
            <thead><tr>{page.columns.map((column, index) => <th scope="col" key={index}>{column.name}<small>{column.data_type}</small></th>)}</tr></thead>
            <tbody>{page.rows.map((row, index) => <tr key={index}>{row.map((cell, cellIndex) => <td key={cellIndex}>
              <code className={cell === null ? 'null-cell' : ''}>{cell ?? 'null'}</code></td>)}</tr>)}</tbody>
          </table></div>
          {!page.rows.length && <p className="empty">{t('查询未返回行')}</p>}
        </> : <p className="empty">{t('运行查询后可在这里查看结果。')}</p>)}
        {view === 'raw' && (page ? <RawResponse page={page} />
          : <p className="empty">{t('运行查询后可在这里查看原始响应。')}</p>)}
      </div>
      {page && <details className="query-details source-versions"><summary>{t('执行详情')}</summary>
        <dl className="execution-metadata">
          <div><dt>{t('执行标识')}</dt><dd><code>{result.requestId}</code></dd></div>
          <div><dt>{t('模型版本')}</dt><dd><code>{page.model_revision}</code></dd></div>
          {page.execution_worker && <div><dt>{t('执行节点')}</dt><dd><code>{page.execution_worker}</code></dd></div>}
          {page.csr_generation && <div><dt>{t('CSR 版本')}</dt><dd><code>{page.csr_generation}</code></dd></div>}
        </dl>
        {!!page.sources.length && <dl>{page.sources.map((source, index) => <div key={index}><dt>{source.source}</dt><dd><code>{source.version}</code><small>{source.consistency}</small></dd></div>)}</dl>}
        {!!page.freshness?.length && <dl>{page.freshness.map((source, index) => <div key={index}><dt>{source.source}</dt><dd>
          <small>{t('最近观察 {0} · 全量对账 {1}', source.observed_at_ms ? new Date(source.observed_at_ms).toLocaleString(locale) : t('未知'), source.reconciled_at_ms ? new Date(source.reconciled_at_ms).toLocaleString(locale) : t('未知'))}</small>
          <small>{source.deletion_capture === 'marker_and_reconciliation' ? t('软删除与对账') : source.deletion_capture === 'reconciliation' ? t('全量对账捕获删除') : t('删除覆盖未知')}</small>
          {source.result_generation && source.result_generation !== source.generation && <small>{t('结果读取历史代次')}</small>}
        </dd></div>)}</dl>}
      </details>}
    </section>
    <div className="query-library">
    <details className="query-details query-history-section"><summary>{t('查询模板 ')}<span className="tool-count">{templates.length}</span></summary>
      <form className="query-toolbar template-form" onSubmit={event => {
        event.preventDefault();
        if (!templateName.trim() || !query.trim() || !timeoutValid) return;
        const statement = statementFromEditor();
        const needsParameters = !!statement.parameters_json || language === 'cypher' && parameterRequired;
        if (!keepParameters) delete statement.parameters_json;
        saveTemplates([{ id: crypto.randomUUID(), name: templateName.trim(), statement, needsParameters }, ...templates]);
        setTemplateName(''); setKeepParameters(false);
      }}>
        <label>{t('模板名称')}<input value={templateName} maxLength={80} onChange={event => setTemplateName(event.target.value)} /></label>
        {language === 'cypher' && <label><input type="checkbox" checked={keepParameters} onChange={event => setKeepParameters(event.target.checked)} />{t('在此浏览器保存参数值')}</label>}
        <button className="button compact" disabled={!templateName.trim() || !query.trim() || !timeoutValid}>{t('保存查询模板')}</button>
      </form>
      {!templates.length && <p className="query-library-hint">{t('把常用语句存为模板，下次直接载入。')}</p>}
      <ul className="version-list query-history">{templates.map(item => <li key={item.id}>
        <div><strong>{item.name}</strong><small>{item.statement.language} · {item.statement.query}</small></div>
        <button className="button compact" disabled={!!running} onClick={() => load(item.statement, item.needsParameters)}>{t('载入模板')}</button>
        <button className="button compact" aria-label={t('移除模板 {0}', item.name)} onClick={() => saveTemplates(templates.filter(template => template.id !== item.id))}>{t('移除')}</button>
      </li>)}</ul>
    </details>
    <details className="query-details query-history-section">
      <summary>{t('查询历史 ')}<span className="tool-count">{history.length}</span></summary>
      {!history.length ? <p className="empty">{t('运行过的查询会显示在这里。')}</p> : <ol className="version-list query-history">
        {history.map((item, i) => <li key={i}><div><code title={item.query}>{item.query}</code>
          <small>{item.language} · {item.status}{t(' · 版本 {0} · {1}', item.model_revision.slice(0, 10), new Date(item.at).toLocaleString(locale))}</small></div>
          <button className="button compact" disabled={!!running} onClick={() => load(item, item.needsParameters)}>{t('载入语句')}</button></li>)}
      </ol>}
    </details>
    </div>
  </section>;
}
