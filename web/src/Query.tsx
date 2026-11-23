import { locale, t } from './locale.ts';
import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { Braces, Check, Code2, Copy, Download, Filter, GitFork, Network, Play, SlidersHorizontal, Square, Table2 } from 'lucide-react';
import type { GraphSelection } from './GraphCanvas';
import { typeSampleQuery } from './queryCompletions';
import { queryLimitErrors, queryParameterError } from './queryParameters';
import { GraphExplorer, type GraphExplorerHandle } from './GraphExplorer';
import { api, ApiError, downloadFile, errorText, request, type FreshnessPolicy, type GraphSchema, type QueryResult } from './api';

const QueryEditor = lazy(() => import('./QueryEditor'));
const QueryScope = lazy(() => import('./QueryScope'));
const QueryAdvanced = lazy(() => import('./QueryAdvanced'));
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

const ageFields = [
  ['max_observation_age_seconds', t('来源观察最大年龄（秒）')],
  ['max_reconciliation_age_seconds', t('全量对账最大年龄（秒）')],
] as const;
const defaults = { cypher: 'MATCH (n) RETURN n LIMIT 25', sql: 'SELECT 1', gremlin: 'g.V().limit(25)' };
const actorKey = (id: string, actor: string) => actor === 'admin' || actor === '@dev' ? id : actor + ':' + id;
const historyKey = (id: string) => 'graphlake-query-history-v1:' + id;
function readHistory(id: string): HistoryItem[] {
  try { return JSON.parse(localStorage.getItem(historyKey(id)) ?? '[]'); } catch { return []; }
}
const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
function RawResponse({ page }: { page: QueryResult }) {
  const json = useMemo(() => JSON.stringify(page, null, 2), [page]);
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState('');
  const timer = useRef<number>(undefined);
  useEffect(() => () => window.clearTimeout(timer.current), []);
  async function copyJson() {
    try {
      await navigator.clipboard.writeText(json);
      setCopyError('');
      setCopied(true);
      window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => setCopied(false), 2200);
    } catch {
      setCopyError(t('无法访问剪贴板，请手动选中复制。'));
    }
  }
  return <div className="raw-result-box">
    <div className="code-snippet-bar">
      <span>JSON</span>
      <button type="button" className={'button compact' + (copied ? ' copied' : '')} onClick={() => void copyJson()}>
        {copied ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
        {copied ? t('已复制') : t('复制 JSON')}
      </button>
    </div>
    {copyError && <p className="inline-error query-notice" role="alert">{copyError}</p>}
    <pre className="raw-result" aria-label={t('原始 JSON 响应')} tabIndex={0}>{json}</pre>
  </div>;
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
  const [parametersTouched, setParametersTouched] = useState(false);
  const parameterProblem = useMemo(() => queryParameterError(parameters), [parameters]);
  const showParameterError = parametersTouched && !!parameterProblem;
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
  const [editingAdvanced, setEditingAdvanced] = useState(false);
  const [parametersOpen, setParametersOpen] = useState(false);
  const [captureResult, setCaptureResult] = useState(false);
  const [freshness, setFreshness] = useState<FreshnessPolicy>({});
  const [parameterRequired, setParameterRequired] = useState(false);
  const [loadNotice, setLoadNotice] = useState('');
  const [templateName, setTemplateName] = useState('');
  const [templateError, setTemplateError] = useState('');
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
  const parameterInput = useRef<HTMLTextAreaElement>(null);
  const limitErrors = queryLimitErrors(timeout, freshness);
  const timeoutValid = !limitErrors.timeout;
  const agesValid = !limitErrors.max_observation_age_seconds && !limitErrors.max_reconciliation_age_seconds;
  const hasFreshness = ageFields.some(([key]) => freshness[key] !== undefined) || language === 'cypher' && !!freshness.require_current_csr;
  const advancedCount = (timeout ? 1 : 0) + (canExport && captureResult ? 1 : 0) + (generation ? 1 : 0)
    + (freshness.max_observation_age_seconds !== undefined ? 1 : 0)
    + (freshness.max_reconciliation_age_seconds !== undefined ? 1 : 0)
    + (language === 'cypher' && freshness.require_current_csr ? 1 : 0);
  const canRun = !!query.trim() && timeoutValid && agesValid && (!parameterRequired || language !== 'cypher' || !!parameters.trim());
  const statementFromEditor = (): Statement => ({
    model_revision: revision, language, query,
    parameters_json: language === 'cypher' && parameters.trim() ? parameters : undefined,
    graph_options: generation ? undefined : graphOptions.trim() || undefined, capture_result: canExport && captureResult,
    csr_generation: language === 'cypher' ? generation : undefined,
    freshness: hasFreshness ? { ...freshness, require_current_csr: language === 'cypher' ? freshness.require_current_csr : undefined } : undefined,
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
    setParametersTouched(false);
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
      if (missingParameters) setParametersOpen(true);
      requestAnimationFrame(() => {
        const input = missingParameters ? parameterInput.current
          : composer.current?.querySelector<HTMLElement>('[contenteditable="true"], textarea');
        input?.focus({ preventScroll: true });
        input?.scrollIntoView({ block: 'center' });
      });
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
      setTemplateError('');
      return true;
    } catch {
      setTemplateError(t('浏览器无法保存查询模板，请检查存储空间。'));
      return false;
    }
  }

  function validateParameters() {
    if (language !== 'cypher' || !parameterProblem) return true;
    setParametersTouched(true);
    setParametersOpen(true);
    requestAnimationFrame(() => parameterInput.current?.focus());
    return false;
  }

  async function run(operation: 'query' | 'export' = 'query', mode: QueryMode = 'replace', sample?: string) {
    const usesEditor = operation === 'query' && mode !== 'replay' && sample === undefined;
    if (active.current || pending || operation === 'export' && !canExport || usesEditor && !canRun) return;
    if (usesEditor && !validateParameters()) return;
    let statement = operation === 'export' ? result?.statement : mode === 'replay' ? canvas?.statement : statementFromEditor();
    if (sample !== undefined) {
      if (!statement || !timeoutValid || !agesValid) return;
      statement = { ...statement, language: 'cypher', query: sample, parameters_json: undefined };
    }
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
  const starters = useMemo(() => {
    const data = shownSchema?.data;
    if (!data) return [];
    const items: { label: string; query: string }[] = [];
    for (const name of Object.keys(data.edges).slice(0, 2)) {
      items.push({ label: t('查看 {0} 关系样例', name), query: typeSampleQuery('edges', name) });
    }
    for (const name of Object.keys(data.nodes)) {
      if (items.length >= 3) break;
      items.push({ label: t('查看 {0} 节点样例', name), query: typeSampleQuery('nodes', name) });
    }
    return items;
  }, [shownSchema?.data]);
  function runStarter(sample: string) {
    if (query === defaults[language]) {
      setLanguage('cypher');
      setQueries(old => ({ ...old, cypher: sample }));
      setParameterRequired(false);
    }
    void run('query', 'replace', sample);
  }
  const starterChips = starters.length > 0 && <div className="query-starters" role="group" aria-label={t('快速试一试')}>
    <span className="query-starters-label">{t('快速试一试')}</span>
    <div className="query-starter-list">
      {starters.map(starter => <button key={starter.query} type="button" className="button compact query-starter"
        disabled={!!running || !!pending || !timeoutValid || !agesValid}
        onClick={() => runStarter(starter.query)}>
        <Play size={13} aria-hidden="true" />{starter.label}
      </button>)}
    </div>
  </div>;

  return <section className="query-workspace" aria-label={t('只读查询')}>
    <div className="query-input-column">
      <div className="query-composer" ref={composer}>
        <div className="query-toolbar">
          {running ? <button className="button" disabled={stopping} onClick={() => void stop()}><Square size={16} />{stopping ? t('正在取消…') : t('取消查询')}</button>
            : <button className="button primary" disabled={!canRun || !!pending} onClick={() => void run()}><Play size={16} />{t('运行查询')}<kbd className="kbd-on-primary" aria-hidden="true">{isMac ? '⌘↵' : 'Ctrl↵'}</kbd></button>}
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
            {language === 'cypher' && <button type="button" className={'button compact query-pill' + (parametersOpen || parameters.trim() || parameterRequired ? ' is-active' : '')}
              aria-expanded={parametersOpen || parameterRequired}
              onClick={() => setParametersOpen(open => !open)}>
              <Braces size={14} aria-hidden="true" />{t('Cypher 参数')}{parameters.trim() ? ' · JSON' : ''}
            </button>}
            <button type="button" className={'button compact query-pill' + (graphOptions.trim() && !generation ? ' is-active' : '')}
              disabled={!!generation || !!running} onClick={() => setEditingScope(true)}>
              <Filter size={14} aria-hidden="true" />{t('查询范围')}{graphOptions.trim() && !generation ? t(' · 已设置') : ''}
            </button>
            <button type="button" className={'button compact query-pill' + (advancedCount > 0 || !timeoutValid || !agesValid ? ' is-active' : '')}
              disabled={!!running} onClick={() => setEditingAdvanced(true)}>
              <SlidersHorizontal size={14} aria-hidden="true" />{t('高级选项')}
              {advancedCount > 0 && <span className="tab-count">{advancedCount}</span>}
            </button>
          </div>
          {language === 'cypher' && (parametersOpen || parameterRequired || showParameterError) && <div className="query-param-drawer form-fields">
            <label>{t('Cypher 参数（JSON）')}<textarea ref={parameterInput} value={parameters} onChange={event => setParameters(event.target.value)}
              onBlur={() => setParametersTouched(true)} placeholder="{}" spellCheck={false} rows={3} className="mono"
              aria-invalid={showParameterError || undefined} aria-describedby={showParameterError ? id + '-parameter-error' : parameterRequired && !parameters.trim() ? id + '-query-parameters-hint' : undefined} /></label>
            {parameterProblem && <span className="inline-error" id={id + '-parameter-error'} role="status"
              style={{ visibility: showParameterError ? 'visible' : 'hidden' }}>{parameterProblem}</span>}
          </div>}
        </div>
        {!agesValid && <p className="inline-error query-notice" id={id + '-freshness-error'} role="alert">{t('来源观察和全量对账的最大年龄须为 1 至 2592000 秒的整数。')}</p>}
        {parameterRequired && language === 'cypher' && !parameters.trim() && <p className="notice query-notice" id={id + '-query-parameters-hint'} role="status">{t('此查询需要参数。填写 Cypher 参数后即可运行。')}</p>}
        {loadNotice && <p className="notice query-notice" role="status">{loadNotice}</p>}
      </div>
      {visible && editingScope && <Suspense><QueryScope value={graphOptions} save={setGraphOptions} close={() => setEditingScope(false)} /></Suspense>}
      {visible && editingAdvanced && <Suspense><QueryAdvanced
        language={language} timeout={timeout} defaultTimeout={defaultTimeout}
        canExport={canExport} captureResult={captureResult} freshness={freshness}
        recoverable={recoverable} generation={generation} running={!!running}
        onSelectSnapshot={selectSnapshot} onClearSnapshot={() => setSnapshot(undefined)}
        apply={next => { setTimeoutText(next.timeout); setCaptureResult(next.captureResult); setFreshness(next.freshness); }}
        close={() => setEditingAdvanced(false)} /></Suspense>}
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
      {result?.statement && (result.statement.query !== query || result.statement.language !== language) &&
        <details className="result-statement"><summary>{t('查看本次执行的语句')} · {result.statement.language}</summary><pre>{result.statement.query}</pre></details>}
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
            <button className="button compact" disabled={!!running || !!pending || !timeoutValid || !agesValid}
              onClick={() => void run('query', 'replace', typeSampleQuery(schemaSelection.section, schemaSelection.name))}>
              <Play size={14} aria-hidden="true" />{schemaSelection.section === 'nodes' ? t('查看节点样例') : t('查看关系样例')}
            </button>
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
        {view === 'graph' && !canvas && (page ? <p className="empty">{page.graph.truncated ? t('图元素超过预览上限，请缩小查询范围。') : t('当前结果没有可绘制的图元素。')}</p>
          : <div className="empty empty-with-starters"><p>{t('运行查询后可在这里查看图元素。')}</p>{starterChips}</div>)}
        {view === 'table' && (page ? <>
          <div className="table-scroll" role="region" aria-label={t('查询结果表格')} tabIndex={0}><table>
            <thead><tr>{page.columns.map((column, index) => <th scope="col" key={index}>{column.name}<small>{column.data_type}</small></th>)}</tr></thead>
            <tbody>{page.rows.map((row, index) => <tr key={index}>{row.map((cell, cellIndex) => <td key={cellIndex}>
              <code className={cell === null ? 'null-cell' : ''}>{cell ?? 'null'}</code></td>)}</tr>)}</tbody>
          </table></div>
          {!page.rows.length && <p className="empty">{t('查询未返回行')}</p>}
        </> : <div className="empty empty-with-starters"><p>{t('运行查询后可在这里查看结果。')}</p>{starterChips}</div>)}
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
        if (!templateName.trim() || !query.trim()) return;
        if (!timeoutValid || !agesValid) {
          setEditingAdvanced(true);
          return;
        }
        if (keepParameters && !validateParameters()) return;
        const statement = statementFromEditor();
        const needsParameters = !!statement.parameters_json || language === 'cypher' && parameterRequired;
        if (!keepParameters) delete statement.parameters_json;
        if (saveTemplates([{ id: crypto.randomUUID(), name: templateName.trim(), statement, needsParameters }, ...templates])) {
          setTemplateName(''); setKeepParameters(false);
        }
      }}>
        <label>{t('模板名称')}<input value={templateName} maxLength={80} onChange={event => setTemplateName(event.target.value)} /></label>
        {language === 'cypher' && <label><input type="checkbox" checked={keepParameters} onChange={event => setKeepParameters(event.target.checked)} />{t('在此浏览器保存参数值')}</label>}
        <button className="button compact" disabled={!templateName.trim() || !query.trim()}>{t('保存查询模板')}</button>
      </form>
      {templateError && <p className="inline-error query-notice" role="alert">{templateError}</p>}
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
