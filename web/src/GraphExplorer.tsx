import { memo, useDeferredValue, useEffect, useId, useImperativeHandle, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent, type Ref } from 'react';
import { CircleHelp, Crosshair, Maximize, Minus, Plus, RotateCcw, X } from 'lucide-react';
import { api, ApiError, errorText, type ExplorationMatch, type ExplorationSearch, type GraphElement, type GraphExploration, type GraphSchema, type NeighborFilter, type QueryGraphPage, type QueryResult } from './api';
import { GraphSearchMatches } from './GraphSearchMatches';
import { GraphLegend } from './GraphLegend';
import { NeighborControls } from './NeighborControls';
import { appendGraph, combineVersions, focusNeighborhood, graphLabel, graphType, maxElements, mergePage, removeElement, searchCanvas, visibleGraphElements } from './graphExploration';
import { edgeCurve, edgeLanes, forceVertexLimit, packPaths, positions, scene, type Layout, type Point } from './graphLayout';

const tones = ['#3277db', '#168aa6', '#6664c4', '#346494', '#3679aa', '#5850a7'];
const layoutNames = { radial: '径向布局', layered: '分层布局', force: '力导向布局' } as const;
const minZoom = .005;
const clampZoom = (value: number) => Math.max(minZoom, Math.min(4, value));

type Camera = Point & { zoom: number };
type Drag = { pointer: number; x: number; y: number; camera: Camera; vertex?: string; point?: Point; moved: boolean };
type CrossSearch = ExplorationSearch & { query: string; offset: number; scope: string; visibility: string };
type EdgeGeometry = ReturnType<typeof edgeCurve>;

type Expansion = {
  id: string; filter: NeighborFilter; cursor?: string; held?: GraphExploration;
  current?: string; previous?: string; page: number; batchMode: boolean;
};
type SearchScope = { anchor: GraphElement; filter: NeighborFilter; cursor: string; key: string };
type QueryContinuation = { snapshot: string; next: number; prefix?: string; loaded?: QueryGraphPage };
type CanvasState = { elements: GraphElement[]; selected?: string; camera: Camera; overrides: Map<string, Point>;
  expansions: Map<string, Expansion>; focused: boolean; searchScope?: SearchScope; layout: Layout;
  search: string; searchResult?: CrossSearch };
export type GraphExplorerHandle = { append: (result: QueryResult) => string | undefined };

const EdgePaths = memo(function EdgePaths({ edges, dense, selected, matched, arrow }: {
  edges: EdgeGeometry[]; dense: boolean; selected?: string; matched: ReadonlySet<string>; arrow: string;
}) {
  if (!dense) return edges.map(({ id, element, path }) => <path key={id} d={path}
    className={!element ? 'path-step' : selected === id || matched.has(id) ? 'selected' : undefined}
    markerEnd={element?.directed === false ? undefined : 'url(#' + arrow + ')'} />);
  const normal: string[] = [], highlighted: string[] = [], steps: string[] = [];
  for (const { id, element, path } of edges) {
    if (id === selected) continue;
    (!element ? steps : matched.has(id) && matched.size <= 12 ? highlighted : normal).push(path);
  }
  const active = edges.find(edge => edge.id === selected);
  return <>
    {packPaths(normal).map((path, i) => <path key={'normal:' + i} d={path} />)}
    {packPaths(steps).map((path, i) => <path key={'step:' + i} className="path-step" d={path} />)}
    {packPaths(highlighted).map((path, i) => <path key={'match:' + i} className="selected" d={path} />)}
    {active && <path d={active.path} className="selected" markerEnd={active.element?.directed === false ? undefined : 'url(#' + arrow + ')'} />}
  </>;
});

export function GraphExplorer({ id, page, schema, activeRevision, timeout, busy, reload, rerun, ref }: {
  id: string; page: QueryResult; schema?: GraphSchema; activeRevision: string; timeout?: number; busy: boolean; reload: () => void; rerun: () => void;
  ref?: Ref<GraphExplorerHandle>;
}) {
  const [elements, setElements] = useState(page.graph.elements);
  const [steps, setSteps] = useState(page.graph.steps);
  const [graphTruncated, setGraphTruncated] = useState(page.graph.truncated && page.graph_next === undefined);
  const [continuations, setContinuations] = useState<QueryContinuation[]>(() =>
    page.graph_snapshot && page.graph_next !== undefined ? [{ snapshot: page.graph_snapshot, next: page.graph_next }] : []);
  const [queryGraphError, setQueryGraphError] = useState<string>();
  const [original, setOriginal] = useState<CanvasState>();
  const [selected, setSelected] = useState<string>();
  const [layout, setLayout] = useState<Layout>('radial');
  const [camera, setCamera] = useState<Camera>({ x: 0, y: 0, zoom: 1 });
  const [overrides, setOverrides] = useState(new Map<string, Point>());
  const [pending, setPending] = useState<{ id: string; neighbors: boolean }>();
  const [addedFrom, setAddedFrom] = useState<QueryResult['sources']>([]);
  const [message, setMessage] = useState<{ text: string; error?: boolean; stale?: boolean }>();
  const [expansions, setExpansions] = useState(new Map<string, Expansion>());
  const [rerunReason, setRerunReason] = useState<string>();
  const [focused, setFocused] = useState(false);
  const [help, setHelp] = useState(false);
  const [hiddenTypes, setHiddenTypes] = useState(new Set<string>());
  const [displayLabels, setDisplayLabels] = useState(new Map<string, string | null>());
  const [search, setSearch] = useState('');
  const deferredSearch = useDeferredValue(search);
  const [searchScope, setSearchScope] = useState<SearchScope>();
  const [searchResult, setSearchResult] = useState<CrossSearch>();
  const visibility = JSON.stringify([...hiddenTypes].sort());
  const crossSearch = searchResult?.scope === searchScope?.key && searchResult?.query === search.trim()
    && searchResult?.visibility === visibility ? searchResult : undefined;
  const [searchError, setSearchError] = useState<string>();
  const [searching, setSearching] = useState(false);
  const searchRequest = useRef<AbortController | null>(null);
  const [targetMatch, setTargetMatch] = useState<string>();
  const batchSelections = useRef(new Map<string, string>());
  const elementsById = useMemo(() => new Map(elements.map(element => [element.id, element])), [elements]);
  const visibleElements = useMemo(() => visibleGraphElements(elements, hiddenTypes), [elements, hiddenTypes]);
  const visibleIds = useMemo(() => new Set(visibleElements.map(element => element.id)), [visibleElements]);
  const matches = useMemo(() => searchCanvas(visibleElements, deferredSearch), [visibleElements, deferredSearch]);
  const matched = useMemo(() => new Set(matches.map(element => element.id)), [matches]);
  const surface = useRef<HTMLDivElement>(null);
  const details = useRef<HTMLElement>(null);
  const detailsInvoker = useRef<HTMLElement | null>(null);
  const svg = useRef<SVGSVGElement>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const request = useRef<AbortController | null>(null);
  const gesture = useRef<Drag | null>(null);
  const frame = useRef<number>(0);
  const nextMotion = useRef<(() => void) | null>(null);
  const suppressClick = useRef(false);
  const arrow = useId();
  const graph = useMemo(() => scene(elements, steps), [elements, steps]);
  const placement = useMemo(() => positions(graph, layout), [graph, layout]);
  const vertices = useMemo(() => graph.vertices.filter(element => visibleIds.has(element.id)), [graph.vertices, visibleIds]);
  const lanes = useMemo(() => edgeLanes(graph.edges), [graph.edges]);
  const dense = visibleElements.length > 400;
  const point = (key: string) => overrides.get(key) ?? placement.get(key)!;
  const item = selected && visibleIds.has(selected) ? elementsById.get(selected) : undefined;
  const expansion = selected ? expansions.get(selected) : undefined;
  const viewingBatch = [...expansions.values()].find(value => value.batchMode);
  const held = expansion?.held ? expansion : [...expansions.values()].find(value => value.held);
  const isStale = page.model_revision !== activeRevision;
  const color = (label: string) => tones[Array.from(label).reduce((hash, char) => (hash * 31 + char.codePointAt(0)!) % tones.length, 0)];
  const labelFor = (element: GraphElement) => graphLabel(element, displayLabels.get(graphType(element)));
  const accessibleLabel = (element: GraphElement) => {
    const name = labelFor(element);
    return (element.kind === 'node' ? '节点 ' : '边 ') + element.label + ' [' + element.key.join(', ') + ']'
      + (name === element.label ? '' : ' · ' + name);
  };

  useEffect(() => {
    const panel = details.current;
    if (selected && panel && panel.getBoundingClientRect().top >= window.innerHeight * .7) {
      panel.scrollIntoView({ block: 'start' });
    }
  }, [selected]);

  useImperativeHandle(ref, () => ({ append(result) {
    if (request.current || original) return '请先完成邻居展开或返回原画布，再追加查询结果。';
    if (rerunReason || page.model_revision !== result.model_revision
      || JSON.stringify(page.graph_options) !== JSON.stringify(result.graph_options)) {
      return '画布和查询结果的模型版本或图查询范围不同，无法追加。';
    }
    const versions = combineVersions([...page.sources, ...addedFrom], result.sources);
    if (!versions) return '画布与本次查询的数据源版本不同，无法追加。';
    // Unkeyed edges carry execution-local row identifiers. Keep them distinct across runs.
    const prefix = crypto.randomUUID() + ':';
    const incoming = result.graph.elements.map(element => element.transient && element.kind === 'edge'
      ? { ...element, id: prefix + element.id } : element);
    const next = appendGraph(elements, incoming);
    if (!next) return '追加结果会超过画布上限。移除部分元素后再试。';
    const nextSteps = [...new Map([...steps, ...result.graph.steps].map(step => [JSON.stringify(step), step])).values()];
    setElements(next);
    setSteps(nextSteps);
    if (result.graph.truncated && result.graph_next === undefined) setGraphTruncated(true);
    if (result.graph_snapshot && result.graph_next !== undefined) {
      const { graph_snapshot: snapshot, graph_next: next } = result;
      setContinuations(previous => [...previous, { snapshot, next, prefix }]);
    }
    setAddedFrom(versions);
    fit(positions(scene(next, nextSteps), layout));
  } }));

  useEffect(() => () => {
    request.current?.abort();
    searchRequest.current?.abort();
    cancelAnimationFrame(frame.current);
  }, []);
  useEffect(() => {
    const element = surface.current;
    if (!element) return;
    const wheel = (event: WheelEvent) => {
      if (window.matchMedia('(max-width: 760px)').matches && !event.ctrlKey && !event.metaKey) return;
      if (event.cancelable) event.preventDefault();
      const bounds = element.getBoundingClientRect();
      const x = event.clientX - bounds.left - bounds.width / 2;
      const y = event.clientY - bounds.top - bounds.height / 2;
      setCamera(previous => {
        const zoom = clampZoom(previous.zoom * Math.exp(-Math.max(-180, Math.min(180, event.deltaY)) * .002));
        const ratio = zoom / previous.zoom;
        return { zoom, x: x - (x - previous.x) * ratio, y: y - (y - previous.y) * ratio };
      });
    };
    element.addEventListener('wheel', wheel, { passive: false });
    return () => element.removeEventListener('wheel', wheel);
  }, []);

  function fit(next: ReadonlyMap<string, Point>) {
    const { width, height } = size;
    if (!width || !height || !next.size) return;
    let left = Infinity, top = Infinity, right = -Infinity, bottom = -Infinity;
    for (const { x, y } of next.values()) {
      left = Math.min(left, x); right = Math.max(right, x);
      top = Math.min(top, y); bottom = Math.max(bottom, y);
    }
    const zoom = Math.min(1.3, Math.max(minZoom, Math.min((width - 26) / (right - left + 150), (height - 26) / (bottom - top + 135))));
    setCamera({ x: -(left + right) * zoom / 2, y: -(top + bottom) * zoom / 2, zoom });
  }

  function fitVisible(useOverrides: boolean) {
    fit(new Map([...placement].filter(([id]) => visibleIds.has(id)).map(([id, position]) =>
      [id, useOverrides ? overrides.get(id) ?? position : position])));
  }

  const hasSized = useRef(false);
  useEffect(() => {
    const element = surface.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      if (width && height) setSize(previous => previous.width === width && previous.height === height ? previous : { width, height });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  // A details pane or a tab switch can resize the surface without changing the camera.
  useEffect(() => {
    if (!size.width || !size.height || hasSized.current) return;
    hasSized.current = true;
    fitVisible(false);
  }, [size.width, size.height]); // eslint-disable-line react-hooks/exhaustive-deps

  function applyPage(result: GraphExploration, filter?: NeighborFilter, batchMode = false, canvas = elements) {
    if (!result.selected) {
      setMessage({ text: '在当前数据源中未找到该元素。', error: true });
      return;
    }
    const merged = mergePage(batchMode ? [result.selected] : canvas, result.selected, result.graph.elements);
    if (batchMode) {
      if (!original) setOriginal({ elements, selected, camera, overrides, expansions, focused, searchScope, layout, search, searchResult });
      changeCanvas(merged.elements);
      setSelected(result.selected.id);
      setFocused(true);
    } else setElements(merged.elements);
    setAddedFrom(previous => [...new Map([...previous, ...result.sources].map(source => [source.source, source])).values()]);
    if (filter) {
      if (canvas.length <= 2 && merged.added) fit(positions(scene(merged.elements, steps), layout));
      const id = result.selected.id;
      if (result.page_cursor) {
        setSearchScope({ anchor: result.selected, filter, cursor: result.page_cursor, key: JSON.stringify([id, filter]) });
      }
      setExpansions(previous => new Map(previous).set(id, { id, filter, cursor: result.next_cursor ?? undefined,
        current: result.page_cursor ?? undefined, previous: result.previous_cursor ?? undefined,
        page: result.page_number, batchMode,
        held: merged.remaining.length ? { ...result, graph: { ...result.graph, elements: merged.remaining } } : undefined }));
      if (merged.remaining.length) {
        setMessage({ text: '当前页还有邻居待加入。' });
      } else if (batchMode) {
        setMessage({ text: '正在查看第 ' + result.page_number + ' 批邻居。' });
      } else if (result.graph.truncated && !result.next_cursor) {
        setMessage({ text: '当前页的部分图元素超过预览限制。' });
      } else if (merged.added) {
        setMessage({ text: '已加入 ' + merged.added + ' 个图元素。' });
      } else {
        setMessage({ text: result.graph.elements.some(value => value.kind === 'edge')
          ? '当前页的邻居已在画布上。' : '没有符合条件的邻居。' });
      }
    } else if (merged.remaining.length) {
      setMessage({ text: '画布空间不足。移除节点或聚焦子图后，可重新读取。', error: true });
    }
    return merged;
  }

  async function explore(element: GraphElement, filter?: NeighborFilter, continuation = false,
    batch?: { cursor: string; replace: boolean; locate?: string }, loadAll = false) {
    if (request.current || isStale || !!rerunReason || busy || element.transient) return;
    const previous = continuation ? expansions.get(element.id) : undefined;
    if (!batch && previous?.held) {
      applyPage(previous.held, filter);
      return;
    }
    if (filter && !continuation && !batch) {
      searchRequest.current?.abort();
      setSearchScope(undefined);
      setSearchResult(undefined);
      setSearchError(undefined);
    }
    const controller = new AbortController();
    request.current = controller;
    setPending({ id: element.id, neighbors: !!filter });
    setMessage(undefined);
    try {
      const sourceVersions = new Map([...page.sources, ...addedFrom].map(source => [source.source, source.version]));
      let canvas = elements;
      let cursor = batch?.cursor ?? previous?.cursor;
      do {
        const result = await api<GraphExploration>('/models/' + id + '/explore', {
          method: 'POST', signal: controller.signal, body: JSON.stringify({
            model_revision: page.model_revision,
            element: { kind: element.kind, label: element.label, key: element.key },
            neighbors: !!filter, filter, cursor,
            timeout_ms: timeout, graph_options: page.graph_options, source_versions: Object.fromEntries(sourceVersions),
          }),
        });
        if (controller.signal.aborted) return;
        const merged = applyPage(result, filter, batch?.replace ?? previous?.batchMode ?? false, canvas);
        const selectionKey = batchSelectionKey(element.id, filter, result.page_number);
        const selection = batch?.locate ?? batchSelections.current.get(selectionKey);
        if (batch?.replace && selection && merged?.elements.some(element => element.id === selection)) {
          if (visibleGraphElements(merged.elements, hiddenTypes).some(element => element.id === selection)) {
            setSelected(selection);
            setTargetMatch(selection);
            rememberBatchSelection(selectionKey, selection);
          } else {
            setSearchError('匹配元素或其端点已隐藏。请在图例中显示对应类型。');
          }
        }
        if (!loadAll || !merged || merged.remaining.length || merged.elements.length >= maxElements) break;
        canvas = merged.elements;
        for (const source of result.sources) sourceVersions.set(source.source, source.version);
        cursor = result.next_cursor ?? undefined;
      } while (cursor);
    } catch (cause) {
      if (!controller.signal.aborted) {
        if (cause instanceof ApiError && cause.code === 'graph_sources_changed') {
          setRerunReason('来源版本已变化。已暂停探索，当前画布仍显示原结果。');
        } else if (cause instanceof ApiError && (cause.code === 'neighbor_session_expired'
          || cause.code === 'invalid_neighbor_cursor' && (batch?.cursor || previous?.cursor))) {
          setRerunReason('无法安全恢复此页。可重跑原查询并重建画布。');
        } else {
          setMessage({ text: errorText(cause), error: true, stale: cause instanceof ApiError && cause.code === 'active_revision_changed' });
        }
      }
    } finally {
      if (request.current === controller) {
        request.current = null;
        setPending(undefined);
      }
    }
  }

  function showBatch(expansion: Expansion, cursor: string | undefined) {
    const anchor = elementsById.get(expansion.id);
    if (selected && expansion.batchMode) rememberBatchSelection(batchSelectionKey(expansion.id, expansion.filter, expansion.page), selected);
    if (anchor && cursor) void explore(anchor, expansion.filter, false, { cursor, replace: true });
  }

  function batchSelectionKey(id: string, filter: NeighborFilter | undefined, page: number) {
    return JSON.stringify([id, filter, page]);
  }

  function rememberBatchSelection(key: string, id: string) {
    const entries = batchSelections.current;
    entries.delete(key);
    entries.set(key, id);
    if (entries.size > 64) entries.delete(entries.keys().next().value!);
  }

  async function loadQueryGraph(replace = false) {
    const current = continuations[0];
    if (!current || request.current || original || busy) return;
    const controller = new AbortController();
    request.current = controller;
    setPending({ id: '', neighbors: false });
    setQueryGraphError(undefined);
    try {
      const response = current.loaded ?? await api<QueryGraphPage>('/models/' + id + '/results/' + current.snapshot + '/graph/' + current.next,
        { signal: controller.signal });
      if (controller.signal.aborted) return;
      const incoming = response.graph.elements.map(element => current.prefix && element.transient && element.kind === 'edge'
        ? { ...element, id: current.prefix + element.id } : element);
      const next = appendGraph(replace ? [] : elements, incoming);
      if (!next) {
        setContinuations(previous => [{ ...current, loaded: response }, ...previous.slice(1)]);
        setQueryGraphError('下一批超过画布上限。移除元素或聚焦子图后可继续加载。');
        return;
      }
      setElements(next);
      setSteps(previous => [...new Map([...(replace ? [] : previous), ...response.graph.steps].map(step => [JSON.stringify(step), step])).values()]);
      if (replace) {
        setSelected(undefined);
        setOverrides(new Map());
        setExpansions(new Map());
        setSearchScope(undefined);
        setSearchResult(undefined);
        setFocused(false);
      }
      const remaining = response.next === null ? [] : [{ ...current, next: response.next, loaded: undefined }];
      setContinuations(previous => [...remaining, ...previous.slice(1)]);
      if (response.next === null) setGraphTruncated(previous => previous || response.incomplete);
      if (replace || !elements.length) fit(positions(scene(next, response.graph.steps), layout));
    } catch (cause) {
      if (!controller.signal.aborted) setQueryGraphError(errorText(cause));
    } finally {
      if (request.current === controller) {
        request.current = null;
        setPending(undefined);
      }
    }
  }

  function changeCanvas(next: GraphElement[]) {
    setElements(next);
    const retained = new Set(next.map(element => element.id));
    setExpansions(previous => new Map([...previous].filter(([id]) => retained.has(id))));
    setOverrides(new Map());
    setMessage(undefined);
    fit(positions(scene(next, steps), layout));
  }

  function restoreCanvas() {
    if (!original) return;
    searchRequest.current?.abort();
    setElements(original.elements); setSelected(original.selected); setCamera(original.camera);
    setOverrides(original.overrides); setExpansions(original.expansions); setFocused(original.focused);
    setSearchScope(original.searchScope); setLayout(original.layout); setOriginal(undefined);
    setSearch(original.search); setSearchResult(original.searchResult); setSearchError(undefined);
    setMessage(undefined);
  }

  function choose(element: GraphElement) {
    if (suppressClick.current) { suppressClick.current = false; return; }
    detailsInvoker.current = document.activeElement as HTMLElement;
    setSelected(element.id);
    setMessage(undefined);
    if (!element.properties || element.kind === 'edge' && element.directed !== true) void explore(element);
  }

  function locate(element: GraphElement) {
    detailsInvoker.current = document.activeElement as HTMLElement;
    setSelected(element.id);
    setMessage(undefined);
    setTargetMatch(element.id);
    if (!element.properties) void explore(element);
  }

  async function searchBatches(offset = 0, hidden = hiddenTypes) {
    if (!searchScope || isStale || rerunReason) return;
    const query = search.trim();
    if (!query) return;
    const { anchor, filter, cursor } = searchScope;
    const controller = new AbortController();
    searchRequest.current?.abort();
    searchRequest.current = controller;
    setSearching(true);
    setSearchError(undefined);
    try {
      const result = await api<ExplorationSearch>('/models/' + id + '/explore', {
        method: 'POST', signal: controller.signal, body: JSON.stringify({
          model_revision: page.model_revision, neighbors: true, filter, cursor,
          element: { kind: anchor.kind, label: anchor.label, key: anchor.key },
          timeout_ms: timeout, graph_options: page.graph_options,
          source_versions: Object.fromEntries([...page.sources, ...addedFrom].map(source => [source.source, source.version])),
          search: { query, offset,
            hidden_nodes: [...hidden].filter(type => type.startsWith('node:')).map(type => type.slice(5)),
            hidden_edges: [...hidden].filter(type => type.startsWith('edge:')).map(type => type.slice(5)) },
        }),
      });
      if (!controller.signal.aborted) setSearchResult({
        ...result, query, offset, scope: searchScope.key, visibility: JSON.stringify([...hidden].sort()),
      });
    } catch (cause) {
      if (controller.signal.aborted) return;
      if (cause instanceof ApiError && cause.code === 'graph_sources_changed') {
        setRerunReason('来源版本已变化。已暂停探索，当前画布仍显示原结果。');
      } else {
        setSearchError(errorText(cause));
      }
    } finally {
      if (searchRequest.current === controller) {
        searchRequest.current = null;
        setSearching(false);
      }
    }
  }

  function changeVisibility(next: Set<string>) {
    searchRequest.current?.abort();
    setSearchError(undefined);
    setHiddenTypes(next);
    if (crossSearch || searching) void searchBatches(0, next);
  }

  function locateAcrossBatches(match: ExplorationMatch) {
    const item = elementsById.get(match.id);
    if ((item && !visibleIds.has(item.id)) || hiddenTypes.has(graphType(match))) {
      setSearchError('匹配元素或其端点已隐藏。请在图例中显示对应类型。');
    } else if (item) {
      setSearchError(undefined);
      locate(item);
    }
    else if (searchScope) {
      if (selected && viewingBatch) rememberBatchSelection(batchSelectionKey(viewingBatch.id, viewingBatch.filter, viewingBatch.page), selected);
      void explore(searchScope.anchor, searchScope.filter, false, { cursor: match.cursor, replace: true, locate: match.id });
    }
  }

  function begin(event: ReactPointerEvent<HTMLElement>, vertex?: string) {
    if (event.button !== 0 || event.pointerType === 'touch' && !event.isPrimary) return;
    suppressClick.current = false;
    gesture.current = { pointer: event.pointerId, x: event.clientX, y: event.clientY, camera, vertex, point: vertex ? point(vertex) : undefined, moved: false };
  }
  function move(event: ReactPointerEvent<HTMLDivElement>) {
    const active = gesture.current;
    if (!active || active.pointer !== event.pointerId) return;
    const dx = event.clientX - active.x, dy = event.clientY - active.y;
    if (!active.moved && Math.hypot(dx, dy) < 5) return;
    active.moved = true;
    if (!event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.setPointerCapture(event.pointerId);
    queueMotion(active, dx, dy);
  }
  function queueMotion(active: Drag, dx: number, dy: number) {
    nextMotion.current = () => {
      const { vertex, point, camera } = active;
      if (vertex && point) {
        setOverrides(previous => new Map(previous).set(vertex, { x: point.x + dx / camera.zoom, y: point.y + dy / camera.zoom }));
      } else setCamera({ ...camera, x: camera.x + dx, y: camera.y + dy });
    };
    if (!frame.current) frame.current = requestAnimationFrame(flushMotion);
  }
  function flushMotion() {
    cancelAnimationFrame(frame.current);
    frame.current = 0;
    nextMotion.current?.();
    nextMotion.current = null;
  }
  function end(event: ReactPointerEvent<HTMLDivElement>) {
    const active = gesture.current;
    if (!active || active.pointer !== event.pointerId) return;
    if (active.moved) {
      if (event.type === 'pointerup') queueMotion(active, event.clientX - active.x, event.clientY - active.y);
      flushMotion();
      suppressClick.current = true;
      window.setTimeout(() => { suppressClick.current = false; }, 0);
    }
    gesture.current = null;
  }

  const baseCurves = useMemo(() => lanes.map(edge => edgeCurve(edge, placement.get(edge.from)!, placement.get(edge.to)!)), [lanes, placement]);
  const curves = useMemo(() => !overrides.size ? baseCurves : baseCurves.map((edge, index) =>
    overrides.has(edge.from) || overrides.has(edge.to) ? edgeCurve(lanes[index], point(edge.from), point(edge.to)) : edge
  ), [baseCurves, lanes, overrides, placement]);
  useEffect(() => {
    if (!targetMatch) return;
    const position = overrides.get(targetMatch) ?? placement.get(targetMatch)
      ?? curves.find(curve => curve.id === targetMatch)?.label;
    if (position) setCamera({ zoom: 1, x: -position.x, y: -position.y });
    setTargetMatch(undefined);
  }, [targetMatch, overrides, placement, curves]);
  const { width: viewportWidth, height: viewportHeight } = size;
  const visibleCurves = useMemo(() => curves.filter(curve => visibleIds.has(curve.from) && visibleIds.has(curve.to)
    && (!curve.element || visibleIds.has(curve.id))), [curves, visibleIds]);
  const edgeLabels = useMemo(() => {
    if (!dense) return visibleCurves;
    if (matches.length) return visibleCurves.filter(({ id }) => selected === id || matches.length <= 12 && matched.has(id));
    const shown: EdgeGeometry[] = [];
    const anchor = selected && visibleCurves.find(edge => edge.id === selected);
    if (anchor) shown.push(anchor);
    if (camera.zoom < .65) return shown;
    const occupied = new Set<string>();
    for (const edge of visibleCurves) {
      if (!edge.element || edge.id === selected) continue;
      const x = camera.x + edge.label.x * camera.zoom;
      const y = camera.y + edge.label.y * camera.zoom;
      if (Math.abs(x) > viewportWidth / 2 || Math.abs(y) > viewportHeight / 2) continue;
      const cell = Math.floor(x / 110) + ':' + Math.floor(y / 45);
      if (occupied.has(cell)) continue;
      occupied.add(cell);
      shown.push(edge);
      if (shown.length >= 24) break;
    }
    return shown;
  }, [dense, visibleCurves, selected, matches.length, matched, camera, viewportWidth, viewportHeight]);
  // SVG centers its own coordinate system. HTML vertex buttons share the same origin via CSS.
  const transform = 'translate(' + camera.x + 'px,' + camera.y + 'px) scale(' + camera.zoom + ')';
  const nodes = visibleElements.filter(element => element.kind === 'node').length;
  const edges = visibleElements.length - nodes;
  return <section className="graph-explorer" aria-label="结果图探索">
    <div className="explorer-toolbar">
      <span className="explorer-count" aria-live="polite">{nodes} 个节点 · {edges} 条边
        {visibleElements.length < elements.length && ' · 已隐藏 ' + (elements.length - visibleElements.length) + ' 个元素'}</span>
      <button className="icon-button" aria-label="图探索帮助" aria-expanded={help} onClick={() => setHelp(!help)}><CircleHelp size={18} /></button>
      <label className="explorer-layout"><span className="sr-only">结果图布局</span><select value={layout} onChange={event => {
        const next = event.target.value as Layout;
        setOverrides(new Map());
        setLayout(next);
        fit(new Map([...positions(graph, next)].filter(([id]) => visibleIds.has(id))));
      }}>{Object.entries(layoutNames).map(([value, name]) => <option key={value} value={value}>{name}</option>)}</select></label>
      {layout === 'force' && graph.vertices.length > forceVertexLimit && <span className="muted small">节点较多，使用径向布局</span>}
      <div className="explorer-zoom">
        <button className="icon-button" aria-label="缩小结果图" disabled={camera.zoom <= minZoom} onClick={() => setCamera(value => ({ ...value, zoom: clampZoom(value.zoom / 1.25) }))}><Minus size={16} /></button>
        <output aria-label="当前缩放">{Number((camera.zoom * 100).toFixed(camera.zoom < .1 ? 1 : 0))}%</output>
        <button className="icon-button" aria-label="放大结果图" disabled={camera.zoom >= 4} onClick={() => setCamera(value => ({ ...value, zoom: clampZoom(value.zoom * 1.25) }))}><Plus size={16} /></button>
        <button className="icon-button" aria-label="适配结果图" title="适配结果图" onClick={() => fitVisible(true)}><Maximize size={15} /></button>
      </div>
      <button className="button compact" onClick={() => { setOverrides(new Map()); fitVisible(false); }}><RotateCcw size={14} />重置视图</button>
      {pending?.neighbors && <button className="button compact" onClick={() => request.current?.abort()}>停止展开</button>}
      {original && <button className="button compact" disabled={!!pending || busy} onClick={restoreCanvas}>返回原画布</button>}
      {viewingBatch && <div className="explorer-actions" aria-label="邻居批次">
        <button className="button compact" disabled={!viewingBatch.previous || !!pending || busy || isStale || !!rerunReason}
          onClick={() => showBatch(viewingBatch, viewingBatch.previous)}>上一批</button>
        <span className="muted small">第 {viewingBatch.page} 批</span>
        <button className="button compact" disabled={!viewingBatch.cursor || !!pending || busy || isStale || !!rerunReason}
          onClick={() => showBatch(viewingBatch, viewingBatch.cursor)}>下一批</button>
      </div>}
    </div>
    {elements.length > 0 && <GraphLegend elements={elements} schema={schema} hidden={hiddenTypes} labels={displayLabels} color={color}
      toggle={type => {
        const next = new Set(hiddenTypes);
        if (!next.delete(type)) next.add(type);
        if (selected && !visibleGraphElements(elements, next).some(element => element.id === selected)) setSelected(undefined);
        changeVisibility(next);
      }} setLabel={(type, field) => {
        setDisplayLabels(old => {
          const next = new Map(old);
          if (field !== undefined) next.set(type, field);
          else next.delete(type);
          return next;
        });
      }} reset={() => {
        changeVisibility(new Set());
        setDisplayLabels(new Map());
      }} />}
    {elements.length > 0 && <div className="explorer-search">
      <div className="explorer-search-controls">
        <input type="search" aria-label="搜索画布属性" placeholder="搜索业务键、标签或属性" value={search} maxLength={256}
          onChange={event => {
            searchRequest.current?.abort();
            setSearchResult(undefined);
            setSearchError(undefined);
            setSearch(event.target.value);
          }} />
        {searchScope && <button className="button compact" disabled={!search.trim() || searching || isStale || !!rerunReason}
          onClick={() => void searchBatches()}>{searching ? '正在搜索…' : '搜索全部批次'}</button>}
        {deferredSearch.trim() && <span className="muted small" role="status">
          {crossSearch ? crossSearch.total + ' 个跨批匹配' : matches.length + ' 个匹配'}
        </span>}
      </div>
      {searchError && <p role="alert" className="inline-error small">{searchError}</p>}
      {crossSearch && crossSearch.matches.length > 0
        ? <GraphSearchMatches key={crossSearch.query + ':' + crossSearch.offset} matches={crossSearch.matches} locate={locateAcrossBatches}
          label={match => {
            const loaded = elementsById.get(match.id);
            return loaded ? labelFor(loaded) : match.label;
          }} />
        : !crossSearch && matches.length > 0 && <GraphSearchMatches key={deferredSearch} matches={matches} locate={locate} label={labelFor} />}
      {crossSearch && crossSearch.total > 100 && <div className="explorer-search-controls">
        <button className="button compact" disabled={searching || crossSearch.offset === 0}
          onClick={() => void searchBatches(crossSearch.offset - 100)}>上一组匹配</button>
        <button className="button compact" disabled={searching || crossSearch.offset + 100 >= crossSearch.total}
          onClick={() => void searchBatches(crossSearch.offset + 100)}>下一组匹配</button>
      </div>}
    </div>}
    {help && <p className="explorer-help">使用缩放按钮调整视图，桌面也可用滚轮。拖动空白处平移，拖动节点可调整位置。选择节点或边查看属性，选择节点后可展开一跳邻居。</p>}
    {visibleCurves.some(curve => !curve.element) && <p className="explorer-help">虚线表示路径中相邻的节点。查询结果中未包含它们之间的边。</p>}
    {rerunReason && <div className="explorer-notice" role="alert">
      <span>{rerunReason}</span>
      <button className="button compact" disabled={busy || isStale} onClick={rerun}>重跑原查询并重建画布</button>
    </div>}
    {!!continuations.length && <div className="explorer-notice">
      <button className="button compact" disabled={!!pending || !!original || busy} onClick={() => void loadQueryGraph()}>
        {pending?.id === '' ? '正在加载…' : '加载本次查询的下一批图元素'}
      </button>
      {continuations[0].loaded && <button className="button compact" disabled={!!pending || !!original || busy}
        onClick={() => void loadQueryGraph(true)}>切换到本次查询的下一批</button>}
      {queryGraphError && <span role="alert">{queryGraphError}</span>}
    </div>}
    {graphTruncated && !focused && !continuations.length && <p className="explorer-help">查询图可能未包含全部元素。可从当前节点继续探索。</p>}
    {(held || elements.length === maxElements) && <div className="explorer-notice" role="status">
      <span>{held
        ? '画布空间不足。可切换到当前批次，或腾出空间后继续加入。'
        : '画布已满（' + maxElements + ' 个元素）。移除节点或聚焦子图后可继续加入。'}</span>
      {held?.current && <button className="button compact" disabled={!!pending || busy || isStale || !!rerunReason}
        onClick={() => showBatch(held, held.current)}>切换到当前批次</button>}
      {held && visibleIds.has(held.id) && item?.id !== held.id && <button className="button compact" onClick={() => setSelected(held.id)}>返回待加入的节点</button>}
    </div>}
    <div className="explorer-body">
      <div className="explorer-surface" ref={surface} aria-label="结果画布" onPointerDown={event => {
        if (event.target === event.currentTarget || event.target === svg.current) begin(event);
      }} onPointerMove={move} onPointerUp={end} onPointerCancel={end} onLostPointerCapture={end}>
        <svg ref={svg} className="explorer-lines" aria-hidden="true">
          <defs><marker id={arrow} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#7893bb" />
          </marker></defs>
          <g transform={'translate(' + (viewportWidth / 2 + camera.x) + ' ' + (viewportHeight / 2 + camera.y) + ') scale(' + camera.zoom + ')'}>
            <EdgePaths edges={visibleCurves} dense={dense} selected={selected} matched={matched} arrow={arrow} />
          </g>
        </svg>
        <div className="explorer-points" style={{ transform }}>
          {vertices.map(element => {
            const position = point(element.id);
            return <button className={'explorer-node ' + element.kind + (matched.has(element.id) ? ' matched' : '')} aria-label={accessibleLabel(element)}
              key={element.id} aria-pressed={selected === element.id} style={{ left: position.x, top: position.y, '--element-color': color(element.label) } as React.CSSProperties}
              onClick={() => choose(element)} onPointerDown={event => { event.stopPropagation(); begin(event, element.id); }}
              onContextMenu={event => { event.preventDefault(); choose(element); }}>
              <span className="explorer-node-dot" />{(!dense || selected === element.id || matches.length <= 12 && matched.has(element.id)) &&
                <span className="explorer-node-label" title={accessibleLabel(element)}>{labelFor(element)}</span>}
            </button>;
          })}
          {edgeLabels.map(({ element, label }) => element && (selected === element.id ||
            (Math.abs(camera.x + label.x * camera.zoom) <= viewportWidth / 2 + 80 &&
             Math.abs(camera.y + label.y * camera.zoom) <= viewportHeight / 2 + 32)) &&
            <button className={'explorer-edge' + (matched.has(element.id) ? ' matched' : '')} key={element.id} style={{ left: label.x, top: label.y }}
            aria-label={accessibleLabel(element)} title={accessibleLabel(element)} aria-pressed={selected === element.id}
            onClick={() => choose(element)}>{labelFor(element)}</button>)}
        </div>
        {!visibleElements.length && <p className="empty">{elements.length ? '当前类型筛选隐藏了所有图元素。' : '当前结果没有图元素。'}</p>}
      </div>
      {item && <aside className="explorer-inspector" ref={details} aria-label="图元素属性">
        <div className="explorer-item-heading"><div><small>{item.kind === 'node' ? '节点' : '边'}{labelFor(item) !== item.label && ' · ' + item.label}</small><h3>{labelFor(item)}</h3></div>
          <button className="icon-button" aria-label="关闭图元素属性" onClick={() => {
            setSelected(undefined);
            detailsInvoker.current?.focus();
          }}><X size={17} /></button></div>
        <dl><div><dt>{item.transient ? '临时标识' : '业务键'}</dt><dd><code>{JSON.stringify(item.key)}</code></dd></div>
          {Object.entries(item.properties ?? {}).map(([name, value]) => <div key={name}><dt>{name}</dt><dd><code>{value ?? 'null'}</code></dd></div>)}
        </dl>
        {item.property_preview && <p className="muted small">部分属性已截断。</p>}
        {item.properties && !Object.keys(item.properties).length && <p className="muted small">此元素没有已映射的属性。</p>}
        {pending?.id === item.id && !pending.neighbors && <p role="status" className="muted small">正在读取属性…</p>}
        {item.transient && !item.properties && <p className="muted small">此边没有业务键。返回该边可在查询结果中查看属性。</p>}
        {(!item.properties || item.property_preview) && !item.transient && !pending && !isStale && !rerunReason && <button className="button compact" disabled={busy} onClick={() => void explore(item)}>读取属性</button>}
        {item.kind === 'node' && <NeighborControls key={item.id} label={item.label} schema={schema}
          activeFilter={expansion?.id === item.id ? expansion.filter : undefined}
          next={expansion?.id === item.id ? expansion.held ? 'pending' : expansion.cursor ? 'more' : 'done' : undefined}
          disabled={!!pending || isStale || !!rerunReason || busy}
          busy={pending?.id === item.id && pending.neighbors}
          expand={(filter, next) => void explore(item, filter, next)}
          loadAll={!expansion?.batchMode ? filter => void explore(item, filter, true, undefined, true) : undefined} />}
        <div className="explorer-actions">
          {item.kind === 'node' && <button className="button compact" disabled={!!pending} onClick={() => {
            const next = focusNeighborhood(elements, steps, item.id);
            changeCanvas(next);
            setFocused(true);
            if (next.length === elements.length && elements.length === maxElements) {
              setMessage({ text: '当前元素都在此节点的一跳内，可移除节点腾出空间。' });
            }
          }}><Crosshair size={14} />聚焦子图</button>}
          <button className="button compact" disabled={!!pending} onClick={() => {
            changeCanvas(removeElement(elements, item.id));
            setSelected(undefined);
          }}><X size={14} />{item.kind === 'node' ? '移除节点' : '移除边'}</button>
        </div>
        {isStale && <p className="muted small">模型版本已切换。重新运行查询后可继续探索。</p>}
        {addedFrom.length > 0 && <details className="explorer-sources">
          <summary>探索读取的来源版本</summary>{addedFrom.map(source => <p key={source.source}><code>{source.source} · {source.version}</code></p>)}
        </details>}
        {message && <p className={message.error ? 'inline-error small' : 'muted small'} role={message.error ? 'alert' : 'status'}>{message.text}
          {message.stale && <button className="text-button" onClick={reload}>重新加载模型</button>}</p>}
      </aside>}
    </div>
  </section>;
}
