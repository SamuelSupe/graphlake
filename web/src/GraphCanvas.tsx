import { t } from './locale.ts';
import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type RefObject } from 'react';
import { Box, Minus, Move, Plus, RotateCcw, Waypoints, X } from 'lucide-react';
import { CanvasVertex } from './CanvasVertex';
import { endpoint, type Model, type Selection } from './model';
import { canvasColumns, connectedNodeOrder, edgeCurve, pinOf, placeNodes, pointOf, pointOn, type CanvasLayout, type Curve, type Point } from './modelCanvasGeometry';
import './modelCanvas.css';

export type GraphSelection = Selection & { section: 'nodes' | 'edges' };
export type CanvasConnection = { from?: string };
export type CanvasView = { layout: CanvasLayout; zoom: number };
type Editing = {
  enabled: boolean;
  positions: ReadonlyMap<string, Point>;
  move: (node: string, position: Point) => void;
  arrange: () => void;
  connection?: CanvasConnection;
  start: (node: string) => void;
  pick: (node: string) => void;
  cancel: () => void;
};
const tones = ['#843b52', '#9a612c', '#537785', '#765f80', '#70734f'];

function drawCurve(context: CanvasRenderingContext2D, points: Curve, color: string, weight: number, preview = false) {
  const [from, c1, c2, to] = points;
  context.strokeStyle = context.fillStyle = color;
  context.lineWidth = weight;
  context.setLineDash(preview ? [6, 4] : []);
  context.beginPath();
  context.moveTo(...from);
  context.bezierCurveTo(...c1, ...c2, ...to);
  context.stroke();
  const tip = pointOn(points, .66);
  const before = pointOn(points, .63);
  const angle = Math.atan2(tip[1] - before[1], tip[0] - before[0]);
  context.beginPath();
  context.moveTo(...tip);
  for (const delta of [-.54, .54]) context.lineTo(tip[0] - 10 * Math.cos(angle + delta), tip[1] - 10 * Math.sin(angle + delta));
  context.closePath();
  context.fill();
}

export default function GraphCanvas({ model, selected, select, editing, viewState }: {
  model: Pick<Model, 'nodes' | 'edges'>;
  selected?: Selection;
  select?: (selection: GraphSelection) => void;
  editing?: Editing;
  viewState?: RefObject<CanvasView | undefined>;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const stage = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const [layout, setLayout] = useState<CanvasLayout>(() => viewState?.current?.layout ?? 'spread');
  const [zoom, setZoom] = useState(() => viewState?.current?.zoom ?? 1);
  const [size, setSize] = useState({ width: 600, height: 600 });
  const [moving, setMoving] = useState<{ name: string; point: Point }>();
  const [placeNode, setPlaceNode] = useState<string>();
  const [moveMessage, setMoveMessage] = useState('');
  const [previewTarget, setPreviewTarget] = useState<string>();
  const [cursor, setCursor] = useState<Point>();
  const pointerFrame = useRef(0);
  const hint = useId();
  const enabled = !!editing?.enabled;
  const connection = enabled ? editing?.connection : undefined;
  const placing = enabled && !connection && placeNode && Object.hasOwn(model.nodes, placeNode) ? placeNode : undefined;

  useLayoutEffect(() => {
    if (viewState) viewState.current = { layout, zoom };
  }, [viewState, layout, zoom]);

  useEffect(() => {
    const element = viewport.current;
    if (!element) return;
    const observer = new ResizeObserver(() => {
      const { clientWidth: width, clientHeight: height } = element;
      setSize(previous => previous.width === width && previous.height === height ? previous : { width, height });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  useEffect(() => {
    if (!enabled || connection) {
      setPlaceNode(undefined);
      setMoving(undefined);
    }
    if (!connection) {
      cancelAnimationFrame(pointerFrame.current);
      setPreviewTarget(undefined);
      setCursor(undefined);
    }
    return () => cancelAnimationFrame(pointerFrame.current);
  }, [enabled, connection]);

  const names = useMemo(() => Object.keys(model.nodes), [model.nodes]);
  const links = useMemo(() => Object.entries(model.edges).map(([name, edge]) => ({
    name, from: endpoint(edge.from).node, to: endpoint(edge.to).node,
  })), [model.edges]);
  const order = useMemo(() => layout === 'spread' ? connectedNodeOrder(names, links) : names, [names, links, layout]);
  const width = Math.max(264, size.width);
  const columns = canvasColumns(width);
  const height = Math.max(280, Math.ceil(names.length / columns) * 124 + 95, size.height);
  const positions = editing?.positions;
  const movePreview = enabled && !connection ? moving : undefined;
  const points = useMemo(() => {
    const result = placeNodes(order, width, height, columns, layout);
    for (const [name, pin] of positions ?? []) {
      if (result.has(name)) result.set(name, pointOf(pin, width, height));
    }
    if (movePreview && result.has(movePreview.name)) result.set(movePreview.name, movePreview.point);
    return result;
  }, [order, width, height, columns, layout, positions, movePreview]);

  const { lanes, curves } = useMemo(() => {
    const lanes = new Map<string, number>();
    const curves: { name: string; points: Curve; labelBelow: boolean; style: { left: number; top: number } }[] = [];
    for (const { name, from, to } of links) {
      const a = points.get(from);
      const b = points.get(to);
      if (!a || !b) continue;
      const pair = JSON.stringify([from, to].sort());
      const lane = lanes.get(pair) ?? 0;
      lanes.set(pair, lane + 1);
      const loop = from === to;
      const curve = edgeCurve(a, b, height, lane, loop, from > to);
      const labelBelow = loop && a[1] <= height / 2;
      const [x, y] = pointOn(curve, .47);
      curves.push({ name, points: curve, labelBelow, style: { left: x, top: labelBelow ? y + 5 : y - 5 } });
    }
    return { lanes, curves };
  }, [links, points, height]);
  const from = connection?.from;
  const origin = from === undefined ? undefined : points.get(from);
  const target = previewTarget === undefined ? undefined : points.get(previewTarget);
  const destination = target ?? cursor;
  const preview = useMemo(() => origin && destination ? edgeCurve(origin, destination, height,
    target ? lanes.get(JSON.stringify([from, previewTarget].sort())) ?? 0 : 0,
    from === previewTarget, !!previewTarget && from! > previewTarget, target ? undefined : 0) : undefined,
  [origin, destination, height, lanes, from, previewTarget, target]);
  const selectedEdge = selected?.section === 'edges' ? selected.name : undefined;

  useLayoutEffect(() => {
    const surface = canvas.current;
    const context = surface?.getContext('2d');
    if (!surface || !context) return;
    const scale = Math.min(2, devicePixelRatio, 4096 / Math.max(width, height));
    const w = Math.round(width * scale);
    const h = Math.round(height * scale);
    if (surface.width !== w) surface.width = w;
    if (surface.height !== h) surface.height = h;
    context.setTransform(scale, 0, 0, scale, 0, 0);
    context.clearRect(0, 0, width, height);
    for (const curve of curves) {
      const current = selectedEdge === curve.name;
      drawCurve(context, curve.points, current ? '#8a2946' : '#aa9095', current ? 2.3 : 1.6);
    }
    if (preview) drawCurve(context, preview, '#8a2946', 2, true);
  }, [width, height, curves, selectedEdge, preview]);

  function stagePoint(x: number, y: number): Point {
    const rect = stage.current?.getBoundingClientRect();
    if (!rect || !rect.width || !rect.height) return [0, 0];
    return [(x - rect.left) * width / rect.width, (y - rect.top) * height / rect.height];
  }

  function commit(name: string, position: Point) {
    editing?.move(name, pinOf(position, width, height));
    setMoveMessage(t('{0} 的画布位置已调整', name));
  }

  function arrange(next: CanvasLayout) {
    setLayout(next);
    editing?.arrange();
    viewport.current?.scrollTo(0, 0);
  }

  const selectedNode = selected?.section === 'nodes' && points.has(selected.name) ? selected.name : undefined;
  const instructions = placing ? t('点击画布空白处放置 {0}', placing)
    : connection ? from ? t('起点：{0}。请选择终点，支持自环。', from) : t('请选择连线起点。')
    : t('拖动节点调整布局，点节点旁的「＋」开始连线。');
  return <section className={'canvas-panel' + (editing ? ' canvas-editable' : '')} aria-label={t('模型画布')}
    onKeyDown={event => {
      if (event.key === 'Escape' && placing) { event.stopPropagation(); setPlaceNode(undefined); }
    }}>
    {editing && names.length > 0 && <div className="canvas-instructions" data-active={!!(connection || placing) || undefined}>
      <p role="status">{connection && <Waypoints size={15} aria-hidden="true" />}{instructions}</p>
      {connection ? <button type="button" className="text-button" onClick={editing.cancel}><X size={14} aria-hidden="true" />{t('取消连线')}</button>
        : enabled && selectedNode && <button type="button" className="text-button" aria-pressed={!!placing}
          onClick={() => setPlaceNode(placing ? undefined : selectedNode)}>
          {placing ? <X size={14} aria-hidden="true" /> : <Move size={14} aria-hidden="true" />}{placing ? t('取消移动') : t('移动选中节点')}
        </button>}
    </div>}
    <span id={hint} className="sr-only">{t('可拖动，或按方向键移动节点。')}</span>
    <span className="sr-only" role="status">{moveMessage}</span>
    <div className="canvas-frame"><div className="canvas-viewport" ref={viewport} tabIndex={0} role="region" aria-label={t('画布滚动区')}>
      {names.length ? <div ref={stage} className="graph-stage" style={{ width, height, zoom }} data-placing={!!placing || undefined} data-linking={!!connection || undefined}
        onClick={event => {
          if (!placing || event.target !== event.currentTarget) return;
          commit(placing, stagePoint(event.clientX, event.clientY));
          setPlaceNode(undefined);
        }}
        onPointerMove={event => {
          if (!from || event.pointerType === 'touch') return;
          const point = stagePoint(event.clientX, event.clientY);
          cancelAnimationFrame(pointerFrame.current);
          pointerFrame.current = requestAnimationFrame(() => setCursor(point));
        }}
        onPointerLeave={() => {
          cancelAnimationFrame(pointerFrame.current);
          setCursor(undefined);
          setPreviewTarget(undefined);
        }}>
        <canvas ref={canvas} aria-hidden="true" style={{ width, height }} />
        {curves.map(curve => select ? <button type="button" key={curve.name} className="graph-edge" style={curve.style}
            data-label-below={curve.labelBelow || undefined}
            disabled={!!connection || !!placing}
            aria-pressed={selectedEdge === curve.name}
            onClick={() => select({ section: 'edges', name: curve.name })}>{curve.name}</button>
            : <span key={curve.name} className="graph-edge" style={curve.style} data-label-below={curve.labelBelow || undefined}>{curve.name}</span>)}
        {names.map((name, index) => <CanvasVertex key={name} name={name} point={points.get(name)!} tone={tones[index % tones.length]}
          selected={selected?.section === 'nodes' && selected.name === name}
          linking={!!connection} source={from === name} target={!!from && previewTarget === name} incoming={!!from} moving={moving?.name === name}
          activate={select && (() => {
            if (connection) editing?.pick(name);
            else if (!placing) select({ section: 'nodes', name });
          })}
          connect={enabled && !placing ? () => editing?.start(name) : undefined}
          highlight={target => { if (connection) setPreviewTarget(target); }}
          hint={enabled && !connection && !placing ? hint : undefined}
          move={enabled && !connection && !placing ? {
            stagePoint,
            preview: point => setMoving(point ? { name, point: pointOf(pinOf(point, width, height), width, height) } : undefined),
            commit: point => commit(name, point),
          } : undefined} />)}
      </div> : <div className="canvas-empty"><Box size={25} aria-hidden="true" /><span>{t('暂无节点')}</span></div>}
    </div></div>
    {names.length > 0 && <div className="canvas-controls">
      <label>{t('布局')}<select aria-label={t('画布布局')} value={layout} onChange={event => arrange(event.target.value as CanvasLayout)}>
        <option value="spread">{t('自动布局')}</option><option value="grid">{t('网格')}</option>
      </select></label>
      {editing && <button type="button" className="icon-button" aria-label={t('自动排列节点')} title={t('自动排列节点')} onClick={() => arrange(layout)}>
        <RotateCcw size={15} aria-hidden="true" />
      </button>}
      <div className="canvas-zoom">
        <button type="button" className="icon-button" aria-label={t('缩小画布')} disabled={zoom <= .75} onClick={() => setZoom(value => Math.max(.75, value - .125))}><Minus size={15} aria-hidden="true" /></button>
        <button type="button" className="zoom-reset" aria-label={t('恢复 100% 缩放')} onClick={() => { setZoom(1); viewport.current?.scrollTo(0, 0); }}>{Math.round(zoom * 100)}%</button>
        <button type="button" className="icon-button" aria-label={t('放大画布')} disabled={zoom >= 1.5} onClick={() => setZoom(value => Math.min(1.5, value + .125))}><Plus size={15} aria-hidden="true" /></button>
      </div>
    </div>}
  </section>;
}
