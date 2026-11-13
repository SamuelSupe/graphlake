import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { Box, Minus, Plus } from 'lucide-react';
import { endpoint, type Model, type Selection } from './model';

export type GraphSelection = Selection & { section: 'nodes' | 'edges' };

type Point = [number, number];
type Curve = { name: string; points: [Point, Point, Point, Point] };
type Layout = 'spread' | 'grid';

const radius = 22;
const tones = ['#397bde', '#50a2d1', '#627ce0', '#367fa4', '#596cb8'];

function pointOn(points: Curve['points'], t: number): Point {
  const u = 1 - t;
  return [0, 1].map(axis =>
    u ** 3 * points[0][axis] + 3 * u ** 2 * t * points[1][axis]
    + 3 * u * t ** 2 * points[2][axis] + t ** 3 * points[3][axis],
  ) as Point;
}

function placeNodes(names: string[], width: number, height: number, columns: number, layout: Layout) {
  const rows = Math.ceil(names.length / columns);
  const spanX = Math.min(width - 120, 380);
  const spanY = Math.min(height - 144, 190);
  return new Map(names.map((name, index): [string, Point] => {
    if (layout === 'spread' && names.length <= 2) {
      const side = names.length === 1 ? 0 : index === 0 ? 0.5 : -0.5;
      return [name, [width / 2 + spanX * side, height / 2 - spanY * side]];
    }
    if (layout === 'spread' && names.length <= 10) {
      const angle = index * 2 * Math.PI / names.length - Math.PI / 2;
      const x = Math.min(width / 2 - 64, Math.max(145, names.length * 31));
      const y = Math.min(height / 2 - 67, Math.max(118, names.length * 24));
      return [name, [width / 2 + x * Math.cos(angle), height / 2 + y * Math.sin(angle)]];
    }
    const row = Math.floor(index / columns);
    const count = Math.min(columns, names.length - row * columns);
    const x = width / 2 + (index % columns - (count - 1) / 2) * Math.min(176, (width - 96) / columns);
    return [name, [x, height / 2 + (row - (rows - 1) / 2) * 124]];
  }));
}

export function GraphCanvas({ model, selected, select }: {
  model: Pick<Model, 'nodes' | 'edges'>;
  selected?: Selection;
  select?: (selection: GraphSelection) => void;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const [layout, setLayout] = useState<Layout>('spread');
  const [zoom, setZoom] = useState(1);
  const [size, setSize] = useState({ width: 600, height: 600 });

  useEffect(() => {
    const element = viewport.current;
    if (!element) return;
    const observer = new ResizeObserver(() => setSize({ width: element.clientWidth, height: element.clientHeight }));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const names = Object.keys(model.nodes);
  const width = Math.max(264, Math.floor(size.width / zoom));
  const columns = Math.max(2, Math.min(7, Math.floor(width / 145)));
  const height = Math.max(280, Math.ceil(names.length / columns) * 124 + 95, Math.floor(size.height / zoom));
  const points = placeNodes(names, width, height, columns, layout);
  const lanes = new Map<string, number>();
  const curves: Curve[] = [];
  for (const [name, edge] of Object.entries(model.edges)) {
    const from = endpoint(edge.from).node;
    const to = endpoint(edge.to).node;
    const a = points.get(from);
    const b = points.get(to);
    if (!a || !b) continue;
    const pair = JSON.stringify([from, to].sort());
    const lane = lanes.get(pair) ?? 0;
    lanes.set(pair, lane + 1);
    if (from === to) {
      const [x, y] = a;
      const spread = 52 + Math.min(lane, 8) * 11;
      curves.push({ name, points: [[x - 15, y + 18], [x - spread, y + spread], [x + spread, y + spread], [x + 15, y + 18]] });
      continue;
    }
    const dx = b[0] - a[0];
    const dy = b[1] - a[1];
    const length = Math.hypot(dx, dy) || 1;
    const unit: Point = [dx / length, dy / length];
    const bend = lane === 0 ? 0 : Math.min(90, Math.ceil(lane / 2) * 33) * (lane % 2 === 0 ? -1 : 1);
    const normal: Point = [-unit[1] * bend, unit[0] * bend];
    const start: Point = [a[0] + unit[0] * radius, a[1] + unit[1] * radius];
    const end: Point = [b[0] - unit[0] * radius, b[1] - unit[1] * radius];
    curves.push({ name, points: [start, [a[0] + dx / 3 + normal[0], a[1] + dy / 3 + normal[1]],
      [a[0] + 2 * dx / 3 + normal[0], a[1] + 2 * dy / 3 + normal[1]], end] });
  }
  useLayoutEffect(() => {
    const surface = canvas.current;
    const context = surface?.getContext('2d');
    if (!surface || !context) return;
    const scale = Math.min(2, devicePixelRatio, 4096 / Math.max(width, height));
    surface.width = Math.round(width * scale);
    surface.height = Math.round(height * scale);
    context.scale(scale, scale);
    for (const curve of curves) {
      const [from, c1, c2, to] = curve.points;
      const current = selected?.section === 'edges' && selected.name === curve.name;
      context.strokeStyle = current ? '#2457d7' : '#a6b7d2';
      context.fillStyle = context.strokeStyle;
      context.lineWidth = current ? 2.3 : 1.5;
      context.beginPath();
      context.moveTo(...from);
      context.bezierCurveTo(...c1, ...c2, ...to);
      context.stroke();
      const tip = pointOn(curve.points, .58);
      const before = pointOn(curve.points, .55);
      const angle = Math.atan2(tip[1] - before[1], tip[0] - before[0]);
      context.beginPath();
      context.moveTo(...tip);
      for (const delta of [-0.54, 0.54]) context.lineTo(tip[0] - 10 * Math.cos(angle + delta), tip[1] - 10 * Math.sin(angle + delta));
      context.closePath();
      context.fill();
    }
  });

  function reset() {
    setZoom(1);
    setLayout('spread');
    viewport.current?.scrollTo(0, 0);
  }

  return <section className="canvas-panel" aria-label="模型画布">
    <div className="canvas-viewport" ref={viewport}>
      {names.length ? <div className="graph-stage" style={{ width, height, zoom }}>
        <canvas ref={canvas} aria-hidden="true" style={{ width, height }} />
        {names.map((name, index) => {
          const [left, top] = points.get(name)!;
          const content = <><span className="node-label" title={name}>{name}</span>
            <span className="node-dot" style={{ background: tones[index % tones.length] }} /></>;
          const style = { left, top };
          return select ? <button key={name} className="graph-node" style={style}
            aria-label={name} aria-pressed={selected?.section === 'nodes' && selected.name === name}
            onClick={() => select({ section: 'nodes', name })}>{content}</button>
            : <div key={name} className="graph-node" style={style}>{content}</div>;
        })}
        {curves.map(curve => {
          const [x, y] = pointOn(curve.points, .48);
          const style = { left: x, top: y - 5 };
          return select ? <button key={curve.name} className="graph-edge" style={style}
            aria-pressed={selected?.section === 'edges' && selected.name === curve.name}
            onClick={() => select({ section: 'edges', name: curve.name })}>{curve.name}</button>
            : <span key={curve.name} className="graph-edge" style={style}>{curve.name}</span>;
        })}
      </div> : <div className="canvas-empty"><Box size={25} /><span>暂无节点</span></div>}
    </div>
    {names.length > 0 && <div className="canvas-controls">
      <label>布局<select aria-label="画布布局" value={layout} onChange={event => setLayout(event.target.value as Layout)}>
        <option value="spread">展开</option><option value="grid">网格</option>
      </select></label>
      <div className="canvas-zoom">
        <button className="icon-button" aria-label="缩小画布" disabled={zoom <= .75} onClick={() => setZoom(value => Math.max(.75, value - .125))}><Minus size={15} /></button>
        <button className="zoom-reset" aria-label="重置画布布局" onClick={reset}>{Math.round(zoom * 100)}%</button>
        <button className="icon-button" aria-label="放大画布" disabled={zoom >= 1.5} onClick={() => setZoom(value => Math.min(1.5, value + .125))}><Plus size={15} /></button>
      </div>
    </div>}
  </section>;
}
