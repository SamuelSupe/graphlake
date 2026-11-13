import type { GraphElement } from './api';

export type Point = { x: number; y: number };
export type Layout = 'radial' | 'layered' | 'force';
export const forceVertexLimit = 400;

export type Scene = {
  vertices: GraphElement[];
  edges: { id: string; element?: GraphElement; from: string; to: string }[];
};

export function edgeLanes(edges: Scene['edges']) {
  const pairs = new Map<string, number>();
  return edges.map(edge => {
    const pair = JSON.stringify(edge.from <= edge.to ? [edge.from, edge.to] : [edge.to, edge.from]);
    const lane = pairs.get(pair) ?? 0;
    pairs.set(pair, lane + 1);
    return { ...edge, lane };
  });
}

export function edgeCurve(edge: ReturnType<typeof edgeLanes>[number], a: Point, b: Point) {
  const { from, to, lane } = edge;
  const dx = b.x - a.x, dy = b.y - a.y, length = Math.max(1, Math.hypot(dx, dy));
  const bend = (lane ? Math.ceil(lane / 2) * (lane % 2 ? 24 : -24) : 0) * (from > to ? -1 : 1);
  const mid = { x: (a.x + b.x) / 2 - dy / length * bend, y: (a.y + b.y) / 2 + dx / length * bend };
  const start = { x: a.x + dx / length * 15, y: a.y + dy / length * 15 };
  const end = { x: b.x - dx / length * 17, y: b.y - dy / length * 17 };
  const loopHeight = 85 + lane * 30;
  const path = from === to ? `M ${a.x - 8} ${a.y - 8} C ${a.x - 58 - lane * 12} ${a.y - loopHeight} ${a.x + 58 + lane * 12} ${a.y - loopHeight} ${a.x + 8} ${a.y - 8}`
    : `M ${start.x} ${start.y} Q ${mid.x} ${mid.y} ${end.x} ${end.y}`;
  const label = { x: (start.x + 2 * mid.x + end.x) / 4, y: from === to ? a.y - loopHeight * .75 - 2 : (start.y + 2 * mid.y + end.y) / 4 };
  return { ...edge, path, label };
}

export function packPaths(paths: string[], batchSize = 128) {
  const packed = [];
  for (let i = 0; i < paths.length; i += batchSize) packed.push(paths.slice(i, i + batchSize).join(' '));
  return packed;
}

export function scene(elements: GraphElement[], steps: [string, string][]): Scene {
  const vertices = elements.filter(element => element.kind === 'node');
  const known = new Set(vertices.map(vertex => vertex.id));
  const edges: Scene['edges'] = [];
  for (const element of elements) {
    if (element.kind !== 'edge') continue;
    if (element.from && element.to && known.has(element.from) && known.has(element.to)) {
      edges.push({ id: element.id, element, from: element.from, to: element.to });
    } else {
      vertices.push(element);
    }
  }
  const linked = new Set(edges.map(edge => JSON.stringify([edge.from, edge.to].sort())));
  for (const [from, to] of steps) {
    if (known.has(from) && known.has(to) && !linked.has(JSON.stringify([from, to].sort()))) {
      edges.push({ id: 'path:' + JSON.stringify([from, to]), from, to });
    }
  }
  return { vertices, edges };
}

export function positions({ vertices, edges }: Scene, layout: Layout): Map<string, Point> {
  const links = new Map(vertices.map(vertex => [vertex.id, [] as string[]]));
  const incoming = new Map(vertices.map(vertex => [vertex.id, 0]));
  for (const edge of edges) {
    if (edge.from === edge.to) continue;
    links.get(edge.from)!.push(edge.to);
    if (layout !== 'layered') links.get(edge.to)!.push(edge.from);
    incoming.set(edge.to, incoming.get(edge.to)! + 1);
  }

  const levels: string[][] = [];
  const seen = new Set<string>();
  // Try roots first, then pick the first remaining vertex for disconnected components and cycles.
  const seeds = [...vertices].sort((left, right) => Number(incoming.get(left.id)! > 0) - Number(incoming.get(right.id)! > 0));
  for (const { id } of seeds) {
    if (seen.has(id)) continue;
    const queue = [{ id, depth: 0 }];
    seen.add(id);
    for (const current of queue) {
      (levels[current.depth] ??= []).push(current.id);
      for (const next of links.get(current.id) ?? []) {
        if (seen.has(next)) continue;
        seen.add(next);
        queue.push({ id: next, depth: current.depth + 1 });
      }
    }
  }

  const result = new Map<string, Point>();
  if (layout === 'layered') {
    levels.forEach((level, depth) => {
      level.forEach((id, index) => result.set(id, { x: (depth - (levels.length - 1) / 2) * 190, y: (index - (level.length - 1) / 2) * 86 }));
    });
    return result;
  }

  levels.forEach((level, depth) => {
    const radius = depth === 0 && level.length === 1 ? 0 : Math.max(depth * 130, level.length * 15, 70);
    level.forEach((id, index) => {
      const angle = (2 * Math.PI * index / level.length) - Math.PI / 3;
      result.set(id, { x: radius * Math.cos(angle), y: radius * Math.sin(angle) });
    });
  });
  // Keep layout work bounded when a canvas contains many distinct vertices.
  if (layout === 'radial' || vertices.length < 2 || vertices.length > forceVertexLimit) return result;

  const points = vertices.map(vertex => result.get(vertex.id)!);
  const index = new Map(vertices.map((vertex, i) => [vertex.id, i]));
  const springs = edges.filter(edge => edge.from !== edge.to).map(edge => [index.get(edge.from)!, index.get(edge.to)!]);
  for (let step = 0; step < 80; step++) {
    const forces = points.map(point => ({ x: -point.x * .012, y: -point.y * .012 }));
    for (let i = 0; i < points.length; i++) for (let j = i + 1; j < points.length; j++) {
      const dx = points[i].x - points[j].x || .01, dy = points[i].y - points[j].y;
      const squared = Math.max(16, dx * dx + dy * dy);
      const force = Math.min(8, 4600 / squared);
      const scale = force / Math.sqrt(squared);
      forces[i].x += dx * scale; forces[i].y += dy * scale;
      forces[j].x -= dx * scale; forces[j].y -= dy * scale;
    }
    for (const [i, j] of springs) {
      const dx = points[j].x - points[i].x, dy = points[j].y - points[i].y;
      const length = Math.max(1, Math.hypot(dx, dy));
      const scale = (length - 155) * .018 / length;
      forces[i].x += dx * scale; forces[i].y += dy * scale;
      forces[j].x -= dx * scale; forces[j].y -= dy * scale;
    }
    const speed = 1 - step / 100;
    points.forEach((point, i) => {
      point.x += Math.max(-20, Math.min(20, forces[i].x)) * speed;
      point.y += Math.max(-20, Math.min(20, forces[i].y)) * speed;
    });
  }
  return result;
}
