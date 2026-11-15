export type Point = [number, number];
export type Curve = [Point, Point, Point, Point];
export type CanvasLayout = 'spread' | 'grid';

const horizontalInset = 68;
const verticalInset = 62;
const nodeRadius = 22;
const clampUnit = (value: number) => Math.min(1, Math.max(0, value));

export function pinOf([x, y]: Point, width: number, height: number): Point {
  return [
    clampUnit((x - horizontalInset) / (width - 2 * horizontalInset)),
    clampUnit((y - verticalInset) / (height - 2 * verticalInset)),
  ];
}

export function pointOf([x, y]: Point, width: number, height: number): Point {
  return [
    horizontalInset + x * (width - 2 * horizontalInset),
    verticalInset + y * (height - 2 * verticalInset),
  ];
}

export function placeNodes(names: string[], width: number, height: number, columns: number, layout: CanvasLayout) {
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
      const x = Math.min(width / 2 - horizontalInset, Math.max(145, names.length * 31));
      const y = Math.min(height / 2 - verticalInset, Math.max(118, names.length * 24));
      return [name, [width / 2 + x * Math.cos(angle), height / 2 + y * Math.sin(angle)]];
    }
    const row = Math.floor(index / columns);
    const count = Math.min(columns, names.length - row * columns);
    const x = width / 2 + (index % columns - (count - 1) / 2) * Math.min(176, (width - 96) / columns);
    return [name, [x, height / 2 + (row - (rows - 1) / 2) * 124]];
  }));
}

export function pointOn(points: Curve, t: number): Point {
  const u = 1 - t;
  return [0, 1].map(axis =>
    u ** 3 * points[0][axis] + 3 * u ** 2 * t * points[1][axis]
    + 3 * u * t ** 2 * points[2][axis] + t ** 3 * points[3][axis],
  ) as Point;
}

export function edgeCurve(a: Point, b: Point, height: number, lane: number, loop: boolean, reverse = false, targetRadius = nodeRadius): Curve {
  if (loop) {
    const [x, y] = a;
    const spread = 52 + Math.min(lane, 8) * 11;
    const side = y <= height / 2 ? 1 : -1;
    return [[x - 15, y + side * 18], [x - spread, y + side * spread], [x + spread, y + side * spread], [x + 15, y + side * 18]];
  }
  const dx = b[0] - a[0];
  const dy = b[1] - a[1];
  const length = Math.hypot(dx, dy) || 1;
  const unit: Point = [dx / length, dy / length];
  const bend = lane === 0 ? 0 : Math.min(90, Math.ceil(lane / 2) * 33) * (lane % 2 === 0 ? -1 : 1) * (reverse ? -1 : 1);
  const normal: Point = [-unit[1] * bend, unit[0] * bend];
  const startRadius = Math.min(nodeRadius, length / 3);
  const endRadius = Math.min(targetRadius, length / 3);
  return [
    [a[0] + unit[0] * startRadius, a[1] + unit[1] * startRadius],
    [a[0] + dx / 3 + normal[0], a[1] + dy / 3 + normal[1]],
    [a[0] + 2 * dx / 3 + normal[0], a[1] + 2 * dy / 3 + normal[1]],
    [b[0] - unit[0] * endRadius, b[1] - unit[1] * endRadius],
  ];
}
