import { useEffect, useRef, type PointerEvent } from 'react';
import { Box, Plus } from 'lucide-react';
import type { Point } from './modelCanvasGeometry';

const arrowSteps: Partial<Record<string, Point>> = {
  ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1],
};

type Move = {
  stagePoint: (clientX: number, clientY: number) => Point;
  preview: (point?: Point) => void;
  commit: (point: Point) => void;
};
type Gesture = { id: number; client: Point; at: Point; origin: Point; moved: boolean };

export function CanvasVertex({ name, point, tone, selected, activate, connect, linking, source, target, hint, highlight, move, incoming, moving }: {
  name: string; point: Point; tone: string; selected: boolean;
  activate?: () => void; connect?: () => void; linking: boolean; source: boolean; target: boolean; incoming: boolean;
  hint?: string; highlight: (name?: string) => void; move?: Move; moving: boolean;
}) {
  const button = useRef<HTMLButtonElement>(null);
  const gesture = useRef<Gesture>(undefined);
  const suppressClick = useRef(false);
  const previewFrame = useRef(0);
  useEffect(() => () => cancelAnimationFrame(previewFrame.current), []);

  function position(event: PointerEvent, drag: Gesture): Point {
    const [x, y] = move!.stagePoint(event.clientX, event.clientY);
    return [drag.origin[0] + x - drag.at[0], drag.origin[1] + y - drag.at[1]];
  }

  function finish(event: PointerEvent, commit: boolean) {
    const drag = gesture.current;
    if (!drag || drag.id !== event.pointerId) return;
    gesture.current = undefined;
    cancelAnimationFrame(previewFrame.current);
    if (drag.moved) {
      suppressClick.current = true;
      move?.preview();
      if (commit && move) move.commit(position(event, drag));
    }
  }

  const content = <>
    <span className="node-label" title={name}>{name}</span>
    <span className="node-dot" style={{ background: tone }}><Box size={17} aria-hidden="true" /></span>
  </>;

  return <div className="graph-vertex" style={{ left: point[0], top: point[1] }}
    data-link-source={source || undefined} data-link-target={target || undefined} data-moving={moving || undefined}>
    {activate ? <button ref={button} type="button" className="graph-node" data-movable={!!move || undefined}
      aria-label={linking ? (incoming ? '选为终点 ' : '选为起点 ') + name : name}
      aria-pressed={linking ? source : selected}
      aria-describedby={hint}
      onPointerDown={event => {
        if (!event.isPrimary || event.button !== 0) return;
        suppressClick.current = false;
        if (!move) return;
        const client: Point = [event.clientX, event.clientY];
        gesture.current = { id: event.pointerId, client, at: move.stagePoint(...client), origin: point, moved: false };
        event.currentTarget.setPointerCapture(event.pointerId);
      }}
      onPointerMove={event => {
        const drag = gesture.current;
        if (!move || !drag || drag.id !== event.pointerId) return;
        if (!drag.moved && Math.hypot(event.clientX - drag.client[0], event.clientY - drag.client[1]) < 5) return;
        drag.moved = true;
        const point = position(event, drag);
        cancelAnimationFrame(previewFrame.current);
        previewFrame.current = requestAnimationFrame(() => move.preview(point));
      }}
      onPointerUp={event => finish(event, true)}
      onPointerCancel={event => finish(event, false)}
      onLostPointerCapture={event => finish(event, false)}
      onPointerEnter={() => highlight(name)}
      onPointerLeave={() => highlight()}
      onFocus={() => highlight(name)}
      onBlur={() => highlight()}
      onClick={event => {
        if (suppressClick.current && event.detail > 0) return;
        activate();
      }}
      onKeyDown={event => {
        const drag = gesture.current;
        if (event.key === 'Escape' && drag && move) {
          event.stopPropagation();
          gesture.current = undefined;
          suppressClick.current = true;
          cancelAnimationFrame(previewFrame.current);
          move.preview();
          if (event.currentTarget.hasPointerCapture(drag.id)) event.currentTarget.releasePointerCapture(drag.id);
          return;
        }
        if (!move || drag || event.altKey || event.ctrlKey || event.metaKey) return;
        const delta = arrowSteps[event.key];
        if (!delta) return;
        event.preventDefault();
        const step = event.shiftKey ? 32 : 12;
        move.commit([point[0] + step * delta[0], point[1] + step * delta[1]]);
      }}>{content}</button> : <div className="graph-node">{content}</div>}
    {connect && !linking && <button type="button" className="graph-port" aria-label={'从 ' + name + ' 开始连线'} title={'从 ' + name + ' 开始连线'}
      onClick={() => { button.current?.focus({ preventScroll: true }); connect(); }}><Plus size={14} aria-hidden="true" /></button>}
  </div>;
}
