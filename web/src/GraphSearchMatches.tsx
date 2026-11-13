import { useRef, useState } from 'react';
import type { GraphIdentity } from './api';

type Match = GraphIdentity & { id: string; page?: number };
const rowHeight = 32;
const windowSize = 12;

export function GraphSearchMatches<T extends Match>({ matches, locate, label = match => match.label }: {
  matches: T[]; locate: (match: T) => void; label?: (match: T) => string;
}) {
  const [first, setFirst] = useState(0);
  const list = useRef<HTMLDivElement>(null);
  const start = Math.min(first, Math.max(0, matches.length - windowSize));
  return <div className="explorer-match-window" ref={list} aria-label="匹配的图元素" role="region" tabIndex={0}
    onScroll={event => setFirst(Math.max(0, Math.floor(event.currentTarget.scrollTop / rowHeight) - 2))}
    onKeyDown={event => {
      if (event.target !== event.currentTarget || !list.current) return;
      if (event.key === 'Enter') {
        const match = matches[Math.floor(list.current.scrollTop / rowHeight)];
        if (match) locate(match);
      }
    }}>
    <div style={{ height: matches.length * rowHeight, position: 'relative' }}>
      {matches.slice(start, start + windowSize).map((match, index) =>
        <button type="button" className="explorer-match-row" key={match.id}
          style={{ top: (start + index) * rowHeight, height: rowHeight }}
          onClick={() => locate(match)}>
          <span>{label(match)} [{match.key.join(', ')}]</span>
          {match.page && <small>第 {match.page} 批</small>}
        </button>)}
    </div>
  </div>;
}
