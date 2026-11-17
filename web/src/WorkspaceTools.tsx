import { useId, useRef, type MouseEvent, type ReactNode } from 'react';
import { ChevronDown } from 'lucide-react';
import { t } from './locale.ts';

const supportsPopover = 'popover' in HTMLElement.prototype;

export function WorkspaceTools({ children }: { children: ReactNode }) {
  const id = useId();
  const panel = useRef<HTMLDivElement>(null);
  const fallback = useRef<HTMLDetailsElement>(null);
  const label = t('更多工具');
  const trigger = <>{label}<ChevronDown size={14} aria-hidden="true" /></>;

  function dismissAfterAction(event: MouseEvent<HTMLDivElement>) {
    if (!(event.target instanceof Element) || !event.target.closest('button:not(:disabled)')) return;
    if (panel.current?.matches(':popover-open')) panel.current.hidePopover();
    if (fallback.current) fallback.current.open = false;
  }

  if (!supportsPopover) return <details className="workspace-tools" ref={fallback}>
    <summary className="tool-button workspace-tools-trigger">{trigger}</summary>
    <div className="workspace-tools-panel" role="group" aria-label={label} onClick={dismissAfterAction}>{children}</div>
  </details>;

  return <div className="workspace-tools">
    <button type="button" className="tool-button workspace-tools-trigger" popoverTarget={id} aria-controls={id}>{trigger}</button>
    <div id={id} className="workspace-tools-panel" ref={panel} popover="auto" role="group" aria-label={label}
      onClick={dismissAfterAction}>{children}</div>
  </div>;
}
