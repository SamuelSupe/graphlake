import { useRef, useSyncExternalStore, type ReactNode } from 'react';
import { Menu } from 'lucide-react';
import { t } from './locale.ts';
import { Mark } from './ui';
import './navigation.css';

const compact = window.matchMedia('(max-width: 760px)');
const supportsPopover = 'popover' in HTMLElement.prototype;

function subscribe(changed: () => void) {
  compact.addEventListener('change', changed);
  return () => compact.removeEventListener('change', changed);
}

function isCompact() {
  return supportsPopover && compact.matches;
}

export function ConsoleNavigation({ page, home, account, children }: {
  page: string;
  home: () => void;
  account: ReactNode;
  children: ReactNode;
}) {
  const collapsed = useSyncExternalStore(subscribe, isCompact);
  const menu = useRef<HTMLDivElement>(null);
  const menuId = 'console-destinations';

  return <header className="app-rail" data-collapsed={collapsed || undefined}>
    <button className="rail-brand" aria-label={t('GraphLake 首页')} onClick={home}>
      <Mark size={26} /><span>GraphLake</span>
    </button>
    <nav className="rail-nav" aria-label={t('主导航')}>
      {collapsed && <button className="rail-menu-trigger button compact" type="button"
        popoverTarget={menuId} aria-controls={menuId} aria-label={t('导航菜单 · {0}', page)}>
        <Menu size={17} aria-hidden="true" /><span>{page}</span>
      </button>}
      <div className="rail-menu" id={menuId} ref={menu} popover={collapsed ? 'auto' : undefined}>
        <div className="rail-items rail-destinations" onClick={event => {
          if (collapsed && event.target instanceof Element && event.target.closest('.rail-link') && menu.current?.matches(':popover-open')) {
            menu.current.hidePopover();
          }
        }}>{children}</div>
        {collapsed && account}
      </div>
    </nav>
    {!collapsed && account}
  </header>;
}
