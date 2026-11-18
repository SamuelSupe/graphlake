import type { ComponentProps } from 'react';
import type { Workspace } from './Editor';

export function modelPath(id: string, workspace: Workspace = 'graph') {
  return '/models/' + id + (workspace === 'graph' ? '' : '/' + workspace);
}

export function AppLink({ href, navigate, ...props }: Pick<ComponentProps<'a'>, 'id' | 'className' | 'aria-current' | 'aria-label' | 'children'> & {
  href: string;
  navigate: (path: string) => void;
}) {
  return <a {...props} href={href} onClick={event => {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    navigate(href);
  }} />;
}
