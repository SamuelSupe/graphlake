import type { ReactNode } from 'react';
import { ArrowLeft } from 'lucide-react';
import { t } from './locale.ts';

export function ModelWorkspaceHeader({ workspace, name, back, switcher, rename, status, action }: {
  workspace: string;
  name: string;
  back: () => void;
  switcher?: ReactNode;
  rename?: ReactNode;
  status?: ReactNode;
  action?: ReactNode;
}) {
  return <header className="workspace-header model-workspace-header">
    <div className="model-header-nav">
      <button className="model-back" type="button" aria-label={workspace + ' · ' + t('返回模型列表')} title={t('返回模型列表')} onClick={back}>
        <ArrowLeft size={16} aria-hidden="true" />{workspace}
      </button>
      {switcher}
    </div>
    <div className="model-heading-row">
      <div className="model-heading-info">
        <div className="model-heading-name"><h1>{name}</h1>{rename}</div>
        {status && <div className="model-heading-status">{status}</div>}
      </div>
      {action}
    </div>
  </header>;
}
