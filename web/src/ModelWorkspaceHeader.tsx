import type { ReactNode } from 'react';
import { ArrowLeft, Database, Network, Terminal } from 'lucide-react';
import { AppLink, modelPath } from './AppLink';
import type { Workspace } from './Editor';
import { t } from './locale.ts';

export function ModelWorkspaceHeader({ workspace, name, modelId, revision, archiveError, back, switcher, rename, status, action, navigation }: {
  workspace: string;
  name: string;
  modelId?: string;
  revision?: string | null;
  archiveError?: boolean;
  back: () => void;
  switcher?: ReactNode;
  rename?: ReactNode;
  status?: ReactNode;
  action?: ReactNode;
  navigation?: { current: Workspace; catalogs: boolean; query: boolean; sourceCount?: number; goTo: (workspace: Workspace) => void };
}) {
  const showTabs = Boolean(modelId && navigation && (navigation.catalogs || navigation.query));
  return <header className={'workspace-header model-workspace-header' + (showTabs ? ' has-section-tabs' : '')}>
    <div className="model-header-nav">
      <div className="model-breadcrumb"><button className="model-back" type="button" aria-label={workspace + ' · ' + t('返回模型列表')} title={t('返回模型列表')} onClick={back}>
        <ArrowLeft size={15} aria-hidden="true" />{workspace}
      </button>
        {modelId && modelId !== name && <><span aria-hidden="true">/</span><code>{modelId}</code></>}
      </div>
      {switcher}
    </div>
    <div className="model-heading-row">
      <div className="model-heading-info">
        <div className="model-heading-name"><h1>{name}</h1>{rename}</div>
        <div className="model-heading-status">
          {(archiveError || revision !== undefined) && <span className={'release-label ' + (archiveError ? 'unavailable' : revision ? 'published' : '')}>
            <i aria-hidden="true" />{archiveError ? t('版本存储异常') : revision ? t('已发布') : t('未发布')}
            {revision && !archiveError && <code title={revision}>{revision.slice(0, 10)}</code>}
          </span>}
          {status}
        </div>
      </div>
      {action}
    </div>
    {showTabs && navigation && <nav className="model-section-nav" aria-label={t('模型工作区')}>
      <AppLink className="model-section-link" href={modelPath(modelId!)} navigate={() => navigation.goTo('graph')} aria-current={navigation.current === 'graph' ? 'page' : undefined}>
        <Network size={15} aria-hidden="true" /><span>{t('图模型')}</span>
      </AppLink>
      {navigation.catalogs && <AppLink className="model-section-link" href={modelPath(modelId!, 'catalogs')} navigate={() => navigation.goTo('catalogs')} aria-current={navigation.current === 'catalogs' ? 'page' : undefined}>
        <Database size={15} aria-hidden="true" /><span>{t('数据源')}</span>
        {navigation.sourceCount !== undefined && <span className="tab-count">{navigation.sourceCount}</span>}
      </AppLink>}
      {navigation.query && <AppLink className="model-section-link" href={modelPath(modelId!, 'query')} navigate={() => navigation.goTo('query')} aria-current={navigation.current === 'query' ? 'page' : undefined}>
        <Terminal size={15} aria-hidden="true" /><span>{t('查询')}</span>
      </AppLink>}
    </nav>}
  </header>;
}
