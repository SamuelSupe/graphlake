import { diagnosticText, t } from './locale.ts';
import { useLayoutEffect, useRef, useState } from 'react';
import { Activity, ArrowRight, Box, CircleAlert, Database, GitFork, LayoutGrid, List, LoaderCircle, Network, Plus, RefreshCw, Search, Terminal, Upload } from 'lucide-react';
import type { ModelSummary } from './api';
import { AppLink, modelPath } from './AppLink';
import type { Workspace } from './Editor';
import { Mark, StatusLabel } from './ui';

const workspaces = {
  graph: { title: t('图模型'), description: t('连接数据，构建关系，发布后即可查询。') },
  catalogs: { title: t('数据源'), description: t('选择模型，管理它的数据连接。') },
  query: { title: t('查询'), description: t('选择已发布的模型，开始探索数据。') },
};
const filters = [
  { key: 'all', label: t('全部') },
  { key: 'published', label: t('已发布') },
  { key: 'unpublished', label: t('未发布') },
  { key: 'attention', label: t('需处理') },
] as const;
export type Filter = typeof filters[number]['key'];

export type DirectoryState = { search: string; filter: Filter; scroll: number; view?: 'cards' | 'table'; returnFocus?: string };

function matches(model: ModelSummary, filter: Filter) {
  if (filter === 'published') return !!model.active_revision && !model.archive_error;
  if (filter === 'unpublished') return !model.active_revision;
  if (filter === 'attention') return model.status === 'failed' || !!model.archive_error;
  return true;
}

export function Models({ models, error, refreshing, importError, workspace, navigate, canQuery, memory, health, create, importModel, retry, administrator }: {
  health: () => void;
  memory: DirectoryState;
  administrator: boolean; models?: ModelSummary[]; error: string; refreshing: boolean; importError: string; workspace: Workspace;
  canQuery: (id: string) => boolean;
  navigate: (path: string) => void; create: () => void;
  importModel: (file: File) => void; retry: () => void;
}) {
  const [view, setView] = useState(memory.view ?? 'cards');
  const [search, updateSearch] = useState(memory.search);
  const setSearch = (value: string) => { memory.search = value; updateSearch(value); };
  const [filter, updateFilter] = useState<Filter>(memory.filter);
  const setFilter = (value: Filter) => { memory.filter = value; updateFilter(value); };
  const searchInput = useRef<HTMLInputElement>(null);
  const refreshButton = useRef<HTMLButtonElement>(null);
  useLayoutEffect(() => {
    if (!models || !memory.returnFocus) return;
    document.getElementById(memory.returnFocus)?.focus({ preventScroll: true });
    delete memory.returnFocus;
  }, [memory, models]);

  function follow(path: string) {
    memory.returnFocus = document.activeElement?.id;
    navigate(path);
  }

  const needle = search.trim().toLowerCase();
  const available = models?.filter(model => workspace !== 'query' || matches(model, 'published'));
  const activeFilter = workspace === 'query' ? 'all' : filter;
  const filtered = available?.filter(model => matches(model, activeFilter)
    && (model.name.toLowerCase().includes(needle) || model.id.toLowerCase().includes(needle))) ?? [];
  const statusFilters = filters.map(item => ({ ...item, count: available?.filter(model => matches(model, item.key)).length }));
  const narrowed = !!needle || activeFilter !== 'all';
  const { title, description } = workspaces[workspace];

  function resetFilters() {
    setSearch('');
    setFilter('all');
    searchInput.current?.focus();
  }

  const metrics = [
    { label: t('模型总数'), Icon: Network, value: available?.length },
    { label: t('数据源'), Icon: Database, value: available?.reduce((sum, model) => sum + model.sources, 0) },
    { label: t('节点标签'), Icon: Box, value: available?.reduce((sum, model) => sum + model.nodes, 0) },
    { label: t('边类型'), Icon: GitFork, value: available?.reduce((sum, model) => sum + model.edges, 0) },
  ];

  return <main className="workbench model-directory">
    <header className="directory-cover">
      <div className="directory-intro">
        <div className="directory-heading"><h1>{title}</h1><p>{description}</p></div>
        {workspace === 'graph' && administrator && <div className="workspace-context">
          <button className="button primary" onClick={create}><Plus size={16} aria-hidden="true" />{t('新建模型')}</button>
          <label className="button"><Upload size={16} aria-hidden="true" />{t('导入 YAML')}<input className="sr-only" type="file" accept=".yaml,.yml" onChange={event => {
            const file = event.target.files?.[0]; event.target.value = '';
            if (file) importModel(file);
          }} /></label>
        </div>}
      </div>
      <dl className="directory-metrics" aria-label={t('可见模型的结构汇总')}>
        {metrics.map(({ label, Icon, value }) => <div key={label}>
          <dt><Icon size={16} aria-hidden="true" />{label}</dt>
          <dd>{value === undefined ? '—' : value.toLocaleString()}</dd>
        </div>)}
      </dl>
    </header>
    <section className="model-library" aria-labelledby="library-heading">
      <div className="library-heading">
        <h2 id="library-heading">{t('模型目录')}</h2>
        <div className="directory-search">
          <label className="search"><Search size={17} aria-hidden="true" /><input ref={searchInput} type="search"
            aria-label={t('搜索模型名称或标识')} placeholder={t('搜索模型名称或标识')} value={search} onChange={event => setSearch(event.target.value)} /></label>
          {workspace !== 'query' && <select className="compact-model-filter" aria-label={t('按模型状态筛选')} value={activeFilter}
            onChange={event => setFilter(event.target.value as Filter)}>
            {statusFilters.map(({ key, label, count }) => <option key={key} value={key}>{label}{count !== undefined && ` (${count})`}</option>)}
          </select>}
          <button className="button refresh-models" ref={refreshButton} aria-disabled={refreshing} aria-label={refreshing ? t('正在刷新…') : t('刷新模型列表')}
            title={refreshing ? t('正在刷新…') : t('刷新模型列表')} onClick={() => { if (!refreshing) retry(); }}>
            {refreshing ? <LoaderCircle size={17} aria-hidden="true" /> : <RefreshCw size={17} aria-hidden="true" />}
          </button>
        </div>
        <div className="library-views" role="group" aria-label={t('目录视图')}>
          {([{ mode: 'cards', label: t('卡片视图'), Icon: LayoutGrid }, { mode: 'table', label: t('列表视图'), Icon: List }] as const)
            .map(({ mode, label, Icon }) => <button type="button" key={mode} aria-label={label} title={label} aria-pressed={mode === view}
              onClick={() => { memory.view = mode; setView(mode); }}><Icon size={16} aria-hidden="true" /></button>)}
        </div>
      </div>
      {workspace !== 'query' && <div className="directory-tools">
        <div className="model-filters" role="group" aria-label={t('按模型状态筛选')}>
          {statusFilters.map(item => <button key={item.key} aria-pressed={activeFilter === item.key}
            onClick={() => setFilter(item.key)}>{item.label}
            {item.count !== undefined && <span>{item.count}</span>}
          </button>)}
        </div>
      </div>}
      {importError && <p className="notice error listing-notice" role="alert">{importError}</p>}
      {error && models && <p className="notice error listing-notice" role="alert">{t('刷新失败，当前显示上次读取的目录。{0}', error)}</p>}
      {error && !models ? <div className="directory-empty panel-table" role="alert"><CircleAlert size={28} aria-hidden="true" />
        <h2>{t('暂时无法读取模型')}</h2><p>{error}</p><button className="button" disabled={refreshing} onClick={() => { refreshButton.current?.focus(); retry(); }}>{t('重新加载')}</button>
      </div> : !models ? <div className="directory-empty panel-table" role="status">{t('正在读取模型…')}</div>
        : !filtered.length ? <div className="directory-empty panel-table">
          {narrowed ? <Search size={28} aria-hidden="true" /> : <Network size={32} aria-hidden="true" />}
          <h2>{narrowed ? t('没有符合条件的模型') : workspace === 'query' ? t('暂无可查询的模型') : administrator && workspace === 'graph' ? t('创建第一个图模型') : t('暂无图模型')}</h2>
          <p>{narrowed ? t('试试其他名称或标识，或清除筛选查看全部模型。')
            : workspace === 'query' ? t('模型发布后会显示在这里。') : administrator ? t('连接数据源，定义节点和边，再发布模型开始查询。') : t('获得模型访问权限后，可在这里查看模型。')}</p>
          {narrowed ? <button className="button" onClick={resetFilters}>{t('清除筛选')}</button>
            : workspace === 'graph' && administrator && <button className="button primary" onClick={create}><Plus size={16} aria-hidden="true" />{t('新建模型')}</button>}
        </div>
        : <div className="panel-table directory" data-view={view}><table className="models-table" role="table" aria-label={title}>
          <thead role="rowgroup"><tr role="row"><th scope="col">{t('模型')}</th><th scope="col">{t('模型结构')}</th><th scope="col">{t('发布状态')}</th><th scope="col">{t('草稿校验')}</th><th scope="col"><span className="sr-only">{t('操作')}</span></th></tr></thead>
          <tbody role="rowgroup">{filtered.map(model => <tr key={model.id} role="row" className="directory-row">
            <th scope="row" role="rowheader" className="directory-name"><AppLink id={'model-name-' + model.id} className="model-name" href={modelPath(model.id, workspace)} navigate={follow}>
              <span className="model-icon"><Mark size={20} /></span><span>{model.name}<code className="model-id">{model.id}</code></span>
            </AppLink></th>
            <td role="cell" className="directory-structure"><div className="model-counts">
              <span>{t('数据源')} <strong>{model.sources}</strong></span>
              <span>{t('节点标签')} <strong>{model.nodes}</strong></span>
              <span>{t('边类型')} <strong>{model.edges}</strong></span>
            </div></td>
            <td role="cell" className="directory-release">
              <small className="directory-field-label">{t('发布状态')}</small>
              <span className={'release-label ' + (model.archive_error ? 'unavailable' : model.active_revision ? 'published' : '')}>
                <i aria-hidden="true" />{model.archive_error ? t('版本异常') : model.active_revision ? t('已发布') : t('未发布')}
              </span>
              {model.archive_error && <small className="version-hint" title={diagnosticText(model.archive_error)}>{t('打开模型查看异常')}</small>}
            </td>
            <td role="cell" className="directory-status"><small className="directory-field-label">{t('草稿校验')}</small><StatusLabel status={model.status} /></td>
            <td role="cell" className="directory-actions"><div className="model-actions">
              {workspace === 'graph' && canQuery(model.id) && matches(model, 'published') && <AppLink id={'model-query-' + model.id} className="text-button" aria-label={t('查询 {0}', model.name)} href={modelPath(model.id, 'query')} navigate={follow}><Terminal size={15} aria-hidden="true" />{t('查询')}</AppLink>}
              <AppLink id={'model-open-' + model.id} className="text-button" aria-label={(workspace === 'query' ? t('查询 ') : t('打开 ')) + model.name} href={modelPath(model.id, workspace)} navigate={follow}>{workspace === 'query' ? t('查询') : t('打开')} <ArrowRight size={16} aria-hidden="true" /></AppLink>
            </div></td>
          </tr>)}</tbody>
        </table></div>}
      {available && <div className="directory-footer"><span role="status">{refreshing ? t('正在刷新…') : t('显示 {0} / {1} 个模型', filtered.length, available.length)}</span>
        <div className="actions">
          {narrowed && filtered.length > 0 && <button className="text-button" onClick={resetFilters}>{t('清除筛选')}</button>}
          {workspace === 'graph' && administrator && <button className="text-button" onClick={health}><Activity size={16} aria-hidden="true" />{t('运行健康')}</button>}
        </div>
      </div>}
    </section>
  </main>;
}
