import { useRef, useState } from 'react';
import { Activity, ArrowRight, CircleAlert, Network, Plus, RefreshCw, Search, Terminal, Upload } from 'lucide-react';
import type { ModelSummary } from './api';
import type { Workspace } from './Editor';
import { Mark, StatusLabel } from './ui';

const workspaces = {
  graph: { title: '图模型', description: '连接数据，构建关系，发布后即可查询。' },
  catalogs: { title: '数据源', description: '选择模型，管理它的数据连接。' },
  query: { title: '查询', description: '选择已发布的模型，开始探索数据。' },
};
const filters = [
  { key: 'all', label: '全部' },
  { key: 'published', label: '已发布' },
  { key: 'unpublished', label: '未发布' },
  { key: 'attention', label: '需处理' },
] as const;
export type Filter = typeof filters[number]['key'];

export type DirectoryState = { search: string; filter: Filter; scroll: number };

function matches(model: ModelSummary, filter: Filter) {
  if (filter === 'published') return !!model.active_revision && !model.archive_error;
  if (filter === 'unpublished') return !model.active_revision;
  if (filter === 'attention') return model.status === 'failed' || !!model.archive_error;
  return true;
}

export function Models({ models, error, importError, workspace, open, query, canQuery, memory, health, create, importModel, retry, administrator }: {
  health: () => void;
  memory: DirectoryState;
  administrator: boolean; models?: ModelSummary[]; error: string; importError: string; workspace: Workspace;
  canQuery: (id: string) => boolean;
  open: (id: string) => void; query?: (id: string) => void; create: () => void;
  importModel: (file: File) => void; retry: () => void;
}) {
  const [search, updateSearch] = useState(memory.search);
  const setSearch = (value: string) => { memory.search = value; updateSearch(value); };
  const [filter, updateFilter] = useState<Filter>(memory.filter);
  const setFilter = (value: Filter) => { memory.filter = value; updateFilter(value); };
  const searchInput = useRef<HTMLInputElement>(null);
  const needle = search.trim().toLowerCase();
  const available = models?.filter(model => workspace !== 'query' || matches(model, 'published'));
  const activeFilter = workspace === 'query' ? 'all' : filter;
  const filtered = available?.filter(model => matches(model, activeFilter)
    && (model.name.toLowerCase().includes(needle) || model.id.toLowerCase().includes(needle))) ?? [];
  const narrowed = !!needle || activeFilter !== 'all';
  const { title, description } = workspaces[workspace];

  function resetFilters() {
    setSearch('');
    setFilter('all');
    searchInput.current?.focus();
  }

  return <main className="workbench model-directory">
    <header className="workspace-header directory-header">
      <div className="directory-heading"><h1>{title}</h1><p>{description}</p></div>
      {workspace === 'graph' && administrator && <div className="workspace-context">
        <label className="button"><Upload size={16} aria-hidden="true" />导入 YAML<input className="sr-only" type="file" accept=".yaml,.yml" onChange={event => {
          const file = event.target.files?.[0]; event.target.value = '';
          if (file) importModel(file);
        }} /></label>
        <button className="button primary" onClick={create}><Plus size={16} aria-hidden="true" />新建模型</button>
      </div>}
    </header>
    <div className="listing">
      <div className="directory-tools">
        {workspace !== 'query' && <div className="model-filters" role="group" aria-label="按模型状态筛选">
          {filters.map(item => <button key={item.key} aria-pressed={activeFilter === item.key}
            onClick={() => setFilter(item.key)}>{item.label}
            {available && !error && <span>{available.filter(model => matches(model, item.key)).length}</span>}
          </button>)}
        </div>}
        <div className="directory-search">
          <label className="search"><Search size={17} aria-hidden="true" /><input ref={searchInput} type="search"
            aria-label="搜索模型名称或标识" placeholder="搜索模型名称或标识" value={search} onChange={event => setSearch(event.target.value)} /></label>
          <button className="button refresh-models" aria-label="刷新模型列表" title="刷新模型列表" onClick={retry}><RefreshCw size={17} aria-hidden="true" /></button>
        </div>
      </div>
      {importError && <p className="notice error listing-notice" role="alert">{importError}</p>}
      {error ? <div className="directory-empty panel-table" role="alert"><CircleAlert size={28} aria-hidden="true" />
        <h2>暂时无法读取模型</h2><p>{error}</p><button className="button" onClick={retry}>重新加载</button>
      </div> : !models ? <div className="directory-empty panel-table" role="status">正在读取模型…</div>
        : !filtered.length ? <div className="directory-empty panel-table">
          {narrowed ? <Search size={28} aria-hidden="true" /> : <Network size={32} aria-hidden="true" />}
          <h2>{narrowed ? '没有符合条件的模型' : workspace === 'query' ? '暂无可查询的模型' : administrator && workspace === 'graph' ? '创建第一个图模型' : '暂无图模型'}</h2>
          <p>{narrowed ? '试试其他名称或标识，或清除筛选查看全部模型。'
            : workspace === 'query' ? '模型发布后会显示在这里。' : administrator ? '连接数据源，定义节点和边，再发布模型开始查询。' : '获得模型访问权限后，可在这里查看模型。'}</p>
          {narrowed ? <button className="button" onClick={resetFilters}>清除筛选</button>
            : workspace === 'graph' && administrator && <button className="button primary" onClick={create}><Plus size={16} aria-hidden="true" />新建模型</button>}
        </div>
        : <div className="panel-table directory"><table className="models-table" role="table" aria-label={title}>
          <thead role="rowgroup"><tr role="row"><th scope="col">模型</th><th scope="col">模型结构</th><th scope="col">发布状态</th><th scope="col">草稿校验</th><th scope="col"><span className="sr-only">操作</span></th></tr></thead>
          <tbody role="rowgroup">{filtered.map(model => <tr key={model.id} role="row" className="directory-row">
            <th scope="row" role="rowheader" className="directory-name"><button className="model-name" onClick={() => open(model.id)}>
              <span className="model-icon"><Mark size={20} /></span><span>{model.name}<code className="model-id">{model.id}</code></span>
            </button></th>
            <td role="cell" className="directory-structure"><div className="model-counts">
              <span><strong>{model.sources}</strong> 数据源</span><span><strong>{model.nodes}</strong> 节点标签</span><span><strong>{model.edges}</strong> 边类型</span>
            </div></td>
            <td role="cell" className="directory-release">
              <small className="directory-field-label">发布状态</small>
              <span className={'release-label ' + (model.archive_error ? 'unavailable' : model.active_revision ? 'published' : '')}>
                <i aria-hidden="true" />{model.archive_error ? '版本异常' : model.active_revision ? '已发布' : '未发布'}
              </span>
              {model.archive_error && <small className="version-hint" title={model.archive_error}>打开模型查看异常</small>}
            </td>
            <td role="cell" className="directory-status"><small className="directory-field-label">草稿校验</small><StatusLabel status={model.status} /></td>
            <td role="cell" className="directory-actions"><div className="model-actions">
              {workspace !== 'query' && query && canQuery(model.id) && matches(model, 'published') && <button className="text-button" aria-label={'查询 ' + model.name} onClick={() => query(model.id)}><Terminal size={15} aria-hidden="true" />查询</button>}
              <button className="text-button" aria-label={(workspace === 'query' ? '查询 ' : '打开 ') + model.name} onClick={() => open(model.id)}>{workspace === 'query' ? '查询' : '打开'} <ArrowRight size={16} aria-hidden="true" /></button>
            </div></td>
          </tr>)}</tbody>
        </table></div>}
      {available && !error && <div className="directory-footer"><span role="status">显示 {filtered.length} / {available.length} 个模型</span>
        <div className="actions">
          {narrowed && filtered.length > 0 && <button className="text-button" onClick={resetFilters}>清除筛选</button>}
          {workspace === 'graph' && administrator && <button className="text-button" onClick={health}><Activity size={16} aria-hidden="true" />运行健康</button>}
        </div>
      </div>}
    </div>
  </main>;
}
