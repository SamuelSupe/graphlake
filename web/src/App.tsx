import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { ArrowRight, Database, LogOut, Network, Plus, Search, Terminal, Upload } from 'lucide-react';
import { api, errorText, type ModelSummary, type SavedModel } from './api';
import { AuthGate } from './Auth';
import { Editor, type Workspace } from './Editor';
import { documentOf, emptyModel, modelOf } from './model';
import { Mark, Modal, NameDialog, StatusLabel } from './ui';
import './styles.css';

const nav = [
  { key: 'graph', label: '图模型', Icon: Network },
  { key: 'catalogs', label: '数据源', Icon: Database },
  { key: 'query', label: '查询', Icon: Terminal },
] as const;

function modelPath(id: string, workspace: Workspace = 'graph') {
  return '/models/' + id + (workspace === 'graph' ? '' : '/' + workspace);
}

function route(path: string): { id?: string; workspace: Workspace } {
  const [, id, section] = /^\/models\/([A-Za-z0-9][A-Za-z0-9_-]{0,63})(?:\/(catalogs|query))?\/?$/.exec(path) ?? [];
  const workspace = section === 'catalogs' || path === '/catalogs' ? 'catalogs'
    : section === 'query' || path === '/query' ? 'query' : 'graph';
  return { id, workspace };
}

function Models({ models, error, importError, workspace, open, create, importModel, retry }: {
  models?: ModelSummary[]; error: string; importError: string; workspace: Workspace;
  open: (id: string) => void; create: () => void; importModel: (file: File) => void; retry: () => void;
}) {
  const [search, setSearch] = useState('');
  const needle = search.trim().toLowerCase();
  const available = models?.filter(model => workspace !== 'query' || model.active_revision);
  const filtered = available?.filter(model => model.name.toLowerCase().includes(needle) || model.id.toLowerCase().includes(needle)) ?? [];
  const title = nav.find(item => item.key === workspace)!.label;
  return <main className="workbench">
    <header className="workspace-header"><div className="workspace-title"><h1>{title}</h1></div>
      {workspace === 'graph' && <div className="workspace-context">
        <label className="button compact"><Upload size={16} />导入模型 YAML<input className="sr-only" type="file" accept=".yaml,.yml" onChange={event => {
          const file = event.target.files?.[0]; event.target.value = '';
          if (file) importModel(file);
        }} /></label>
        <button className="button primary compact" onClick={create}><Plus size={16} />新建模型</button>
      </div>}
    </header>
    <div className="listing">
      <div className="table-tools">
        <label className="search"><Search size={17} aria-hidden="true" /><input type="search" aria-label="搜索模型名称或标识" placeholder="搜索模型名称或标识"
          value={search} onChange={event => setSearch(event.target.value)} /></label>
        {available && <span role="status">{filtered.length} 个模型</span>}
      </div>
      {importError && <p className="notice error listing-notice" role="alert">{importError}</p>}
      {error ? <div className="empty" role="alert">{error}<button className="button" onClick={retry}>重试</button></div>
        : !models ? <p className="empty" role="status">正在读取模型…</p>
        : <div className="panel-table directory"><table className="models-table" role="table" aria-label={title}>
          <thead role="rowgroup"><tr role="row"><th scope="col">模型名称</th><th scope="col">标识</th><th scope="col">数据源</th><th scope="col">节点标签</th><th scope="col">边类型</th><th scope="col">校验状态</th><th scope="col"><span className="sr-only">操作</span></th></tr></thead>
          <tbody role="rowgroup">{filtered.map(model => <tr key={model.id} role="row" className="directory-row">
            <th scope="row" role="rowheader" className="directory-name"><button className="model-name" onClick={() => open(model.id)}><span className="model-icon"><Mark size={18} /></span><span>{model.name}</span></button></th>
            <td role="cell" className="directory-detail"><code className="tag">{model.id}</code></td>
            <td role="cell" className="directory-count"><small aria-hidden="true">数据源</small>{model.sources}</td>
            <td role="cell" className="directory-count"><small aria-hidden="true">节点标签</small>{model.nodes}</td>
            <td role="cell" className="directory-count"><small aria-hidden="true">边类型</small>{model.edges}</td>
            <td role="cell" className="directory-status"><StatusLabel status={model.status} /><small className="version-hint" title={model.active_revision ?? undefined}>{model.active_revision ? '生效 ' + model.active_revision.slice(0, 10) : '未发布'}</small></td>
            <td role="cell" className="directory-actions"><button className="text-button" aria-label={'打开 ' + model.name} onClick={() => open(model.id)}>打开 <ArrowRight size={16} /></button></td>
          </tr>)}
          {!filtered.length && <tr><td colSpan={7}><div className="empty">{needle ? '没有匹配的图模型' : workspace === 'query' ? '暂无已发布的图模型' : '暂无图模型'}</div></td></tr>}
          </tbody>
        </table></div>}
    </div>
  </main>;
}

function App({ logout, locked }: { logout?: () => Promise<void>; locked: boolean }) {
  const [path, setPath] = useState(window.location.pathname);
  const [openModels, setOpenModels] = useState(() => {
    const id = route(window.location.pathname).id;
    return id ? [id] : [];
  });
  const [draft, setDraft] = useState<SavedModel>();
  const [creating, setCreating] = useState(false);
  const [imported, setImported] = useState<{ yaml: string; name: string }>();
  const [importError, setImportError] = useState('');
  const [models, setModels] = useState<ModelSummary[]>();
  const [error, setError] = useState('');
  const [logoutError, setLogoutError] = useState('');
  const [loggingOut, setLoggingOut] = useState(false);
  const [confirmLogout, setConfirmLogout] = useState(false);
  const [attempt, retry] = useState(0);
  const dirty = useRef(new Set<string>());
  const current = useRef(path);
  const lastModel = useRef<string>(undefined);
  const onDirty = useCallback((id: string, value: boolean) => {
    if (value) dirty.current.add(id);
    else dirty.current.delete(id);
  }, []);
  const reloadModels = useCallback(() => retry(value => value + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    setError('');
    api<ModelSummary[]>('/models', { signal: controller.signal }).then(setModels, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [attempt]);

  const navigate = useCallback((next: string, replace = false) => {
    if (next === current.current) return;
    if (current.current === '/new' && !replace && dirty.current.has('') && !window.confirm('新模型有未保存的修改。离开当前页面？')) {
      if (window.location.pathname !== current.current) window.history.pushState(null, '', current.current);
      return;
    }
    const nextId = route(next).id;
    if (nextId) setOpenModels(previous => {
      const recent = [...previous.filter(id => id !== nextId), nextId];
      const evict = recent.length > 5 ? recent.findIndex(id => id !== nextId && !dirty.current.has(id)) : -1;
      if (evict >= 0) recent.splice(evict, 1);
      return recent;
    });
    if (window.location.pathname !== next) {
      if (replace) window.history.replaceState(null, '', next);
      else window.history.pushState(null, '', next);
    }
    current.current = next;
    setPath(next);
  }, []);

  useEffect(() => {
    const pop = () => {
      if (locked) window.history.replaceState(null, '', current.current);
      else navigate(window.location.pathname);
    };
    function unload(event: BeforeUnloadEvent) {
      if (dirty.current.size) { event.preventDefault(); event.returnValue = true; }
    }
    window.addEventListener('popstate', pop);
    window.addEventListener('beforeunload', unload);
    return () => {
      window.removeEventListener('popstate', pop);
      window.removeEventListener('beforeunload', unload);
    };
  }, [locked, navigate]);

  const { id, workspace } = route(path);
  useEffect(() => { if (id) lastModel.current = id; }, [id]);
  useLayoutEffect(() => { window.scrollTo(0, 0); }, [path]);
  useEffect(() => { if (!locked) document.title = 'GraphLake · ' + nav.find(item => item.key === workspace)!.label; }, [locked, workspace]);
  const newModel = path === '/new' ? draft : undefined;

  async function importModel(file: File) {
    setImportError('');
    try {
      if (file.size > 1024 * 1024) throw new Error('YAML 文件超过 1 MiB');
      const yaml = await file.text();
      const name = modelOf(yaml).graph || file.name.replace(/\.ya?ml$/i, '');
      setImported({ yaml, name });
    } catch (cause) { setImportError(errorText(cause)); }
  }

  function openWorkspace(next: Workspace) {
    if (newModel) {
      if (next !== 'graph') navigate('/' + next);
      return;
    }
    if (next === 'graph') {
      navigate(id ? modelPath(id) : '/');
      return;
    }
    const target = id ?? models?.find(model => model.id === lastModel.current && (next !== 'query' || model.active_revision))?.id
      ?? models?.find(model => next !== 'query' || model.active_revision)?.id;
    navigate(target ? modelPath(target, next) : '/' + next);
  }

  async function signOut() {
    if (!logout) return;
    setConfirmLogout(false);
    setLoggingOut(true);
    setLogoutError('');
    try { await logout(); }
    catch (cause) { setLogoutError(errorText(cause)); setLoggingOut(false); }
  }

  return <div className="app-shell">
    <nav className="app-rail" aria-label="主导航">
      <button className="rail-brand" aria-label="GraphLake 首页" onClick={() => navigate('/')}><Mark size={26} /></button>
      <div className="rail-items">{nav.map(({ key, label, Icon }) =>
        <button key={key} className="rail-link" aria-current={workspace === key ? 'page' : undefined} onClick={() => openWorkspace(key)}>
          <Icon size={20} aria-hidden="true" /><span>{label}</span>
        </button>)}</div>
      {logout && <div className="rail-items rail-account">
        <button className="rail-link" onClick={() => dirty.current.size ? setConfirmLogout(true) : void signOut()} disabled={loggingOut}>
          <LogOut size={20} aria-hidden="true" /><span>退出登录</span>
        </button>
      </div>}
    </nav>
    {openModels.map(model => <div className="model-workspace" key={model} data-inactive={model !== id || undefined}
      inert={model !== id} aria-hidden={model !== id || undefined}>
      <Editor id={model} active={model === id} onDirty={onDirty} workspace={model === id ? workspace : 'query'} models={models ?? []}
          close={() => navigate('/')} goTo={next => navigate(modelPath(model, next))}
          changeModel={key => navigate(modelPath(key, workspace))}
          changed={reloadModels} created={key => navigate(modelPath(key), true)} />
    </div>)}
    {newModel && <Editor key="draft" id={newModel.id} initial={newModel} active onDirty={onDirty}
      workspace={workspace} models={models ?? []} close={() => navigate('/')} goTo={next => navigate(modelPath(newModel.id, next))}
      changeModel={key => navigate(modelPath(key, workspace))} changed={reloadModels} created={key => navigate(modelPath(key), true)} />}
    {!id && !newModel && <Models models={models} error={error} importError={workspace === 'graph' ? importError : ''} retry={reloadModels} workspace={workspace}
      open={key => navigate(modelPath(key, workspace))} create={() => setCreating(true)} importModel={file => void importModel(file)} />}
    {logoutError && <p className="notice error auth-logout-error" role="alert">{logoutError}</p>}
    {confirmLogout && !locked && <Modal title="退出登录" close={() => setConfirmLogout(false)}>
      <div className="dialog-form">
        <p>模型有未保存的修改。退出登录会放弃这些修改。</p>
        <div className="dialog-actions">
          <button className="button" onClick={() => setConfirmLogout(false)} data-initial-focus>继续编辑</button>
          <button className="button primary" onClick={() => void signOut()}>放弃修改并退出</button>
        </div>
      </div>
    </Modal>}
    {imported && <NameDialog title="导入图模型" initial={imported.name} withId close={() => setImported(undefined)}
      submit={(name, key) => {
        if (models?.some(model => model.id === key)) throw new Error('模型标识已存在');
        let yaml = imported.yaml;
        if (modelOf(yaml).graph !== name) {
          const doc = documentOf(yaml);
          doc.set('graph', name);
          yaml = doc.toString();
        }
        setDraft({ id: key, yaml, revision: '', active_revision: null, status: 'unchecked' });
        navigate('/new');
      }} />}
    {creating && <NameDialog title="新建图模型" withId close={() => setCreating(false)}
      submit={(name, key) => {
        setDraft({ id: key, yaml: emptyModel(name), revision: '', active_revision: null, status: 'unchecked' });
        navigate('/new');
      }} />}
  </div>;
}

createRoot(document.getElementById('root')!).render(
  <AuthGate workspace={(logout, locked) => <App logout={logout} locked={locked} />} />,
);
