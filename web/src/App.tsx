import { t } from './locale.ts';
import { lazy, Suspense, useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Bot, Database, LayoutDashboard, LogOut, Network, Settings2, ShieldCheck, Terminal, UserRound } from 'lucide-react';
import { api, errorText, type ModelSummary, type SavedModel } from './api';
import { AuthGate, type Session } from './Auth';
import { LanguageSelect } from './LanguageSelect';
import { Access, type Grants } from './Access';
import { Query } from './Query';
import { AppLink, modelPath } from './AppLink';
import { useMutations } from './Mutations';
import type { Workspace } from './Editor';
import type { Point } from './modelCanvasGeometry';
import { documentOf, emptyModel, modelOf } from './model';
import { Modal, NameDialog } from './ui';
import { ConsoleNavigation } from './ConsoleNavigation';
import { ModelWorkspaceHeader } from './ModelWorkspaceHeader';
import { Models, type DirectoryState } from './Models';
import type { SettingsTab } from './Settings';
import './styles.css';
import './panels.css';
import './directory.css';

const Settings = lazy(() => import('./Settings'));
const AI = lazy(() => import('./AI'));
const Overview = lazy(() => import('./Overview'));
const RecoveryConsole = lazy(() => import('./RecoveryConsole'));
const Editor = lazy(() => import('./Editor'));

const nav = [
  { key: 'graph', label: t('图模型'), Icon: Network },
  { key: 'catalogs', label: t('数据源'), Icon: Database },
  { key: 'query', label: t('查询'), Icon: Terminal },
] as const;

const isOverview = (path: string) => /^\/overview\/?$/.test(path);

function route(path: string): { id?: string; workspace: Workspace } {
  const [, id, section] = /^\/models\/([A-Za-z0-9][A-Za-z0-9_-]{0,63})(?:\/(catalogs|query))?\/?$/.exec(path) ?? [];
  const workspace = section === 'catalogs' || path === '/catalogs' ? 'catalogs'
    : section === 'query' || path === '/query' ? 'query' : 'graph';
  return { id, workspace };
}

function App({ logout, locked, identity }: { logout?: () => Promise<void>; locked: boolean; identity: Session }) {
  const administrator = identity.administrator;
  const actor = identity.subject ?? identity.username ?? '@dev';
  const [grants, setGrants] = useState<Grants>();
  const [permissionError, setPermissionError] = useState('');
  const [settingsTab, setSettingsTab] = useState<SettingsTab>();
  const [receipts, setReceipts] = useState(false);
  const [recovered, setRecovered] = useState<SavedModel>();
  const [path, setPath] = useState(window.location.pathname);
  const [overviewVisited, setOverviewVisited] = useState(() => isOverview(path));
  const directory = useRef<Record<Workspace, DirectoryState>>({
    graph: { search: '', filter: 'all', scroll: 0 }, catalogs: { search: '', filter: 'all', scroll: 0 }, query: { search: '', filter: 'all', scroll: 0 },
  });
  const canvasLayouts = useRef(new Map<string, Map<string, Point>>());
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
  const currentWorkspace = useRef<Workspace>('graph');
  const onDirty = useCallback((id: string, value: boolean) => {
    if (value) dirty.current.add(id);
    else dirty.current.delete(id);
  }, []);
  const reloadModels = useCallback(() => retry(value => value + 1), []);

  useEffect(() => {
    if (locked) return;
    const controller = new AbortController();
    setError('');
    api<ModelSummary[]>('/models', { signal: controller.signal }).then(setModels, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [attempt, locked]);

  useEffect(() => {
    if (locked) return;
    const controller = new AbortController();
    const options = { signal: controller.signal };
    if (!administrator) api<Grants>('/my-roles', options).then(value => {
      setGrants(value); setPermissionError('');
    }, cause => {
      if (!controller.signal.aborted) setPermissionError(errorText(cause));
    });
    api<{ mutation_receipts: boolean }>('/options', options).then(value => setReceipts(value.mutation_receipts), () => {});
    return () => controller.abort();
  }, [administrator, locked, attempt]);

  const navigate = useCallback((next: string, replace = false) => {
    if (next === current.current) return;
    const leaving = current.current.replace(/\/$/, '');
    const unsaved = leaving === '/new' && dirty.current.has('')
      || leaving === '/settings' && (dirty.current.has('/settings') || dirty.current.has('/settings/mcp'));
    if (unsaved && !replace && !window.confirm(t('当前页面有未保存的修改。离开当前页面？'))) {
      if (window.location.pathname !== current.current) window.history.pushState(null, '', current.current);
      return;
    }
    if (['/', '/catalogs', '/query'].includes(current.current)) directory.current[currentWorkspace.current].scroll = window.scrollY;
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
    if (isOverview(next)) setOverviewVisited(true);
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
    const scrollRestoration = window.history.scrollRestoration;
    window.history.scrollRestoration = 'manual';
    window.addEventListener('popstate', pop);
    window.addEventListener('beforeunload', unload);
    return () => {
      window.history.scrollRestoration = scrollRestoration;
      window.removeEventListener('popstate', pop);
      window.removeEventListener('beforeunload', unload);
    };
  }, [locked, navigate]);

  const location = route(path);
  const overviewPage = isOverview(path);
  const accessPage = path.replace(/\/$/, '') === '/access';
  const settingsPage = path.replace(/\/$/, '') === '/settings';
  const aiPage = path.replace(/\/$/, '') === '/ai';
  const id = location.id;
  const availableModels = administrator ? models : grants && models?.filter(model => grantFor(model.id).length);
  function grantFor(model: string) { return grants && Object.hasOwn(grants, model) ? grants[model] : []; }
  function canReadModel(model: string) { return administrator || grantFor(model).some(role => role !== 'query' && role !== 'export'); }
  function canQuery(model: string) { return administrator || grantFor(model).includes('query'); }
  function canExport(model: string) { return administrator || canQuery(model) && grantFor(model).includes('export'); }
  const hasModelAccess = administrator || Object.values(grants ?? {}).some(roles => roles.some(role => role !== 'query' && role !== 'export'));
  const canUseQuery = administrator || Object.values(grants ?? {}).some(roles => roles.includes('query'));
  const workspace = location.workspace === 'graph' && !hasModelAccess && canUseQuery ? 'query' : location.workspace;
  const workspaceModels = availableModels?.filter(model => workspace === 'query' ? canQuery(model.id) : canReadModel(model.id));
  currentWorkspace.current = workspace;
  const mutations = useMutations(actor, receipts, model => {
    setRecovered(model);
    reloadModels();
    if (model && path === '/new') navigate(modelPath(model.id), true);
  });
  useLayoutEffect(() => { window.scrollTo(0, ['/', '/catalogs', '/query'].includes(path) ? directory.current[workspace].scroll : 0); }, [path, workspace]);
  const pageTitle = overviewPage ? t('平台概览') : aiPage ? 'AI' : settingsPage ? t('运行配置') : accessPage ? t('权限管理')
    : nav.find(item => item.key === workspace)!.label;
  const selectedName = id && availableModels?.find(model => model.id === id)?.name;
  useEffect(() => {
    if (!locked) document.title = [selectedName, pageTitle, 'GraphLake'].filter(Boolean).join(' · ');
  }, [locked, pageTitle, selectedName]);
  const newModel = administrator && path === '/new' ? draft : undefined;

  async function importModel(file: File) {
    setImportError('');
    try {
      if (file.size > 1024 * 1024) throw new Error(t('YAML 文件超过 1 MiB'));
      const yaml = await file.text();
      const name = modelOf(yaml).graph || file.name.replace(/\.ya?ml$/i, '');
      setImported({ yaml, name });
    } catch (cause) { setImportError(errorText(cause)); }
  }

  function workspaceHref(next: Workspace) {
    if (newModel && next === 'graph') return '/new';
    if (id && (next === 'query' ? canQuery(id) : canReadModel(id))) return modelPath(id, next);
    return next === 'graph' ? '/' : '/' + next;
  }

  async function signOut() {
    if (!logout) return;
    setConfirmLogout(false);
    setLoggingOut(true);
    setLogoutError('');
    try { await logout(); }
    catch (cause) { setLogoutError(errorText(cause)); setLoggingOut(false); }
  }

  const account = <div className="rail-items rail-account">
    <LanguageSelect />
    <div className="rail-identity">
      <UserRound size={20} aria-hidden="true" />
      <div className="rail-identity-text">
        {identity.enabled ? <>
          <bdi className="rail-username" title={identity.username ?? undefined}>{identity.username}</bdi>
          <span>{administrator ? t('管理员') : t('普通用户')}</span>
        </> : <span>{t('未启用认证')}</span>}
      </div>
    </div>
    {logout && <button className="rail-link" onClick={() => dirty.current.size ? setConfirmLogout(true) : void signOut()} disabled={loggingOut}>
      <LogOut size={20} aria-hidden="true" /><span>{t('退出登录')}</span>
    </button>}
  </div>;

  return <div className="app-shell">
    <a className="skip-link" href="#workspace-content">{t('跳转到工作区')}</a>
    <ConsoleNavigation page={pageTitle} home={() => navigate('/')} account={account}>
      <span className="rail-section">{t('工作区')}</span>
      {administrator && <AppLink className="rail-link" href="/overview" navigate={navigate} aria-current={overviewPage ? 'page' : undefined}>
        <LayoutDashboard size={20} aria-hidden="true" /><span>{t('平台概览')}</span>
      </AppLink>}
      {nav.filter(({ key }) => key === 'query' ? canUseQuery : key === 'catalogs' ? administrator : hasModelAccess)
      .map(({ key, label, Icon }) =>
        <AppLink key={key} className="rail-link" href={workspaceHref(key)} navigate={navigate}
          aria-current={!overviewPage && !accessPage && !settingsPage && !aiPage && workspace === key ? 'page' : undefined}>
          <Icon size={20} aria-hidden="true" /><span>{label}</span>
        </AppLink>)}
      <AppLink className="rail-link" href="/ai" navigate={navigate} aria-current={aiPage ? 'page' : undefined}>
        <Bot size={20} aria-hidden="true" /><span>AI</span>
      </AppLink>
      {administrator && <span className="rail-section">{t('管理')}</span>}
      {administrator && identity.enabled && <AppLink className="rail-link" href="/access" navigate={navigate} aria-current={accessPage ? 'page' : undefined}>
        <ShieldCheck size={20} aria-hidden="true" /><span>{t('权限管理')}</span>
      </AppLink>}
      {administrator && <AppLink className="rail-link" href="/settings" navigate={next => { setSettingsTab(undefined); navigate(next); }} aria-current={settingsPage ? 'page' : undefined}>
        <Settings2 size={20} aria-hidden="true" /><span>{t('运行配置')}</span>
      </AppLink>}
    </ConsoleNavigation>
    <div className="app-content" id="workspace-content" tabIndex={-1}>
    {mutations.banner && <div className="global-mutation">{mutations.banner}</div>}
    {openModels.map(model => {
      const modelers = administrator || grantFor(model).includes('modeler');
      const publishers = administrator || grantFor(model).includes('publisher');
      const page = !canReadModel(model) || workspace === 'query' && canQuery(model) ? 'query' : administrator && workspace === 'catalogs' ? 'catalogs' : 'graph';
      const show = model === id;
      const summary = availableModels?.find(entry => entry.id === model);
      if (!canReadModel(model) && !canQuery(model)) return null;
      return <div className="model-workspace" key={model} data-inactive={!show || undefined}
        inert={!show} aria-hidden={!show || undefined}>
        {canReadModel(model)
          ? <Suspense fallback={<p className="empty" role="status">{t('正在打开模型…')}</p>}>
            <Editor id={model} active={show} onDirty={onDirty} workspace={show ? page : 'query'} models={workspaceModels ?? []}
              allow={{ configure: administrator, edit: modelers, publish: publishers, query: canQuery(model), export: canExport(model) }}
              actor={actor} mutations={mutations} recovered={recovered?.id === model ? recovered : undefined} canvasLayouts={canvasLayouts.current}
              close={() => navigate(page === 'graph' ? '/' : '/' + page)} goTo={next => navigate(modelPath(model, next))}
              changeModel={key => navigate(modelPath(key, page))}
              changed={reloadModels} created={key => navigate(modelPath(key), true)} />
            </Suspense>
          : <main className="workbench workspace-query">
              {summary?.active_revision && !summary.archive_error ? <>
                <ModelWorkspaceHeader workspace={t('查询')} name={summary.name} back={() => navigate('/query')} />
                <Query id={model} actor={actor} revision={summary.active_revision} draftPending={false}
                  refresh={reloadModels} visible={show} canExport={canExport(model)} />
              </> : <div className="directory-empty empty">
                <h1>{t('查询')}</h1>
                {error ? <><p role="alert">{error}</p><button className="button" onClick={reloadModels}>{t('重新加载')}</button></>
                  : <p role="status">{models ? t('此模型当前不可查询。请选择其他已发布的模型。') : t('正在读取模型…')}</p>}
                <AppLink className="text-button" href="/query" navigate={navigate}>{t('查询模型列表')}</AppLink>
              </div>}
            </main>}
      </div>;
    })}
    {newModel && <Suspense fallback={<p className="empty" role="status">{t('正在打开模型…')}</p>}>
      <Editor key={newModel.id} id={newModel.id} initial={newModel} active onDirty={onDirty}
      allow={{ configure: true, edit: true, publish: true, query: true, export: true }} actor={actor} mutations={mutations} recovered={recovered?.id === newModel.id ? recovered : undefined} canvasLayouts={canvasLayouts.current}
      workspace={workspace} models={models ?? []} close={() => navigate('/')} goTo={next => navigate(modelPath(newModel.id, next))}
      changeModel={key => navigate(modelPath(key, workspace))} changed={reloadModels} created={key => navigate(modelPath(key), true)} />
      </Suspense>}
    {accessPage && (administrator && identity.enabled ? <Access models={models ?? []} locked={locked} /> : <main className="workbench"><p className="empty">{t('此页面需要管理员权限。')}</p></main>)}
    {administrator && overviewVisited && <div hidden={!overviewPage}>
        <Suspense fallback={<p className="empty" role="status">{t('正在打开平台概览…')}</p>}>
          <Overview active={overviewPage && !locked} openModel={(id, page) => navigate(modelPath(id, page))}
            openSettings={tab => { setSettingsTab(tab); navigate('/settings'); }} />
        </Suspense>
      </div>}
    {overviewPage && !administrator && <main className="workbench"><p className="empty">{t('此页面需要管理员权限。')}</p></main>}
    {settingsPage && (administrator ? <Suspense fallback={<p className="empty" role="status">{t('正在打开运行配置…')}</p>}><Settings initialTab={settingsTab} openModel={(id, page) => navigate(modelPath(id, page))} models={models ?? []} locked={locked} onDirty={onDirty} /></Suspense> : <main className="workbench"><p className="empty">{t('此页面需要管理员权限。')}</p></main>)}
    {aiPage && <Suspense fallback={<p className="empty" role="status">{t('正在打开 AI…')}</p>}>
      <AI locked={locked} authEnabled={identity.enabled}
        openServiceSettings={administrator ? () => { setSettingsTab('mcp'); navigate('/settings'); } : undefined} />
    </Suspense>}
    {!overviewPage && !accessPage && !settingsPage && !aiPage && !newModel && (!id || !canReadModel(id) && !canQuery(id)) &&
      <Models key={workspace} memory={directory.current[workspace]} models={id && !administrator && grants ? [] : workspaceModels} error={permissionError || error}
        importError={workspace === 'graph' ? importError : ''} retry={reloadModels} workspace={workspace}
        administrator={administrator} canQuery={canQuery} health={() => { setSettingsTab('health'); navigate('/settings'); }}
        navigate={navigate}
        create={() => setCreating(true)} importModel={file => void importModel(file)} />}
    </div>
    {logoutError && <p className="notice error auth-logout-error" role="alert">{logoutError}</p>}
    {confirmLogout && !locked && <Modal layout="center" title={t('退出登录')} close={() => setConfirmLogout(false)}>
      <div className="dialog-form">
        <p>{t('有未保存的修改。退出登录会放弃这些修改。')}</p>
        <div className="dialog-actions">
          <button className="button" onClick={() => setConfirmLogout(false)} data-initial-focus>{t('继续编辑')}</button>
          <button className="button primary" onClick={() => void signOut()}>{t('放弃修改并退出')}</button>
        </div>
      </div>
    </Modal>}
    {imported && <NameDialog title={t('导入图模型')} initial={imported.name} modelIds={models?.map(model => model.id) ?? []} close={() => setImported(undefined)}
      submit={(name, key) => {
        let yaml = imported.yaml;
        if (modelOf(yaml).graph !== name) {
          const doc = documentOf(yaml);
          doc.set('graph', name);
          yaml = doc.toString();
        }
        setDraft({ id: key, yaml, revision: '', active_revision: null, status: 'unchecked' });
        navigate('/new');
      }} />}
    {creating && <NameDialog title={t('新建图模型')} modelIds={models?.map(model => model.id) ?? []} close={() => setCreating(false)}
      submit={(name, key) => {
        setDraft({ id: key, yaml: emptyModel(name), revision: '', active_revision: null, status: 'unchecked' });
        navigate('/new');
      }} />}
  </div>;
}

const recovery = /^\/recovery\/?$/.test(window.location.pathname);
document.title = recovery ? t('GraphLake · 灾备恢复验收') : 'GraphLake · ' + t('图模型');
createRoot(document.getElementById('root')!).render(recovery
  ? <Suspense fallback={<main className="empty" role="status">{t('正在打开恢复验收台…')}</main>}><RecoveryConsole /></Suspense>
  : <AuthGate workspace={(logout, locked, identity) => <App key={identity.authorization ?? identity.username ?? '@dev'} logout={logout} locked={locked} identity={identity} />} />,
);
