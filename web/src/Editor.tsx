import { diagnosticText, t } from './locale.ts';
import type { useMutations } from './Mutations';
import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, Braces, Box, Database, GitCommitHorizontal, History, List, Pencil, Plus, RefreshCw, Save, ShieldCheck, Upload, Waypoints, X } from 'lucide-react';
import { api, ApiError, downloadText, errorText, type Column, type ModelSummary, type SavedModel, type Validation, type ValidationSite } from './api';
import { newConnector } from './connectors';
import { AddSource } from './AddSource';
import { Catalogs } from './Catalogs';
import type { LocalTable } from './LocalTables';
import { SourceBrowser, type TableSelection } from './SourceBrowser';
import type { CanvasConnection } from './GraphCanvas';
import { EdgeDraftDialog, type EdgeEnds } from './EdgeDraftDialog';
import type { Point } from './modelCanvasGeometry';
import { Inspector, type Edit } from './Inspector';
import { addEdgeType, documentOf, incidentEdgeTypes, modelOf, removeGraphType, sourceLabel, sourceSignature, text, validationTargets, type Section, type Selection, type ValidationTarget } from './model';
import { Query } from './Query';
import { Versions } from './ModelVersions';
import { ModelSetup } from './ModelSetup';
import { setupIssue } from './modelWorkflow';
import { Modal, NameDialog, StatusLabel } from './ui';

export type Workspace = 'graph' | 'catalogs' | 'query';
const GraphCanvas = lazy(() => import('./GraphCanvas'));
const LocalTables = lazy(() => import('./LocalTables'));

export default function Editor({ id, initial, active, onDirty, close, created, changed, workspace, models, goTo, changeModel, allow, actor, mutations, recovered, canvasLayouts }: {
  id: string;
  actor: string;
  allow: { configure: boolean; edit: boolean; publish: boolean; query: boolean; export: boolean };
  mutations: ReturnType<typeof useMutations>;
  recovered?: SavedModel;
  initial?: SavedModel;
  active: boolean;
  onDirty: (id: string, dirty: boolean) => void;
  close: () => void;
  created: (id: string) => void;
  changed: () => void;
  workspace: Workspace;
  models: ModelSummary[];
  goTo: (workspace: Workspace) => void;
  changeModel: (id: string) => void;
  canvasLayouts: Map<string, Map<string, Point>>;
}) {
  const [saved, setSaved] = useState(initial);
  const [yaml, setYaml] = useState(initial?.yaml ?? '');
  const [selection, setSelection] = useState<Selection>();
  const [positions, setPositions] = useState(() => canvasLayouts.get(id) ?? new Map<string, Point>());
  const [connection, setConnection] = useState<CanvasConnection>();
  const [edgeEnds, setEdgeEnds] = useState<EdgeEnds>();
  const [checked, setChecked] = useState<{ yaml: string; result: Validation }>();
  const [locating, setLocating] = useState<ValidationTarget>();
  const [removing, setRemoving] = useState<Selection>();
  const [catalogSelection, setCatalogSelection] = useState<string>();
  const [browsing, setBrowsing] = useState<string>();
  const [localTables, setLocalTables] = useState(false);
  const [modal, setModal] = useState<Section | 'yaml' | 'title' | 'versions' | 'unpublish' | 'nodes-list' | 'edges-list'>();
  const [loading, setLoading] = useState(!initial);
  const [saving, setSaving] = useState(false);
  const [checking, setChecking] = useState(false);
  const [notice, setNotice] = useState<{ text: string; error?: boolean; location?: ValidationSite; yaml?: string }>();
  const [reload, setReload] = useState(0);
  const [activating, setActivating] = useState(false);
  const [tableColumns, setTableColumns] = useState(new Map<string, { signature: string; columns: Column[] }>());
  const inspectorPanel = useRef<HTMLElement>(null);
  const inspectorInvoker = useRef<HTMLElement | null>(null);
  const addNodeButton = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (positions.size) canvasLayouts.set(id, positions);
    else canvasLayouts.delete(id);
  }, [canvasLayouts, id, positions]);

  function refresh() {
    if (dirty && !window.confirm(t('重新加载会丢弃未保存的修改。继续？'))) return;
    setNotice(undefined);
    setTableColumns(new Map());
    setReload(value => value + 1);
  }

  useEffect(() => {
    if (initial) return;
    const controller = new AbortController();
    setLoading(true);
    api<SavedModel>('/models/' + id, { signal: controller.signal }).then(value => {
      setSaved(value);
      setYaml(value.yaml);
      setChecked(undefined);
    }, cause => {
      if (!controller.signal.aborted) setNotice({ text: errorText(cause), error: true });
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [id, initial, reload]);

  useEffect(() => {
    if (!recovered) return;
    setSaved(recovered);
    setYaml(current => saved && current === saved.yaml ? recovered.yaml : current);
    setChecked(undefined);
  // Apply only when a previously interrupted write is resolved.
  }, [recovered]);

  const draft = useMemo(() => {
    try {
      return { model: modelOf(yaml), error: '' };
    } catch (cause) {
      return { model: undefined, error: errorText(cause) };
    }
  }, [yaml]);
  const model = draft.model;
  const incomplete = model && setupIssue(model);
  function getColumns(table: string) {
    const cached = tableColumns.get(table);
    return model && cached && cached.signature === sourceSignature(model, table) ? cached.columns : undefined;
  }
  function rememberColumns({ table, columns }: TableSelection) {
    const signature = model && sourceSignature(model, table);
    if (!signature) return;
    setTableColumns(old => {
      const next = new Map(old);
      next.delete(table);
      next.set(table, { signature, columns });
      if (next.size > 32) next.delete(next.keys().next().value!);
      return next;
    });
  }
  const dirty = Boolean(saved && (!saved.revision || saved.yaml !== yaml));
  const dirtyId = initial ? '' : id;
  useEffect(() => { onDirty(dirtyId, dirty); }, [dirty, dirtyId, onDirty]);
  useEffect(() => () => onDirty(dirtyId, false), [dirtyId, onDirty]);
  const validation = checked?.yaml === yaml ? checked.result : undefined;
  const status = validation?.status ?? (!dirty && saved ? saved.status : 'unchecked');
  const statusText = status === 'passed' && validation ? (validation.check_data ? t('数据校验通过') : t('结构校验通过')) : undefined;

  const selected = model && selection && Object.hasOwn(model[selection.section], selection.name) ? selection : undefined;

  function changeYaml(value: string) {
    setYaml(value);
    setNotice(undefined);
    setChecked(undefined);
    setLocating(undefined);
  }

  const edit: Edit = change => {
    if (!allow.edit) return;
    try {
      const doc = documentOf(yaml);
      change(doc);
      changeYaml(doc.toString());
    } catch (cause) {
      setNotice({ text: errorText(cause), error: true });
    }
  };

  function attachLocal(table: LocalTable, switchMappings: boolean) {
    const sourceName = 'local_' + table.name;
    const reference = sourceName + '.data';
    const existing = model?.sources[sourceName];
    const location = table.reference_path ?? table.path;
    if (existing && (existing.type !== 'parquet' || ![location, table.path].some(path =>
      JSON.stringify(existing.tables) === JSON.stringify({ data: [path] })))) {
      throw new Error(t('数据源名称 {0} 已被占用，请先重命名该数据源。', sourceName));
    }
    edit(doc => {
      doc.setIn(['sources', sourceName], { type: 'parquet', tables: { data: [location] } });
      if (switchMappings && model) for (const section of ['nodes', 'edges'] as const) {
        for (const [name, mapping] of Object.entries(model[section])) {
          if (mapping.source === table.source_table) doc.setIn([section, name, 'source'], reference);
          if (Array.isArray(mapping.inputs)) mapping.inputs.forEach((input, index) => {
            if (input.table === table.source_table) doc.setIn([section, name, 'inputs', index, 'table'], reference);
          });
        }
      }
    });
  }

  async function save() {
    if (!saved || saving || !allow.edit) return;
    if (incomplete) { setNotice({ text: incomplete.message, error: true, yaml }); return; }
    setSaving(true);
    setNotice(undefined);
    try {
      const first = !saved.revision;
      const response = await mutations.run({
        model: id, label: '保存草稿', path: first ? '/models' : '/models/' + id,
        method: first ? 'POST' : 'PUT',
        expected: first ? undefined : '"' + saved.revision + '"',
        body: JSON.stringify(first ? { id, yaml } : { yaml }),
      });
      setSaved(response);
      setYaml(current => current === yaml ? response.yaml : current);
      setNotice({ text: t('草稿已保存') });
      changed();
      if (first) created(id);
    } catch (cause) {
      setNotice({ text: errorText(cause), error: true, location: cause instanceof ApiError ? cause.location : undefined, yaml });
    } finally { setSaving(false); }
  }

  async function validate(checkData = false) {
    if (checking || !allow.edit) return;
    setChecking(true);
    setNotice(undefined);
    try {
      const result = await api<Validation>('/models/validate', {
        method: 'POST', body: JSON.stringify({ id, yaml, check_data: checkData }),
      });
      setChecked({ yaml, result });
      changed();
    } catch (cause) {
      setNotice({ text: errorText(cause), error: true, yaml });
    } finally { setChecking(false); }
  }

  async function activate(revision: string | undefined, action: 'publish' | 'rollback' | 'unpublish' | 'restore', backup?: string) {
    if (!saved || activating || !allow.publish) return;
    setActivating(true);
    setNotice(undefined);
    try {
      const response = await mutations.run({
        model: id, label: { publish: '发布模型', rollback: '回退模型', unpublish: '下线模型', restore: '恢复归档' }[action],
        path: '/models/' + id + (action === 'restore' ? '/history/' + revision : '/' + action),
        method: action === 'restore' ? 'PUT' : 'POST',
        body: JSON.stringify(action === 'restore' ? { yaml: backup } : { revision, active_revision: saved.active_revision }),
      });
      // Version switches leave the working draft intact, including unsaved edits.
      if (saved.revision !== response.revision) setChecked(undefined);
      setSaved(response);
      if (!dirty) setYaml(current => current === yaml ? response.yaml : current);
      setModal(undefined);
      changed();
      setNotice({ text: action === 'unpublish' ? t('模型已下线，草稿和历史版本已保留')
        : action === 'restore' ? t('已恢复版本归档 {0}', revision?.slice(0, 10))
        : (action === 'publish' ? t('已发布版本 ') : t('已回退至版本 ')) + revision?.slice(0, 10) });
    } catch (cause) {
      if (action === 'restore') throw cause;
      setModal(undefined);
      setNotice({ text: errorText(cause), error: true, location: action === 'publish' && cause instanceof ApiError ? cause.location : undefined, yaml });
      if (action === 'publish') changed();
    } finally { setActivating(false); }
  }

  function openSource() {
    setModal('sources');
  }

  function add(section: 'sources' | 'nodes', name: string, type: string, config?: Record<string, unknown>) {
    if (!model) return;
    if (section === 'sources' && (!allow.configure || name.includes('.') || name.includes('\0'))) throw new Error(t('请输入不含点的数据源名称'));
    const exists = (key: Section) => Object.hasOwn(model[key], name);
    if (exists(section) || section !== 'sources' && (exists('nodes') || exists('edges'))) {
      throw new Error(t('名称已存在'));
    }
    edit(doc => {
      if (section === 'sources') doc.setIn([section, name], config ?? newConnector(type));
      else doc.setIn([section, name], { source: '', key: [''], properties: [] });
    });
    if (section === 'sources' && workspace === 'catalogs') setCatalogSelection(name);
    else setSelection({ section, name });
  }

  const listSection = modal === 'nodes-list' ? 'nodes' : modal === 'edges-list' ? 'edges' : undefined;
  const title = { graph: t('图模型'), catalogs: t('数据源'), query: t('查询') }[workspace];
  const busy = mutations.pending || saving || checking || activating;
  const nodeNames = Object.keys(model?.nodes ?? {});
  const canConnect = active && allow.edit && !busy && workspace === 'graph' && !modal && !!nodeNames.length;
  const connecting = canConnect ? connection : undefined;
  useEffect(() => {
    if (!canConnect || connection?.from !== undefined && !Object.hasOwn(model?.nodes ?? {}, connection.from)) {
      setConnection(undefined);
    }
  }, [canConnect, connection, model]);
  useEffect(() => {
    if (!connecting) return;
    function cancel(event: KeyboardEvent) {
      if (event.key !== 'Escape' || document.querySelector('dialog[open]')) return;
      event.preventDefault();
      setConnection(undefined);
    }
    window.addEventListener('keydown', cancel);
    return () => window.removeEventListener('keydown', cancel);
  }, [connecting]);

  function pickEndpoint(name: string) {
    if (!connecting) return;
    if (connecting.from === undefined) { setConnection({ from: name }); return; }
    inspectorInvoker.current = document.activeElement as HTMLElement;
    setEdgeEnds({ from: connecting.from, to: name });
    setConnection(undefined);
    setModal('edges');
  }

  function createEdge(name: string, { from, to }: EdgeEnds) {
    if (!model || !allow.edit || busy) throw new Error(t('当前无法修改草稿'));
    const doc = documentOf(yaml);
    addEdgeType(doc, model, name, from, to);
    changeYaml(doc.toString());
    const target = { section: 'edges', name } as const;
    setSelection(target);
    setLocating({ selection: target, control: 'mapping', caption: t('数据映射') });
    setNotice({ text: t('已在草稿中创建 {0}。请补齐来源表与端点字段。', name) });
  }

  function moveNode(name: string, point: Point) {
    setPositions(previous => new Map(previous).set(name, point));
  }

  function removeNodePosition(name: string, renameTo?: string) {
    setPositions(previous => {
      const position = previous.get(name);
      if (!position) return previous;
      const next = new Map(previous);
      next.delete(name);
      if (renameTo !== undefined) next.set(renameTo, position);
      return next;
    });
  }
  const canPublish = allow.publish && Boolean(saved?.revision && !dirty && !busy && saved.revision !== saved.active_revision);
  const inspectorSelection: Selection | undefined = workspace === 'catalogs'
    ? model && catalogSelection && Object.hasOwn(model.sources, catalogSelection) ? { section: 'sources', name: catalogSelection } : undefined
    : selected;
  const inspectorKey = inspectorSelection && inspectorSelection.section + ':' + inspectorSelection.name;
  useEffect(() => {
    const panel = inspectorPanel.current;
    if (inspectorKey && panel && panel.getBoundingClientRect().top >= window.innerHeight * .7) {
      panel.scrollIntoView({ block: 'start' });
    }
  }, [inspectorKey, workspace]);
  useEffect(() => {
    if (!locating || workspace !== 'graph' || !inspectorPanel.current) return;
    const controls = inspectorPanel.current.querySelectorAll<HTMLElement>('[data-mapping-control]');
    const control = [...controls].find(element => element.dataset.mappingControl === locating.control);
    if (control) {
      for (let parent = control.parentElement; parent && parent !== inspectorPanel.current; parent = parent.parentElement) {
        if (parent instanceof HTMLDetailsElement) parent.open = true;
      }
    }
    const target = control ?? inspectorPanel.current.querySelector<HTMLElement>('[data-mapping-control]');
    target?.focus({ preventScroll: true });
    target?.scrollIntoView({ block: 'nearest' });
    setLocating(undefined);
  }, [locating, workspace, inspectorKey]);
  const currentNotice = notice?.yaml === undefined || notice.yaml === yaml ? notice : undefined;
  const issue = currentNotice?.location ?? validation?.location;
  const targets = model && issue ? validationTargets(model, issue) : [];
  const invalid = new Set(targets.filter(target => target.selection.section === inspectorSelection?.section
    && target.selection.name === inspectorSelection.name).map(target => target.control));
  function locate(target: ValidationTarget) {
    inspectorInvoker.current = document.activeElement as HTMLElement;
    setSelection(target.selection);
    setLocating(target);
    if (workspace !== 'graph') goTo('graph');
  }
  const errorMessage = currentNotice?.location ? currentNotice.text : validation?.error;
  const dependencies = removing?.section === 'nodes' && model ? incidentEdgeTypes(model, removing.name) : [];
  const notices = <>
    {currentNotice && !currentNotice.location && <div className={'notice editor-notice' + (currentNotice.error ? ' error' : '')} role={currentNotice.error ? 'alert' : 'status'}>
      {currentNotice.text}
      {currentNotice.error && saved?.revision && <button className="text-button" onClick={refresh}>{t('重新加载')}</button>}
    </div>}
    {(currentNotice?.location || validation?.error) && <div className="notice error editor-notice">
      <span id={id + '-model-validation-error'} role="alert">{diagnosticText(errorMessage ?? '')}</span>
      <div className="validation-actions">
        {targets.map(target => <button className="text-button" key={target.selection.name + target.control}
          onClick={() => locate(target)}>{t('定位 {0}', target.caption)}</button>)}
        {!targets.length && <button className="text-button" onClick={() => setModal('yaml')}>{t('查看 YAML')}</button>}
      </div>
    </div>}
  </>;
  const inspector = model && inspectorSelection && <aside className="inspector" ref={inspectorPanel} aria-label={t('配置详情')}>
    {workspace !== 'catalogs' && <button className="icon-button inspector-close" aria-label={t('关闭配置详情')} onClick={() => {
      setSelection(undefined);
      inspectorInvoker.current?.focus();
    }}><X size={18} /></button>}
    <fieldset className="inspector-fields" disabled={!allow.edit || inspectorSelection.section === 'sources' && !allow.configure || saving || activating || checking}>
      <Inspector key={inspectorKey} model={model} selected={inspectorSelection}
        edit={edit} select={next => {
          if (next.section === 'sources' && workspace === 'catalogs') setCatalogSelection(next.name);
          else {
            if (next.section === 'nodes' && inspectorSelection.section === 'nodes') removeNodePosition(inspectorSelection.name, next.name);
            setSelection(next);
          }
        }} id={id} yaml={yaml} openYaml={() => setModal('yaml')}
        getColumns={getColumns} rememberColumns={rememberColumns}
        invalid={invalid} onRemove={() => setRemoving(inspectorSelection)}
        columns={inspectorSelection.section !== 'sources' ? validation?.labels[inspectorSelection.name] : undefined} />
    </fieldset>
  </aside>;

  return <main className={'workbench editor workspace-' + workspace}>
    <header className="workspace-header model-workspace-header">
      <div className="workspace-title">
        <button className="icon-button" aria-label={t('返回模型列表')} title={t('返回模型列表')} onClick={close}><ArrowLeft size={17} aria-hidden="true" /></button>
        <h1>{title}</h1>
        {workspace === 'catalogs' && model && allow.configure && allow.edit && <button className="button primary compact catalog-add" type="button"
          disabled={!!busy} aria-haspopup="dialog" aria-label={t('添加数据源')} onClick={openSource}>
          <Plus size={16} aria-hidden="true" /><span className="label-full">{t('添加数据源')}</span><span className="label-compact">{t('添加来源')}</span>
        </button>}
      </div>
      <div className="workspace-context">
        {workspace !== 'query' && dirty && <span className="dirty"><i />{t('未保存')}</span>}
        {saved?.revision && <label className="model-switch"><span className="sr-only">{t('切换图模型')}</span>
          <select value={id} onChange={event => changeModel(event.target.value)}>
            <option value={id}>{model?.graph || id}</option>
            {models.filter(other => other.id !== id && (workspace !== 'query' || other.active_revision && !other.archive_error)).map(other =>
              <option value={other.id} key={other.id}>{other.name} · {other.id}</option>)}
          </select>
        </label>}
        {!saved?.revision && model?.graph && <strong className="draft-name">{model.graph}</strong>}
        {workspace === 'graph' && model && allow.edit && <button className="icon-button" aria-label={t('编辑模型名称')} onClick={() => setModal('title')}><Pencil size={15} /></button>}
        {saved && (workspace !== 'query' || !saved.active_revision || saved.archive_error) && <code className="version-chip" title={saved.active_revision ?? undefined}>{saved.archive_error ? t('版本存储异常') : saved.active_revision ? t('生效 {0}', saved.active_revision.slice(0, 10)) : t('未发布')}</code>}
      </div>
    </header>
    {saved?.archive_error && <p className="notice error" role="alert">{diagnosticText(saved.archive_error)}{' '}
      <button className="button compact" onClick={() => setModal('versions')}>{t('查看版本历史')}</button>
    </p>}
    {workspace !== 'graph' && !(workspace === 'catalogs' && inspectorSelection) && notices}
    {workspace === 'catalogs' && model && saved && <div className="catalog-workspace">
      <div className="catalog-controls">
        {allow.configure && <button className="button compact" disabled={!saved.revision} onClick={() => setLocalTables(true)}>{t('本地表与同步')}</button>}
        <button className="button compact" disabled={!dirty || busy || !!incomplete} title={incomplete?.message} onClick={() => void save()}>{saving ? t('保存中…') : t('保存草稿')}</button>
        <button className="button primary compact" disabled={!canPublish} onClick={() => saved.revision && void activate(saved.revision, 'publish')}>{t('校验并发布')}</button>
        {status !== 'unchecked' && (!validation?.error || status === 'passed') && <span role="status"><StatusLabel status={status} text={statusText} /></span>}
        <details className="workspace-tools"><summary>{t('更多工具')}</summary><div>
          <button className="button compact" disabled={busy} onClick={() => void validate()}>{checking ? t('正在校验…') : t('校验结构')}</button>
          <button className="button compact" disabled={busy} onClick={() => void validate(true)}>{t('校验数据')}</button>
          <button className="button compact" onClick={() => setModal('yaml')}><Braces size={15} />{t('查看 YAML')}</button>
          <button className="button compact" disabled={busy || !saved.revision} onClick={refresh}><RefreshCw size={15} />{t('重新加载')}</button>
        </div></details>
      </div>
      <Catalogs model={model} selected={catalogSelection} configure={name => {
        inspectorInvoker.current = document.activeElement as HTMLElement;
        setCatalogSelection(name);
      }} browse={setBrowsing} />
    </div>}
    {allow.query && saved?.active_revision && !saved.archive_error && <div className="query-shell" hidden={workspace !== 'query'}>
      <Query canExport={allow.export} id={id} actor={actor} revision={saved.active_revision} draftPending={dirty || saved.revision !== saved.active_revision}
        refresh={refresh} editSchema={allow.edit ? () => goTo('graph') : undefined} visible={active && workspace === 'query'} />
    </div>}
    {workspace === 'query' && saved && !saved.active_revision && !saved.archive_error && <div className="empty">
      <p>{t('发布模型后即可查询。')}</p><button className="button" onClick={() => goTo('graph')}>{t('返回图模型')}</button>
    </div>}
    {loading ? <p className="empty" role="status">{t('正在读取配置…')}</p>
      : model && saved && workspace === 'graph' ? <div className="graph-workspace">
        {!saved.active_revision && allow.edit && <ModelSetup configure={allow.configure} publishAllowed={allow.publish} model={model} issue={incomplete} dirty={dirty} busy={!!busy} canPublish={canPublish}
          add={section => section === 'sources' ? openSource() : setModal(section)} select={setSelection} save={() => void save()} publish={() => void activate(saved.revision, 'publish')} />}
        {saved.active_revision && incomplete && <p className="notice">{incomplete.message}</p>}
        <div className="graph-command-bar">
          <div className="graph-toolbar" role="group" aria-label={t('建模工具栏')}>
            {allow.publish && (saved.active_revision || !allow.edit) && <button className="tool-button tool-primary" disabled={!canPublish} onClick={() => void activate(saved.revision, 'publish')}>
              <Upload size={20} aria-hidden="true" />{activating ? t('正在切换…') : t('校验并发布')}
            </button>}
            {allow.edit && saved.active_revision && <div className="tool-group">
              <button className="tool-button" disabled={!dirty || busy || !!incomplete} title={incomplete?.message} onClick={() => void save()}><Save size={20} />{saving ? t('保存中…') : t('保存草稿')}</button>
            </div>}
            {allow.edit && <div className="tool-group">
              <button ref={addNodeButton} className="tool-button" disabled={busy} onClick={() => setModal('nodes')}><Plus size={20} />{t('添加节点')}</button>
              <button type="button" className="tool-button" disabled={!canConnect} aria-pressed={!!connecting}
                onClick={() => setConnection(connecting ? undefined : {})}><Waypoints size={20} aria-hidden="true" />{t('连线建边')}</button>
            </div>}
            <div className="tool-group">
              {allow.configure && <button className="tool-button" disabled={!saved.revision} onClick={() => setLocalTables(true)}><Database size={20} />{t('本地表')}</button>}
              <button className="tool-button" onClick={() => setModal('nodes-list')}><List size={20} />{t('节点 ')}<span className="tool-count">{Object.keys(model.nodes).length}</span></button>
              <button className="tool-button" onClick={() => setModal('edges-list')}><List size={20} />{t('边 ')}<span className="tool-count">{Object.keys(model.edges).length}</span></button>
            </div>
            <details className="workspace-tools"><summary>{t('更多工具')}</summary><div>
              {allow.configure && <button className="button compact" onClick={() => initial ? openSource() : goTo('catalogs')}>
                <Database size={16} />{initial ? t('添加数据源') : t('数据源')}
              </button>}
              {allow.edit && <button className="button compact" disabled={busy} onClick={() => void validate()}><ShieldCheck size={16} />{checking ? t('正在校验…') : t('校验结构')}</button>}
              {allow.edit && <button className="button compact" disabled={busy} onClick={() => void validate(true)}><ShieldCheck size={16} />{t('校验数据')}</button>}
              <button className="button compact" disabled={!saved.revision || activating} onClick={() => setModal('versions')}><History size={16} />{t('版本历史')}</button>
              <button className="button compact" onClick={() => setModal('yaml')}><Braces size={16} />{t('查看 YAML')}</button>
            </div></details>
          </div>
          {status !== 'unchecked' && (!validation?.error || status === 'passed') && <span className="draft-status graph-status" role="status"><StatusLabel status={status} text={statusText} /></span>}
        </div>
        {notices}
        <div className="graph-body">
          <Suspense fallback={<div className="canvas-panel"><p className="empty" role="status">{t('正在打开画布…')}</p></div>}>
          <GraphCanvas model={model} selected={selected} select={next => {
            inspectorInvoker.current = document.activeElement as HTMLElement;
            setSelection(next);
          }} editing={allow.edit ? {
            enabled: canConnect,
            positions,
            move: moveNode,
            arrange: () => setPositions(new Map()),
            connection: connecting,
            start: from => setConnection({ from }),
            pick: pickEndpoint,
            cancel: () => setConnection(undefined),
          } : undefined} />
          </Suspense>
          {inspector}
        </div>
      </div> : !saved ? <div className="empty" role="alert">{workspace === 'graph' && notice?.text}<button className="button" onClick={refresh}>{t('重试读取配置')}</button></div>
      : !model && workspace !== 'query' && <div className="empty" role="alert">{draft.error}<button className="button" onClick={() => setModal('yaml')}>{t('编辑 YAML')}</button></div>}
    {active && <>{workspace === 'catalogs' && inspectorSelection && <Modal title={t('配置数据源')} size="medium" close={() => setCatalogSelection(undefined)}>
      <div className="panel-layout source-settings">
        <div className="panel-body form-fields">{inspector}{notices}</div>
        <footer className="panel-footer">
          <p className="panel-footer-context" role="status">{dirty ? t('当前模型有未保存的修改。关闭后仍保留在草稿中。')
            : saved?.revision !== saved?.active_revision ? t('草稿已保存，发布后生效。') : t('当前配置已保存并发布。')}</p>
          <button className="button" onClick={() => setCatalogSelection(undefined)}>{t('关闭配置')}</button>
          <button className="button primary" disabled={!dirty || busy || !allow.edit || !allow.configure} onClick={() => void save()}>{saving ? t('保存中…') : t('保存草稿')}</button>
        </footer>
      </div>
    </Modal>}
    {listSection && model && <Modal title={listSection === 'nodes' ? t('节点标签') : t('边类型')} description={t('选择标签，返回画布继续配置映射。')} close={() => setModal(undefined)}>
      <div className="panel-layout"><div className="panel-body form-fields">
        {!Object.keys(model[listSection]).length && <p className="muted">{t('暂无{0}', listSection === 'nodes' ? t('节点标签') : t('边类型'))}</p>}
        {Object.keys(model[listSection]).map(name => <button className="sidebar-item" key={name} onClick={() => {
          setSelection({ section: listSection, name }); setModal(undefined);
        }}>{listSection === 'nodes' ? <Box size={17} /> : <GitCommitHorizontal size={17} />}<code>{name}</code></button>)}
        </div>
        {allow.edit && <footer className="panel-footer"><button className="button primary" disabled={busy || listSection === 'edges' && !nodeNames.length}
          onClick={() => { setEdgeEnds(undefined); setModal(listSection); }}><Plus size={16} />{t('添加{0}', listSection === 'nodes' ? t('节点') : t('边'))}</button></footer>}
      </div>
    </Modal>}
    {localTables && saved && active && allow.configure && <Suspense fallback={<p role="status">{t('正在打开本地表…')}</p>}>
      <LocalTables saved={saved} clean={!dirty} close={() => setLocalTables(false)} attach={attachLocal} />
    </Suspense>}
    {browsing && model && <SourceBrowser id={id} yaml={yaml} model={model} fixedSource={browsing} close={() => setBrowsing(undefined)} />}
    {removing && model && <Modal layout="center" title={{ sources: t('删除数据源'), nodes: t('删除节点标签'), edges: t('删除边类型') }[removing.section]} close={() => setRemoving(undefined)}>
      <div className="dialog-form">
        <p>{t('从草稿中删除 ')}<code>{removing.name}</code>{t('？生效版本会在发布后更新。')}</p>
        {!!dependencies.length && <div>{t('以下边类型也会从草稿中删除：')}
          <ul>{dependencies.map(edge => <li key={edge}><code>{edge}</code></li>)}</ul>
        </div>}
        <div className="dialog-actions">
          <button className="button" data-initial-focus onClick={() => setRemoving(undefined)}>{t('取消')}</button>
          <button className="button danger" onClick={() => {
            if (removing.section === 'sources' && !allow.configure) return;
            const { section, name } = removing;
            edit(doc => removeGraphType(doc, model, section, name));
            if (section === 'nodes') removeNodePosition(name);
            setRemoving(undefined);
            if (section === 'sources' && workspace === 'catalogs') {
              setCatalogSelection(undefined);
              requestAnimationFrame(() => inspectorInvoker.current?.focus());
            } else {
              setSelection(undefined);
              requestAnimationFrame(() => addNodeButton.current?.focus());
            }
          }}>{t('确认删除')}</button>
        </div>
      </div>
    </Modal>}
    {modal === 'versions' && saved && <Versions key={id + ':' + saved.active_revision} id={id} yaml={yaml} busy={activating || mutations.pending} close={() => setModal(undefined)}
      archiveError={saved.archive_error}
      restore={allow.publish ? (revision, backup) => activate(revision, 'restore', backup) : undefined}
      offline={allow.publish && saved.active_revision ? () => setModal('unpublish') : undefined}
      rollback={allow.publish ? revision => void activate(revision, 'rollback') : undefined} />}
    {modal === 'unpublish' && <Modal layout="center" title={t('下线模型')} close={() => setModal(undefined)}>
      <div className="dialog-form">
        <p>{t('下线后将拒绝新的查询、导出和邻居展开。草稿和历史版本会保留，可以重新发布。')}</p>
        <div className="dialog-actions">
          <button type="button" className="button danger" disabled={activating}
            onClick={() => void activate(undefined, 'unpublish')}>{t('确认下线')}</button>
          <button type="button" className="button" data-initial-focus onClick={() => setModal('versions')}>{t('取消')}</button>
        </div>
      </div>
    </Modal>}
    {modal === 'yaml'  && <Modal title={t('模型 YAML')} size="wide" description={allow.edit ? t('编辑内容保留在当前草稿中，完成后可保存、校验并发布。') : t('查看当前模型配置。')} close={() => setModal(undefined)}>
      <div className="panel-layout yaml-editor"><div className="panel-body">
        <label><span className="sr-only">{t('YAML 配置')}</span><textarea value={yaml} readOnly={!allow.edit} onChange={event => changeYaml(event.target.value)}
          spellCheck={false} className="mono" rows={22} data-initial-focus /></label>
        {draft.error && <p className="inline-error" role="alert">{draft.error}</p>}
        </div><footer className="panel-footer">
          {allow.edit && <label className="button">{t('导入 YAML 到草稿')}<input className="sr-only" type="file" accept=".yaml,.yml" disabled={busy} onChange={async event => {
            const file = event.target.files?.[0]; event.target.value = '';
            if (!file) return;
            try {
              if (file.size > 1024 * 1024) throw new Error(t('YAML 文件超过 1 MiB'));
              const next = await file.text();
              modelOf(next);
              if (yaml !== next && window.confirm(t('将当前编辑内容替换为导入的 YAML？'))) changeYaml(next);
            } catch (cause) { setNotice({ text: errorText(cause), error: true }); setModal(undefined); }
          }} /></label>}
          <button className="button" onClick={() => downloadText(yaml, id + '.yaml')}>{t('下载草稿 YAML')}</button>
          <button className="button primary" onClick={() => setModal(undefined)}>{t('完成')}</button>
        </footer>
      </div>
    </Modal>}
    {modal === 'title' && <NameDialog title={t('编辑模型名称')} initial={text(model?.graph)} close={() => setModal(undefined)}
      submit={name => edit(doc => doc.set('graph', name))} />}
    {modal === 'edges' && model && <EdgeDraftDialog model={model} ends={edgeEnds} create={createEdge}
      close={() => { setEdgeEnds(undefined); setModal(undefined); }} />}
    {modal === 'sources' && <AddSource modelId={id} close={() => setModal(undefined)}
      submit={(name, source) => add('sources', name, text(source.type), source)} />}
    {modal === 'nodes' && <NameDialog title={t('添加节点')} close={() => setModal(undefined)}
      action={t('添加')} submit={(name, type) => add('nodes', name, type)} />}</>}
  </main>;
}
