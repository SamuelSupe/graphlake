import { useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, Braces, Box, Database, GitCommitHorizontal, History, List, Pencil, Plus, RefreshCw, Save, ShieldCheck, Upload, X } from 'lucide-react';
import { api, ApiError, downloadText, errorText, type Column, type ModelSummary, type SavedModel, type Validation, type ValidationSite, type VersionHistory } from './api';
import { Catalogs } from './Catalogs';
import { SourceBrowser, type TableSelection } from './SourceBrowser';
import { GraphCanvas } from './GraphCanvas';
import { Inspector, type Edit } from './Inspector';
import { documentOf, incidentEdgeTypes, modelOf, removeGraphType, sourceLabel, sourceSignature, text, validationTargets, type Section, type Selection, type ValidationTarget } from './model';
import { Query } from './Query';
import { Modal, NameDialog, StatusLabel } from './ui';

export type Workspace = 'graph' | 'catalogs' | 'query';

const sections = [
  { key: 'sources', label: '数据源', Icon: Database },
  { key: 'nodes', label: '节点标签', Icon: Box },
  { key: 'edges', label: '边类型', Icon: GitCommitHorizontal },
] as const;

export function Editor({ id, initial, active, onDirty, close, created, changed, workspace, models, goTo, changeModel }: {
  id: string;
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
}) {
  const [saved, setSaved] = useState(initial);
  const [yaml, setYaml] = useState(initial?.yaml ?? '');
  const [selection, setSelection] = useState<Selection>();
  const [checked, setChecked] = useState<{ yaml: string; result: Validation }>();
  const [locating, setLocating] = useState<ValidationTarget>();
  const [removing, setRemoving] = useState<Selection>();
  const [catalogSelection, setCatalogSelection] = useState<string>();
  const [browsing, setBrowsing] = useState<string>();
  const [modal, setModal] = useState<Section | 'yaml' | 'title' | 'versions' | 'nodes-list' | 'edges-list'>();
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

  function refresh() {
    if (dirty && !window.confirm('重新加载会丢弃未保存的修改。继续？')) return;
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

  const draft = useMemo(() => {
    try {
      return { model: modelOf(yaml), error: '' };
    } catch (cause) {
      return { model: undefined, error: errorText(cause) };
    }
  }, [yaml]);
  const model = draft.model;
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

  const selected = model && selection && Object.hasOwn(model[selection.section], selection.name) ? selection : undefined;

  function changeYaml(value: string) {
    setYaml(value);
    setNotice(undefined);
    setChecked(undefined);
    setLocating(undefined);
  }

  const edit: Edit = change => {
    try {
      const doc = documentOf(yaml);
      change(doc);
      changeYaml(doc.toString());
    } catch (cause) {
      setNotice({ text: errorText(cause), error: true });
    }
  };

  async function save() {
    if (!saved || saving) return;
    setSaving(true);
    setNotice(undefined);
    try {
      const first = !saved.revision;
      const response = await api<SavedModel>(first ? '/models' : '/models/' + id, {
        method: first ? 'POST' : 'PUT',
        headers: first ? {} : { 'If-Match': '"' + saved.revision + '"' },
        body: JSON.stringify(first ? { id, yaml } : { yaml }),
      });
      setSaved(response);
      setYaml(current => current === yaml ? response.yaml : current);
      setNotice({ text: '草稿已保存' });
      changed();
      if (first) created(id);
    } catch (cause) {
      setNotice({ text: errorText(cause), error: true, location: cause instanceof ApiError ? cause.location : undefined, yaml });
    } finally { setSaving(false); }
  }

  async function validate(checkData = false) {
    if (checking) return;
    setChecking(true);
    setNotice(undefined);
    try {
      const result = await api<Validation>('/models/validate', {
        method: 'POST', body: JSON.stringify({ id, yaml, check_data: checkData }),
      });
      setChecked({ yaml, result });
      if (result.status === 'passed') setNotice({ text: checkData ? '数据校验通过，业务键和边端点有效' : '结构校验通过，映射字段与端点类型一致', yaml });
      changed();
    } catch (cause) {
      setNotice({ text: errorText(cause), error: true, yaml });
    } finally { setChecking(false); }
  }

  async function activate(revision: string, action: 'publish' | 'rollback') {
    if (!saved || activating) return;
    setActivating(true);
    setNotice(undefined);
    try {
      const response = await api<SavedModel>('/models/' + id + '/' + action, {
        method: 'POST', body: JSON.stringify({ revision, active_revision: saved.active_revision }),
      });
      // Rollback leaves the working draft intact, including unsaved edits.
      if (saved.revision !== response.revision) setChecked(undefined);
      setSaved(response);
      if (!dirty) setYaml(current => current === yaml ? response.yaml : current);
      setModal(undefined);
      changed();
      setNotice({ text: (action === 'publish' ? '已发布版本 ' : '已回退至版本 ') + revision.slice(0, 10) });
    } catch (cause) {
      setModal(undefined);
      setNotice({ text: errorText(cause), error: true, location: action === 'publish' && cause instanceof ApiError ? cause.location : undefined, yaml });
      if (action === 'publish') changed();
    } finally { setActivating(false); }
  }

  function add(section: Section, name: string, type: string) {
    if (!model) return;
    const exists = (key: Section) => Object.hasOwn(model[key], name);
    if (exists(section) || section !== 'sources' && (exists('nodes') || exists('edges'))) {
      throw new Error('名称已存在');
    }
    edit(doc => {
      if (section === 'sources') doc.setIn([section, name], type === 'postgres' ? { type, dsn_env: '' } : { type, tables: {} });
      else if (section === 'nodes') doc.setIn([section, name], { source: '', key: [''], properties: [] });
      else doc.setIn([section, name], { source: '', key: [], from: { node: '', columns: [] }, to: { node: '', columns: [] }, properties: [] });
    });
    if (section === 'sources' && workspace === 'catalogs') setCatalogSelection(name);
    else setSelection({ section, name });
  }

  const addSection = sections.find(section => section.key === modal);
  const listSection = modal === 'nodes-list' ? 'nodes' : modal === 'edges-list' ? 'edges' : undefined;
  const title = { graph: '图模型', catalogs: '数据源', query: '查询' }[workspace];
  const busy = saving || checking || activating;
  const canPublish = Boolean(saved?.revision && !dirty && !busy && saved.revision !== saved.active_revision);
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
      {currentNotice.error && saved?.revision && <button className="text-button" onClick={refresh}>重新加载</button>}
    </div>}
    {(currentNotice?.location || validation?.error) && <div className="notice error editor-notice">
      <span id={id + '-model-validation-error'} role="alert">{errorMessage}</span>
      <div className="validation-actions">
        {targets.map(target => <button className="text-button" key={target.selection.name + target.control}
          onClick={() => locate(target)}>定位 {target.caption}</button>)}
        {!targets.length && <button className="text-button" onClick={() => setModal('yaml')}>查看 YAML</button>}
      </div>
    </div>}
  </>;
  const inspector = model && inspectorSelection && <aside className="inspector" ref={inspectorPanel} aria-label="配置详情">
    <button className="icon-button inspector-close" aria-label="关闭配置详情" onClick={() => {
      if (workspace === 'catalogs') setCatalogSelection(undefined);
      else setSelection(undefined);
      inspectorInvoker.current?.focus();
    }}><X size={18} /></button>
    <fieldset className="inspector-fields" disabled={saving || activating || checking}>
      <Inspector key={inspectorKey} model={model} selected={inspectorSelection}
        edit={edit} select={setSelection} id={id} yaml={yaml} openYaml={() => setModal('yaml')}
        getColumns={getColumns} rememberColumns={rememberColumns}
        invalid={invalid} onRemove={() => setRemoving(inspectorSelection)}
        columns={inspectorSelection.section !== 'sources' ? validation?.labels[inspectorSelection.name] : undefined} />
    </fieldset>
  </aside>;

  return <main className={'workbench editor workspace-' + workspace}>
    <header className="workspace-header">
      <div className="workspace-title">
        {workspace === 'graph' && <button className="icon-button" aria-label="返回图模型列表" title="返回图模型列表" onClick={close}><ArrowLeft size={17} /></button>}
        <h1>{title}</h1>
        {workspace === 'catalogs' && model && <>
          <button className="button compact" disabled={busy || !saved?.revision} onClick={refresh}><RefreshCw size={15} />刷新</button>
          <button className="button primary compact" disabled={busy} onClick={() => setModal('sources')}><Plus size={16} />添加数据源</button>
        </>}
      </div>
      <div className="workspace-context">
        {workspace !== 'query' && dirty && <span className="dirty"><i />未保存</span>}
        {saved?.revision && <label className="model-switch"><span className="sr-only">切换图模型</span>
          <select value={id} onChange={event => changeModel(event.target.value)}>
            <option value={id}>{model?.graph || id}</option>
            {models.filter(other => other.id !== id && (workspace !== 'query' || other.active_revision)).map(other =>
              <option value={other.id} key={other.id}>{other.name} · {other.id}</option>)}
          </select>
        </label>}
        {!saved?.revision && model?.graph && <strong className="draft-name">{model.graph}</strong>}
        {workspace === 'graph' && model && <button className="icon-button" aria-label="编辑模型名称" onClick={() => setModal('title')}><Pencil size={15} /></button>}
        {saved && <code className="version-chip" title={saved.active_revision ?? undefined}>{saved.active_revision ? '生效 ' + saved.active_revision.slice(0, 10) : '未发布'}</code>}
      </div>
    </header>
    {workspace !== 'graph' && notices}
    {workspace === 'catalogs' && model && saved && <div className="catalog-workspace">
      <div className="catalog-controls">
        <button className="button compact" onClick={() => setModal('yaml')}><Braces size={15} />查看 YAML</button>
        <button className="button compact" disabled={busy} onClick={() => void validate()}>{checking ? '正在校验…' : '校验结构'}</button>
        <button className="button compact" disabled={busy} onClick={() => void validate(true)}>校验数据</button>
        <button className="button compact" disabled={!dirty || busy} onClick={() => void save()}>{saving ? '保存中…' : '保存草稿'}</button>
        <button className="button primary compact" disabled={!canPublish} onClick={() => saved.revision && void activate(saved.revision, 'publish')}>校验并发布</button>
        <span className="draft-status">草稿{validation && (validation.check_data ? ' · 数据' : ' · 结构')} · <StatusLabel status={status} /></span>
      </div>
      <div className="catalog-content">
        <Catalogs model={model} selected={catalogSelection} configure={name => {
          inspectorInvoker.current = document.activeElement as HTMLElement;
          setCatalogSelection(name);
        }} browse={setBrowsing} add={() => setModal('sources')} />
        {inspector}
      </div>
    </div>}
    {saved?.active_revision && <div className="query-shell" hidden={workspace !== 'query'}>
      <Query id={id} revision={saved.active_revision} draftPending={dirty || saved.revision !== saved.active_revision}
        refresh={refresh} editSchema={() => goTo('graph')} visible={active && workspace === 'query'} />
    </div>}
    {workspace === 'query' && saved && !saved.active_revision && <div className="empty">
      <p>发布模型后即可查询。</p><button className="button" onClick={() => goTo('graph')}>返回图模型</button>
    </div>}
    {loading ? <p className="empty" role="status">正在读取配置…</p>
      : model && saved && workspace === 'graph' ? <div className="graph-workspace">
        <div className="graph-command-bar">
          <div className="graph-toolbar" role="group" aria-label="建模工具栏">
            <button className="tool-button tool-primary" disabled={!canPublish} onClick={() => void activate(saved.revision, 'publish')}>
              <Upload size={20} aria-hidden="true" />{activating ? '正在切换…' : '校验并发布'}
            </button>
            <div className="tool-group">
              <button className="tool-button" onClick={() => setModal('yaml')}><Braces size={20} />查看 YAML</button>
              <button className="tool-button" disabled={!dirty || busy} onClick={() => void save()}><Save size={20} />{saving ? '保存中…' : '保存草稿'}</button>
              <button className="tool-button" disabled={!saved.revision || activating} onClick={() => setModal('versions')}><History size={20} />版本历史</button>
            </div>
            <div className="tool-group">
              <button ref={addNodeButton} className="tool-button" disabled={busy} onClick={() => setModal('nodes')}><Plus size={20} />添加节点</button>
              <button className="tool-button" disabled={busy} onClick={() => setModal('edges')}><GitCommitHorizontal size={20} />添加边</button>
            </div>
            <div className="tool-group">
              <button className="tool-button" onClick={() => setModal('nodes-list')}><List size={20} />节点</button>
              <button className="tool-button" onClick={() => setModal('edges-list')}><List size={20} />边</button>
              <button className="tool-button" onClick={() => initial ? setModal('sources') : goTo('catalogs')}>
                <Database size={20} />{initial ? '添加数据源' : '数据源'}
              </button>
              <button className="tool-button" disabled={busy} onClick={() => void validate()}><ShieldCheck size={20} />{checking ? '正在校验…' : '校验结构'}</button>
              <button className="tool-button" disabled={busy} onClick={() => void validate(true)}><ShieldCheck size={20} />校验数据</button>
            </div>
          </div>
          <span className="draft-status graph-status">草稿{validation && (validation.check_data ? ' · 数据' : ' · 结构')} · <StatusLabel status={status} /></span>
        </div>
        {notices}
        <div className="graph-body">
          <GraphCanvas model={model} selected={selected} select={next => {
            inspectorInvoker.current = document.activeElement as HTMLElement;
            setSelection(next);
          }} />
          {inspector}
        </div>
      </div> : !saved ? <div className="empty" role="alert">{workspace === 'graph' && notice?.text}<button className="button" onClick={refresh}>重试读取配置</button></div>
      : !model && workspace !== 'query' && <div className="empty" role="alert">{draft.error}<button className="button" onClick={() => setModal('yaml')}>编辑 YAML</button></div>}
    {active && <>{listSection && model && <Modal title={listSection === 'nodes' ? '节点标签' : '边类型'} close={() => setModal(undefined)}>
      <div className="dialog-form">
        {!Object.keys(model[listSection]).length && <p className="muted">暂无{listSection === 'nodes' ? '节点标签' : '边类型'}</p>}
        {Object.keys(model[listSection]).map(name => <button className="sidebar-item" key={name} onClick={() => {
          setSelection({ section: listSection, name }); setModal(undefined);
        }}>{listSection === 'nodes' ? <Box size={17} /> : <GitCommitHorizontal size={17} />}<code>{name}</code></button>)}
        <div className="dialog-actions"><button className="button primary" onClick={() => setModal(listSection)}><Plus size={16} />添加{listSection === 'nodes' ? '节点' : '边'}</button></div>
      </div>
    </Modal>}
    {browsing && model && <SourceBrowser id={id} yaml={yaml} model={model} fixedSource={browsing} close={() => setBrowsing(undefined)} />}
    {removing && removing.section !== 'sources' && model && <Modal title={removing.section === 'nodes' ? '删除节点标签' : '删除边类型'} close={() => setRemoving(undefined)}>
      <div className="dialog-form">
        <p>从草稿中删除 <code>{removing.name}</code>？生效版本会在发布后更新。</p>
        {!!dependencies.length && <div>以下边类型也会从草稿中删除：
          <ul>{dependencies.map(edge => <li key={edge}><code>{edge}</code></li>)}</ul>
        </div>}
        <div className="dialog-actions">
          <button className="button" data-initial-focus onClick={() => setRemoving(undefined)}>取消</button>
          <button className="button danger" onClick={() => {
            if (removing.section === 'sources') return;
            const { section, name } = removing;
            edit(doc => removeGraphType(doc, model, section, name));
            setRemoving(undefined);
            setSelection(undefined);
            requestAnimationFrame(() => addNodeButton.current?.focus());
          }}>确认删除</button>
        </div>
      </div>
    </Modal>}
    {modal === 'versions' && saved && <Versions id={id} yaml={yaml} busy={activating} close={() => setModal(undefined)}
      rollback={revision => void activate(revision, 'rollback')} />}
    {modal === 'yaml'  && <Modal title="模型 YAML" close={() => setModal(undefined)}>
      <div className="yaml-dialog">
        <label><span className="sr-only">YAML 配置</span><textarea value={yaml} onChange={event => changeYaml(event.target.value)}
          spellCheck={false} className="mono" rows={22} data-initial-focus /></label>
        {draft.error && <p className="inline-error" role="alert">{draft.error}</p>}
        <div className="dialog-actions">
          <label className="button">导入 YAML 到草稿<input className="sr-only" type="file" accept=".yaml,.yml" disabled={busy} onChange={async event => {
            const file = event.target.files?.[0]; event.target.value = '';
            if (!file) return;
            try {
              if (file.size > 1024 * 1024) throw new Error('YAML 文件超过 1 MiB');
              const next = await file.text();
              modelOf(next);
              if (yaml !== next && window.confirm('将当前编辑内容替换为导入的 YAML？')) changeYaml(next);
            } catch (cause) { setNotice({ text: errorText(cause), error: true }); setModal(undefined); }
          }} /></label>
          <button className="button" onClick={() => downloadText(yaml, id + '.yaml')}>下载草稿 YAML</button>
          <button className="button primary" onClick={() => setModal(undefined)}>完成</button>
        </div>
      </div>
    </Modal>}
    {modal === 'title' && <NameDialog title="编辑模型名称" initial={text(model?.graph)} close={() => setModal(undefined)}
      submit={name => edit(doc => doc.set('graph', name))} />}
    {addSection && <NameDialog title={addSection.key === 'sources' ? '添加数据源' : addSection.key === 'nodes' ? '添加节点' : '添加边'} close={() => setModal(undefined)}
      choices={addSection.key === 'sources' ? [['postgres', 'PostgreSQL'], ['parquet', 'Parquet']] : undefined}
      submit={(name, type) => add(addSection.key, name, type)} />}</>}
  </main>;
}


function Versions({ id, yaml, busy, close, rollback }: { id: string; yaml: string; busy: boolean; close: () => void; rollback: (revision: string) => void }) {
  const [history, setHistory] = useState<VersionHistory>();
  const [error, setError] = useState('');
  const [comparison, setComparison] = useState<{ revision: string; yaml: string }>();
  const [reading, setReading] = useState(false);
  async function compare(revision: string) {
    setReading(true); setError('');
    try {
      const archived = await api<{ yaml: string }>('/models/' + id + '/history/' + encodeURIComponent(revision));
      setComparison({ revision, yaml: archived.yaml });
    } catch (cause) { setError(errorText(cause)); }
    finally { setReading(false); }
  }
  useEffect(() => {
    const controller = new AbortController();
    api<VersionHistory>('/models/' + id + '/history', { signal: controller.signal }).then(setHistory, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [id]);
  return <Modal title="模型版本历史" close={close}>
    <div className={comparison ? 'yaml-dialog' : 'dialog-form'}>
      {comparison && <div className="yaml-comparison">
        <label>历史 YAML · {comparison.revision.slice(0, 14)}<textarea readOnly value={comparison.yaml} rows={12} className="mono" /></label>
        <label>当前草稿 YAML<textarea readOnly value={yaml} rows={12} className="mono" /></label>
        <button className="button compact" onClick={() => downloadText(comparison.yaml, id + '-' + comparison.revision.slice(0, 14) + '.yaml')}>下载历史 YAML</button>
        <button className="button compact" onClick={() => setComparison(undefined)}>关闭对照</button>
      </div>}
      <p className="muted small">回退前会校验该版本与当前数据源，草稿会保留。</p>
      {error ? <p className="inline-error" role="alert">{error}</p> : !history ? <p role="status">正在加载版本…</p>
        : !history.versions.length ? <p>尚未发布任何版本。</p> : <ol className="version-list">
        {history.versions.map((entry, i) => <li key={i}>
            <div><code title={entry.revision}>{entry.revision.slice(0, 14)}</code><small>
              {{ adopt: '既有配置', publish: '发布', rollback: '回退' }[entry.action]}
              {entry.at_ms > 0 && ' · ' + new Date(entry.at_ms).toLocaleString()}</small></div>
            <button className="button compact" disabled={reading} onClick={() => void compare(entry.revision)}>查看并对照</button>
            {history.active_revision === entry.revision
              ? history.versions.findIndex(version => version.revision === entry.revision) === i && <span className="tag">生效中</span>
              : <button className="button" aria-label={'回退至 ' + entry.revision.slice(0, 14)} disabled={busy}
                onClick={() => rollback(entry.revision)}>回退</button>}
          </li>)}
        </ol>}
    </div>
  </Modal>;
}
