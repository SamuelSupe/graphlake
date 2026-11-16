import { diagnosticText, locale, t } from './locale.ts';
import { useEffect, useRef, useState } from 'react';
import { ArrowDownToLine, ArrowRight, ArrowUpRight, Check, CheckCircle2, ChevronDown, Clock3, Database, HardDrive, History, LoaderCircle, Pause, Plus, RefreshCw, Search, Settings2, ShieldCheck, TriangleAlert, X } from 'lucide-react';
import { api, errorText, type SavedModel } from './api';
import Configure from './LocalTableConfig';
import { Modal } from './ui';
import './localTables.css';

export type Mode = 'full' | 'incremental';
type Run = { id: string; mode: Mode; trigger: string; status: string; started_at_ms: number; finished_at_ms: number | null; loaded_rows: number | null; total_rows: number | null; error: string | null };
export type LocalTable = { name: string; source_table: string; columns: string[]; key_columns: string[]; watermark_column: string | null; mode: Mode; interval_seconds: number | null; timeout_seconds: number; revision: string; path: string; ready: boolean; next_run_at_ms: number | null; history: Run[] };
const statuses: Record<string, string> = { running: t('同步中'), succeeded: t('同步成功'), failed: t('同步失败'), interrupted: t('已中断'), cancelled: t('已取消') };
const time = (ms: number | null) => ms ? new Date(ms).toLocaleString(locale) : '—';
const needsAttention = (table: LocalTable) => ['failed', 'interrupted'].includes(table.history[0]?.status);
const modeName = (mode: Mode) => mode === 'full' ? t('全量替换') : t('增量更新');
const intervalName = (seconds: number | null) => !seconds ? t('手动触发') : seconds % 86400 === 0 ? t('每 {0} 天', seconds / 86400) : seconds % 3600 === 0 ? t('每 {0} 小时', seconds / 3600) : seconds % 60 === 0 ? t('每 {0} 分钟', seconds / 60) : t('每 {0} 秒', seconds);
const duration = (run: Run) => run.finished_at_ms === null ? t('运行中') : run.finished_at_ms - run.started_at_ms < 1000 ? `${run.finished_at_ms - run.started_at_ms} ms` : `${((run.finished_at_ms - run.started_at_ms) / 1000).toFixed(1)} s`;

function RunState({ state }: { state?: string }) {
  const Icon = state === 'running' ? LoaderCircle : state === 'succeeded' ? CheckCircle2 : state === 'failed' || state === 'interrupted' ? TriangleAlert : state === 'cancelled' ? Pause : Clock3;
  return <span className={'sync-state sync-state-' + (state ?? 'idle')}><Icon size={13} aria-hidden="true" />{state ? statuses[state] : t('等待首次加载')}</span>;
}

export default function LocalTables({ saved, clean, close, attach }: {
  saved: SavedModel; clean: boolean; close: () => void; attach: (table: LocalTable, switchMappings: boolean) => void;
}) {
  const base = '/models/' + saved.id + '/local-tables';
  const [tables, setTables] = useState<LocalTable[]>();
  const [error, setError] = useState('');
  const [loadError, setLoadError] = useState('');
  const [notice, setNotice] = useState('');
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [editing, setEditing] = useState<LocalTable | 'new'>();
  const [history, setHistory] = useState<string>();
  const [pending, setPending] = useState(new Set<string>());
  const [attempt, retry] = useState(0);
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function refresh() {
      try {
        const next = await api<LocalTable[]>(base, { signal: controller.signal });
        if (!controller.signal.aborted) { setTables(next); setLoadError(''); }
      } catch (cause) { if (!controller.signal.aborted) setLoadError(errorText(cause)); }
      finally { if (!controller.signal.aborted) timer = setTimeout(refresh, 2000); }
    }
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [base, attempt]);

  async function action(name: string, operation: string, body?: unknown) {
    setPending(previous => new Set(previous).add(name)); setError(''); setNotice('');
    try {
      await api(base + '/' + name + '/' + operation, { method: 'POST', ...(body ? { body: JSON.stringify(body) } : {}) });
      if (alive.current) { setNotice(operation === 'cancel' ? t('已请求取消任务') : t('已提交加载任务，可以离开此页面，任务将继续运行。')); retry(value => value + 1); }
    } catch (cause) { if (alive.current) setError(errorText(cause)); }
    finally { if (alive.current) setPending(previous => { const next = new Set(previous); next.delete(name); return next; }); }
  }

  function useLocal(table: LocalTable, switchMappings: boolean) {
    try {
      attach(table, switchMappings);
      setNotice(switchMappings ? t('已在草稿中切换使用此源表的映射；保存并发布后生效。') : t('已加入模型草稿，可在节点或边的来源选择器中选择此表；保存并发布后生效。'));
      setError('');
    } catch (cause) { setError(errorText(cause)); }
  }

  const visible = tables?.filter(table => (table.name + ' ' + table.source_table).toLowerCase().includes(search.trim().toLowerCase())
    && (filter === 'all' || filter === 'ready' && table.ready || filter === 'running' && table.history[0]?.status === 'running' || filter === 'attention' && needsAttention(table) || filter === 'empty' && !table.ready));
  const runs = tables?.find(table => table.name === history)?.history ?? [];
  const summary = [
    { label: t('本地表'), count: tables?.length, Icon: Database, kind: 'total' },
    { label: t('副本已就绪'), count: tables?.filter(table => table.ready).length, Icon: CheckCircle2, kind: 'ready' },
    { label: t('同步中'), count: tables?.filter(table => table.history[0]?.status === 'running').length, Icon: RefreshCw, kind: 'running' },
    { label: t('定时刷新'), count: tables?.filter(table => table.interval_seconds).length, Icon: Clock3, kind: 'scheduled' },
  ];

  return <Modal title={t('本地表与数据同步')} description={t('管理外部源表的本地副本、刷新策略与加载记录。')} size="wide" close={close}>
    <div className="local-tables panel-body">
      <div className="sync-overview" aria-label={t('同步概况')}>{summary.map(({ label, count, Icon, kind }) => <div className={'sync-metric sync-metric-' + kind} key={kind}>
        <span><Icon size={15} aria-hidden="true" />{label}</span><strong>{count?.toLocaleString(locale) ?? '—'}</strong>
      </div>)}</div>
      {!clean && <p className="sync-notice"><Settings2 size={16} aria-hidden="true" />{t('先保存模型草稿，再创建本地表。已有表仍可管理。')}</p>}
      {(error || loadError) && <p className="sync-notice is-error" role="alert"><TriangleAlert size={16} aria-hidden="true" />{error || loadError}</p>}
      {notice && <p className="sync-notice" role="status"><Check size={16} aria-hidden="true" /><span>{notice}</span><button className="icon-button" aria-label={t('关闭提示')} onClick={() => setNotice('')}><X size={14} /></button></p>}
      <div className="sync-directory-heading"><div><h3>{t('本地表')}</h3><span>{tables ? t('{0} 张表', tables.length) : t('正在连接…')}</span></div>
        <button className="button primary" onClick={() => setEditing('new')} disabled={!clean}><Plus size={16} aria-hidden="true" />{t('创建本地表')}</button></div>
      <div className="sync-filters"><label className="sync-search"><Search size={16} aria-hidden="true" /><span className="sr-only">{t('搜索本地表')}</span><input type="search" value={search} placeholder={t('搜索表名或来源…')} onChange={event => setSearch(event.target.value)} /></label>
        <label><span className="sr-only">{t('筛选同步状态')}</span><select value={filter} onChange={event => setFilter(event.target.value)}><option value="all">{t('全部状态')}</option><option value="ready">{t('副本已就绪')}</option><option value="running">{t('同步中')}</option><option value="attention">{t('需要关注')}</option><option value="empty">{t('尚未加载')}</option></select></label>
        <span className="sync-list-count" role="status">{tables ? t('{0} 个结果', visible?.length) : t('正在读取…')}</span></div>
      {!tables && <div className="sync-loading" role="status"><div /><div /><span className="sr-only">{t('正在读取本地表…')}</span></div>}
      {tables?.length === 0 && <div className="sync-empty"><span className="sync-empty-icon"><Database size={26} /><ArrowRight size={18} /><HardDrive size={26} /></span><h3>{t('让源表拥有本地副本')}</h3><p>{t('选择来源与字段，按需启用增量更新和定时刷新。')}<br />{t('加载完成后，即可用于图模型查询。')}</p><button className="button" disabled={!clean} onClick={() => setEditing('new')}><Plus size={15} />{t('创建第一张本地表')}</button></div>}
      {tables && tables.length > 0 && !visible?.length && <div className="sync-empty"><Search size={28} /><h3>{t('没有匹配的本地表')}</h3><p>{t('尝试其他表名或同步状态。')}</p><button className="text-button" onClick={() => { setSearch(''); setFilter('all'); }}>{t('清除筛选')}</button></div>}
      <div className="local-table-list">{visible?.map(table => {
        const run = table.history[0];
        const running = run?.status === 'running';
        const locked = pending.has(table.name);
        const success = table.history.find(item => item.status === 'succeeded');
        return <section className="local-table-card" data-state={run?.status ?? 'idle'} key={table.name} aria-label={t('本地表 {0}', table.name)}>
          <div className="local-table-title"><span className="sync-table-icon"><HardDrive size={20} aria-hidden="true" /></span><div><h3>{table.name}</h3><p>{table.columns.length}{t(' 个字段{0}', table.key_columns.length ? t(' · 主键 {0}', table.key_columns.join(' + ')) : t(' · 保留重复行'))}</p></div><RunState state={run?.status} /></div>
          <div className="sync-source-line"><span><Database size={14} aria-hidden="true" />{t('外部来源')}</span><code>{table.source_table}</code><ArrowRight size={14} aria-hidden="true" /><span className="sync-local-chip">{t('本地副本')}</span></div>
          <dl className="local-table-facts"><div className="sync-row-count"><dt>{t('最近成功行数')}</dt><dd>{success?.total_rows?.toLocaleString(locale) ?? '—'}<small>{t('行')}</small></dd></div>
            <div><dt>{t('刷新策略')}</dt><dd>{modeName(table.mode)}</dd><span><Clock3 size={12} aria-hidden="true" />{intervalName(table.interval_seconds)}</span></div>
            <div><dt>{t('最近成功')}</dt><dd>{success ? time(success.finished_at_ms) : t('尚未完成')}</dd><span>{success ? t('耗时 {0}', duration(success)) : t('加载后可查询本地副本')}</span></div>
            <div><dt>{t('下次刷新')}</dt><dd>{running && table.interval_seconds ? t('本次完成后计时') : table.next_run_at_ms ? time(table.next_run_at_ms) : t('未启用定时')}</dd><span>{table.watermark_column ? t('增量字段 · {0}', table.watermark_column) : t('完整读取来源表')}</span></div></dl>
          {run?.error && <p className="sync-run-error"><TriangleAlert size={14} aria-hidden="true" />{diagnosticText(run.error)}</p>}
          <footer className="local-table-actions"><div>
            {running ? <button className="button compact" disabled={locked} onClick={() => void action(table.name, 'cancel')}><Pause size={14} />{t('取消加载')}</button>
              : <button className="button compact sync-primary" disabled={locked} onClick={() => void action(table.name, 'load', { mode: table.mode })}><RefreshCw size={14} />{table.ready ? t('立即同步') : t('加载副本')}</button>}
            <button className="button compact sync-quiet" onClick={() => setEditing(table)}><Settings2 size={14} />{t('刷新配置')}</button>
            <button className="button compact sync-quiet" onClick={() => setHistory(table.name)}><History size={14} />{t('加载历史')}</button>
          </div><details className="sync-more"><summary>{t('更多操作')}<ChevronDown size={14} aria-hidden="true" /></summary><div>
            <button disabled={running || locked} onClick={event => { event.currentTarget.closest('details')?.removeAttribute('open'); void action(table.name, 'load', { mode: 'full' }); }}><ArrowDownToLine size={15} />{t('从源表全量加载')}</button>
            {table.key_columns.length > 0 && table.watermark_column && table.mode !== 'incremental' && <button disabled={running || locked} onClick={event => { event.currentTarget.closest('details')?.removeAttribute('open'); void action(table.name, 'load', { mode: 'incremental' }); }}><RefreshCw size={15} />{t('增量更新')}</button>}
            <button disabled={!table.ready} onClick={event => { event.currentTarget.closest('details')?.removeAttribute('open'); useLocal(table, false); }}><Database size={15} />{t('加入模型数据源')}</button>
            <button disabled={!table.ready} onClick={event => { event.currentTarget.closest('details')?.removeAttribute('open'); useLocal(table, true); }}><ArrowUpRight size={15} />{t('切换映射到本地')}</button>
          </div></details></footer>
          {table.ready && needsAttention(table) && <p className="sync-retained"><ShieldCheck size={13} />{t('上次成功的副本仍可查询')}</p>}
        </section>;
      })}</div>
      {tables && tables.length > 0 && <p className="sync-footnote"><ShieldCheck size={14} aria-hidden="true" />{t('加载成功后才替换副本，失败时保留已有数据。')}</p>}
    </div>
    {editing && <Configure key={typeof editing === 'string' ? editing : editing.name} saved={saved} table={editing === 'new' ? undefined : editing}
      close={() => setEditing(undefined)} done={() => { setEditing(undefined); retry(value => value + 1); setNotice(t('本地表配置已保存')); }} />}
    {history && <Modal title={t('{0} · 加载历史', history)} description={t('查看每次加载的执行结果与数据变化，保留最近 20 次记录。')} size="medium" close={() => setHistory(undefined)}>
      <div className="panel-body local-history"><div className="sync-history-summary"><History size={17} /><span>{t('最近执行')}</span><strong>{runs.length}{t(' 次')}</strong></div>
        <ol>{runs.map(run => <li key={run.id} data-state={run.status}>
          <span className="sync-timeline-dot" aria-hidden="true" />
          <article><div className="sync-history-heading"><RunState state={run.status} /><span>{modeName(run.mode)} · {run.trigger === 'scheduled' ? t('定时触发') : t('手动触发')}</span><span>{duration(run)}</span></div>
            <p className="sync-history-time"><time dateTime={new Date(run.started_at_ms).toISOString()}>{time(run.started_at_ms)}</time></p>
            <dl><div><dt>{t('本次读取')}</dt><dd>{run.loaded_rows?.toLocaleString(locale) ?? '—'} <small>{t('行')}</small></dd></div><ArrowRight size={14} aria-hidden="true" /><div><dt>{t('提交后总计')}</dt><dd>{run.total_rows?.toLocaleString(locale) ?? '—'} <small>{t('行')}</small></dd></div></dl>
            {run.error && <p className="sync-run-error"><TriangleAlert size={14} />{diagnosticText(run.error)}</p>}
          </article>
        </li>)}</ol>
        {!runs.length && <div className="sync-empty"><Clock3 size={28} /><h3>{t('还没有加载记录')}</h3><p>{t('从本地表列表提交首次加载。')}</p></div>}
        <p className="sync-footnote">{t('增量任务会重读水位边界；读取行数包含这部分数据。')}</p>
      </div>
    </Modal>}
  </Modal>;
}
