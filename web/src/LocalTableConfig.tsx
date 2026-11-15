import { useRef, useState, type FormEvent } from 'react';
import { ArrowRight, Clock3, Database, KeyRound, ListFilter, RefreshCw, Settings2 } from 'lucide-react';
import { api, errorText, type Column, type SavedModel } from './api';
import { modelOf } from './model';
import { SourceBrowser } from './SourceBrowser';
import { Modal } from './ui';
import type { LocalTable, Mode } from './LocalTables';

export default function Configure({ saved, table, close, done }: { saved: SavedModel; table?: LocalTable; close: () => void; done: () => void }) {
  const [name, setName] = useState('');
  const [source, setSource] = useState('');
  const [columns, setColumns] = useState<Column[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [keys, setKeys] = useState<string[]>([]);
  const [watermark, setWatermark] = useState('');
  const [mode, setMode] = useState<Mode>(table?.mode ?? 'full');
  const [periodic, setPeriodic] = useState(Boolean(table?.interval_seconds));
  const [interval, setInterval] = useState(String(table?.interval_seconds ?? 3600));
  const [timeout, setTimeoutValue] = useState(String(table?.timeout_seconds ?? 3600));
  const [loadNow, setLoadNow] = useState(true);
  const [browse, setBrowse] = useState(false);
  const [saving, setSaving] = useState(false);
  const [created, setCreated] = useState(false);
  const [error, setError] = useState('');
  const errorSummary = useRef<HTMLParagraphElement>(null);
  const canIncrement = table ? table.key_columns.length > 0 && Boolean(table.watermark_column) : true;

  async function submit(event: FormEvent) {
    event.preventDefault(); setError('');
    if (!table && (!source || !selected.length || mode === 'incremental' && (!keys.length || !watermark))) {
      setError('请选择源表和字段；增量更新还需要主键和增量字段。');
      requestAnimationFrame(() => errorSummary.current?.focus()); return;
    }
    setSaving(true);
    const base = '/models/' + saved.id + '/local-tables';
    try {
      const settings = { mode, interval_seconds: periodic ? Number(interval) : null, timeout_seconds: Number(timeout) };
      if (table) await api(base + '/' + table.name, { method: 'PUT', body: JSON.stringify({ revision: table.revision, ...settings }) });
      else {
        if (!created) {
          await api(base, { method: 'POST', body: JSON.stringify({ name, source_table: source, columns: selected, key_columns: keys, watermark_column: watermark || null, model_revision: saved.revision, ...settings }) });
          setCreated(true);
        }
        if (loadNow) await api(base + '/' + name + '/load', { method: 'POST', body: JSON.stringify({ mode }) });
      }
      done();
    } catch (cause) { setError(errorText(cause)); requestAnimationFrame(() => errorSummary.current?.focus()); }
    finally { setSaving(false); }
  }

  return <Modal title={table ? table.name + ' · 刷新配置' : '创建本地表'} description={table ? '调整更新方式与运行计划，下次任务使用新配置。' : '选择来源、定义更新方式，并设置副本刷新计划。'} size="medium" close={close}>
    <form className="panel-layout local-table-form" onSubmit={event => void submit(event)}>
      <div className="panel-body">
        {error && <p ref={errorSummary} tabIndex={-1} className="notice error" role="alert">{error}{created && ' 表已创建，可重试加载或关闭后在列表中操作。'}</p>}
        <fieldset disabled={saving || created} className="sync-form-sections">
          {!table ? <section className="sync-form-section">
            <div className="sync-section-title"><Database size={17} aria-hidden="true" /><h3>来源与字段</h3><span>01</span></div>
            <label>本地表名称<input required maxLength={48} pattern="[A-Za-z0-9][A-Za-z0-9_\-]*" value={name} placeholder="例如 accounts_local" onChange={e => setName(e.target.value)} data-initial-focus /></label>
            <div className="sync-source-picker"><span className="field-label">外部源表</span><button type="button" onClick={() => setBrowse(true)}>
              <Database size={19} aria-hidden="true" /><span><strong>{source || '选择外部源表'}</strong><small>{source ? `${columns.length} 个可用字段 · 点击更换来源` : '浏览已配置的数据源、表与字段'}</small></span><ArrowRight size={16} aria-hidden="true" />
            </button></div>
            {columns.length > 0 && <fieldset className="local-column-picker"><legend>加载字段与主键</legend><div className="sync-columns-heading"><span>加载字段 · 已选 {selected.length}</span><span><KeyRound size={12} />主键</span></div>{columns.map(column => <div className="sync-column-row" key={column.name}>
              <label><input type="checkbox" checked={selected.includes(column.name)} onChange={e => {
                setSelected(previous => e.target.checked ? [...previous, column.name] : previous.filter(name => name !== column.name));
                if (!e.target.checked) { setKeys(previous => previous.filter(name => name !== column.name)); if (watermark === column.name) setWatermark(''); }
              }} /><code>{column.name}</code><small>{column.data_type}</small></label>
              <input type="checkbox" aria-label={column.name + ' 作为主键'} disabled={!selected.includes(column.name)} checked={keys.includes(column.name)} onChange={e => setKeys(previous => e.target.checked ? [...previous, column.name] : previous.filter(name => name !== column.name))} />
            </div>)}</fieldset>}
          </section> : <div className="sync-config-source"><Database size={18} aria-hidden="true" /><div><span>当前来源</span><code>{table.source_table}</code></div><span>{table.columns.length} 个字段</span></div>}
          <section className="sync-form-section">
            <div className="sync-section-title"><ListFilter size={17} aria-hidden="true" /><h3>更新方式</h3><span>{table ? '01' : '02'}</span></div>
            <div className="sync-mode-options" role="group" aria-label="刷新方式">
              <label data-selected={mode === 'full' || undefined}><span><RefreshCw size={17} /><input type="radio" name="sync-mode" value="full" checked={mode === 'full'} onChange={() => setMode('full')} /></span><strong>全量替换</strong><small>读取完整源表，替换现有副本</small></label>
              {canIncrement && <label data-selected={mode === 'incremental' || undefined}><span><ArrowRight size={17} /><input type="radio" name="sync-mode" value="incremental" checked={mode === 'incremental'} onChange={() => setMode('incremental')} /></span><strong>增量更新</strong><small>按水位读取变化，按主键覆盖</small></label>}
            </div>
            {!table && <label>增量字段{mode === 'incremental' ? '（必选）' : '（可选）'}<select value={watermark} onChange={e => setWatermark(e.target.value)}><option value="">选择增量字段</option>{selected.map(name => <option key={name}>{name}</option>)}</select></label>}
            {table?.watermark_column && <p className="sync-config-keys"><KeyRound size={14} /><span>主键 <code>{table.key_columns.join(' + ')}</code></span><span>增量字段 <code>{table.watermark_column}</code></span></p>}
            <p className="field-help">{mode === 'incremental' ? '首次自动全量加载。主键需唯一、非空；增量字段需为整数、日期或时间戳，并随每次新增或更新推进。删除和旧水位数据需全量刷新。' : '完整反映源表的新增、更新与删除。加载成功后再替换，失败时保留已有副本。'}</p>
          </section>
          <section className="sync-form-section">
            <div className="sync-section-title"><Clock3 size={17} aria-hidden="true" /><h3>运行计划</h3><span>{table ? '02' : '03'}</span></div>
            <label className="sync-toggle-row"><span><strong>启用定时刷新</strong><small>按固定间隔，自动保持副本更新</small></span><input type="checkbox" checked={periodic} onChange={e => setPeriodic(e.target.checked)} /></label>
            <div className="sync-timing-fields">{periodic && <label>刷新间隔<span className="sync-input-unit"><input aria-label="间隔（秒）" type="number" min={30} max={2592000} step={1} required value={interval} onChange={e => setInterval(e.target.value)} /><span>秒</span></span></label>}
              <label>加载超时<span className="sync-input-unit"><input aria-label="单次加载超时（秒）" type="number" min={1} max={43200} step={1} required value={timeout} onChange={e => setTimeoutValue(e.target.value)} /><span>秒</span></span></label></div>
            <p className="field-help">{periodic ? '每次任务结束后开始计时，失败按相同间隔重试；同一张表不会重叠运行。最小间隔为 30 秒。' : '关闭定时刷新后，可随时手动同步；已运行的任务需要单独取消。'}</p>
            {!table && <label className="sync-toggle-row"><span><strong>创建后立即加载</strong><small>在后台准备第一份本地副本</small></span><input type="checkbox" checked={loadNow} onChange={e => setLoadNow(e.target.checked)} /></label>}
          </section>
          {table && <p className="sync-footnote"><Settings2 size={14} />来源与字段创建后固定，需要更改时请创建新表。</p>}
        </fieldset>
      </div>
      <footer className="panel-footer"><button type="button" className="button" onClick={close}>关闭</button><button type="submit" className="button primary" disabled={saving}>{saving ? '提交中…' : created ? '重试加载' : table ? '保存配置' : '创建本地表'}</button></footer>
    </form>
    {browse && <SourceBrowser id={saved.id} yaml={saved.yaml} model={modelOf(saved.yaml)} close={() => setBrowse(false)} choose={selection => {
      setSource(selection.table); setColumns(selection.columns); setSelected(selection.columns.map(c => c.name)); setKeys([]); setWatermark('');
    }} />}
  </Modal>;
}
