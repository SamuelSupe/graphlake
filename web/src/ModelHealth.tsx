import { useEffect, useRef, useState } from 'react';
import { Activity, Square } from 'lucide-react';
import { api, ApiError, errorText, type ModelReadiness, type ModelSummary } from './api';

type Check = { revision: string; at: number; result?: ModelReadiness; error?: string };
const labels: Record<ModelReadiness['status'], string> = {
  ready: '运行正常', source_unavailable: '来源不可用', mapping_invalid: '映射不兼容', worker_unavailable: 'Worker 异常',
};
const advice: Record<ModelReadiness['status'], string> = {
  ready: '来源与映射检查通过。', source_unavailable: '检查来源连接、凭据环境变量及文件路径，然后重新检查。',
  mapping_invalid: '核对来源字段、业务键与边端点类型；修正映射后重新校验并发布。',
  worker_unavailable: '检查列出的 Worker 进程、网络和来源访问权限，然后重新检查。',
};

export function ModelHealth({ models, openModel, visible = true }: { models: ModelSummary[]; openModel?: (id: string, workspace: 'graph' | 'catalogs') => void; visible?: boolean }) {
  const [checks, setChecks] = useState<Record<string, Check>>({});
  const [checking, setChecking] = useState<string>();
  const [onlyProblems, setOnlyProblems] = useState(false);
  const active = useRef<AbortController | null>(null);
  useEffect(() => {
    if (!visible) setChecking(undefined);
    return () => {
      active.current?.abort();
      active.current = null;
    };
  }, [visible]);
  async function inspect(candidates: ModelSummary[]) {
    if (active.current) return;
    const controller = new AbortController();
    active.current = controller;
    try {
      // Probes can open source connections; inspect serially to avoid flooding a large catalog.
      for (const model of candidates) {
        if (controller.signal.aborted) break;
        if (!model.active_revision || model.archive_error) continue;
        setChecking(model.id);
        const check: Check = { revision: model.active_revision, at: Date.now() };
        try { check.result = await api<ModelReadiness>('/models/' + model.id + '/readiness', { signal: controller.signal }); }
        catch (cause) {
          if (controller.signal.aborted) break;
          if (cause instanceof ApiError && cause.readiness) check.result = cause.readiness;
          else check.error = errorText(cause);
        }
        if (controller.signal.aborted) break;
        check.at = Date.now();
        setChecks(old => ({ ...old, [model.id]: check }));
      }
    } finally { if (active.current === controller) { active.current = null; setChecking(undefined); } }
  }
  function stop() { active.current?.abort(); active.current = null; setChecking(undefined); }
  const checkFor = (model: ModelSummary) => checks[model.id]?.revision === model.active_revision ? checks[model.id] : undefined;
  const problem = (model: ModelSummary) => !!model.archive_error || model.status === 'failed'
    || !!checkFor(model)?.error || !!checkFor(model)?.result && checkFor(model)!.result!.status !== 'ready';
  const problems = models.filter(problem);
  const shown = onlyProblems ? problems : models;
  const published = models.filter(model => model.active_revision && !model.archive_error);
  const complete = published.filter(model => !!checkFor(model)).length;

  return <section className="model-health" aria-label="模型运行健康">
    <div className="health-heading"><div><h2>模型健康</h2><p className="muted">检查已发布模型的来源与映射，未检查不代表正常。</p></div>
      {checking ? <button className="button compact" onClick={stop}><Square size={15} aria-hidden="true" />停止检查</button>
        : <button className="button compact" disabled={!published.length} onClick={() => void inspect(published)}><Activity size={16} aria-hidden="true" />检查全部</button>}
    </div>
    <div className="health-tools"><p role="status">{checking ? `正在检查 ${models.find(model => model.id === checking)?.name ?? checking}` : `已检查 ${complete} / ${published.length} 个已发布模型 · ${problems.length} 个模型需处理`}</p>
      <label className="check-label"><input type="checkbox" checked={onlyProblems} onChange={event => setOnlyProblems(event.target.checked)} />只看需处理</label>
    </div>
    <ul className="health-models">{shown.map(model => {
      const check = checkFor(model), result = check?.result;
      const status = model.archive_error ? '版本异常' : !model.active_revision ? '未发布' : check?.error ? '检查失败' : result ? labels[result.status] : '尚未检查';
      return <li key={model.id}>
        <div className="health-model-title"><div><strong>{model.name}</strong><code>{model.id}</code></div>
          <div className="health-model-actions">
            <span className={'health-state ' + (model.archive_error || check?.error || result && result.status !== 'ready' ? 'error' : result?.status === 'ready' ? 'ok' : '')}>{status}</span>
            {openModel && <button className="text-button" aria-label={(result?.status === 'source_unavailable' ? '配置数据源 ' : '打开模型 ') + model.name} onClick={() => openModel(model.id, result?.status === 'source_unavailable' ? 'catalogs' : 'graph')}>
              {result?.status === 'source_unavailable' ? '配置数据源' : '打开模型'}
            </button>}
            {model.active_revision && !model.archive_error && <button className="text-button" aria-label={'检查 ' + model.name} disabled={!!checking} onClick={() => void inspect([model])}>检查</button>}
          </div>
        </div>
        <p className="muted small">{model.active_revision ? '发布版本 ' + model.active_revision.slice(0, 10) : '尚无生效版本'} · 草稿{model.status === 'failed' ? '校验失败' : model.status === 'passed' ? '校验通过' : '未校验'}
          {check && !model.archive_error && model.active_revision && <> · 检查于 <time dateTime={new Date(check.at).toISOString()}>{new Date(check.at).toLocaleTimeString()}</time></>}</p>
        {(model.archive_error || check?.error || result && result.status !== 'ready') && <p className="health-advice">{model.archive_error ? '版本归档无法读取，请在版本历史中恢复原始归档。'
          : check?.error ? '检查未完成，请查看错误详情后重试。' : advice[result!.status]}</p>}
        {model.status === 'failed' && !model.archive_error && <p className="health-advice">草稿校验失败，请打开模型修正后再发布。{model.active_revision && '当前查询仍使用生效版本。'}</p>}
        {result && <details open={result.status !== 'ready' || undefined}>
          <summary>数据源 {Object.values(result.sources).filter(Boolean).length} / {Object.keys(result.sources).length} 可用
            {!!result.workers?.length && <> · Worker {result.workers.filter(({ probe }) => probe?.status === 'ready').length} / {result.workers.length} 正常</>}</summary>
          <ul className="health-results">
            {Object.entries(result.sources).map(([name, ok]) => <li key={name} className={ok ? '' : 'health-failure'}>{name} · {ok ? '可用' : '不可用'}</li>)}
            {result.workers?.map(({ worker, probe }) => <li key={'worker-' + worker} className={probe?.status === 'ready' ? '' : 'health-failure'}>Worker {worker} · {!probe ? '无法连接' : labels[probe.status]}
              {probe && Object.entries(probe.sources).filter(([, ok]) => !ok).map(([name]) => ' · ' + name).join('')}</li>)}
          </ul>
        </details>}
        {result?.location && <p className="small">异常位置：{result.location.kind === 'table' ? [result.location.table, result.location.column].filter(Boolean).join('.') : result.location.label + ' · ' + (result.location.kind === 'field' ? result.location.field : result.location.column)}</p>}
        {(model.archive_error || check?.error) && <details><summary>错误详情</summary><p className="small">{model.archive_error || check?.error}</p></details>}
      </li>;
    })}</ul>
    {!shown.length && <p className="empty">{onlyProblems ? '没有已发现的异常，未检查的模型仍需检查。' : '创建并发布模型后，即可在这里检查运行状态。'}</p>}
  </section>;
}
