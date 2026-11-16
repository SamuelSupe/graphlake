import { diagnosticText, locale, t } from './locale.ts';
import { useEffect, useRef, useState } from 'react';
import { Activity, Square } from 'lucide-react';
import { api, ApiError, errorText, type ModelFreshness, type ModelReadiness, type ModelSummary } from './api';

type Check = { revision: string; at: number; result?: ModelReadiness; error?: string; freshness?: ModelFreshness };
const labels: Record<ModelReadiness['status'], string> = {
  ready: t('运行正常'), source_unavailable: t('来源不可用'), mapping_invalid: t('映射不兼容'), worker_unavailable: t('Worker 异常'),
};
const advice: Record<ModelReadiness['status'], string> = {
  ready: t('来源与映射检查通过。'), source_unavailable: t('检查来源连接、凭据环境变量及文件路径，然后重新检查。'),
  mapping_invalid: t('核对来源字段、业务键与边端点类型；修正映射后重新校验并发布。'),
  worker_unavailable: t('检查列出的 Worker 进程、网络和来源访问权限，然后重新检查。'),
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
        if (!controller.signal.aborted) check.freshness = await api<ModelFreshness>('/models/' + model.id + '/freshness', { signal: controller.signal }).catch(() => undefined);
        if (controller.signal.aborted) break;
        check.at = Date.now();
        setChecks(old => ({ ...old, [model.id]: check }));
      }
    } finally { if (active.current === controller) { active.current = null; setChecking(undefined); } }
  }
  function stop() { active.current?.abort(); active.current = null; setChecking(undefined); }
  const checkFor = (model: ModelSummary) => checks[model.id]?.revision === model.active_revision ? checks[model.id] : undefined;
  const problem = (model: ModelSummary) => !!model.archive_error || model.status === 'failed'
    || !!checkFor(model)?.error || !!checkFor(model)?.result && checkFor(model)!.result!.status !== 'ready'
    || checkFor(model)?.freshness?.refresh?.status === 'failed' || checkFor(model)?.freshness?.csr?.current === false;
  const problems = models.filter(problem);
  const shown = onlyProblems ? problems : models;
  const published = models.filter(model => model.active_revision && !model.archive_error);
  const complete = published.filter(model => !!checkFor(model)).length;

  return <section className="model-health" aria-label={t('模型运行健康')}>
    <div className="health-heading"><div><h2>{t('模型健康')}</h2><p className="muted">{t('检查已发布模型的来源与映射，未检查不代表正常。')}</p></div>
      {checking ? <button className="button compact" onClick={stop}><Square size={15} aria-hidden="true" />{t('停止检查')}</button>
        : <button className="button compact" disabled={!published.length} onClick={() => void inspect(published)}><Activity size={16} aria-hidden="true" />{t('检查全部')}</button>}
    </div>
    <div className="health-tools"><p role="status">{checking ? t('正在检查 {0}', models.find(model => model.id === checking)?.name ?? checking) : t('已检查 {0} / {1} 个已发布模型 · {2} 个模型需处理', complete, published.length, problems.length)}</p>
      <label className="check-label"><input type="checkbox" checked={onlyProblems} onChange={event => setOnlyProblems(event.target.checked)} />{t('只看需处理')}</label>
    </div>
    <ul className="health-models">{shown.map(model => {
      const check = checkFor(model), result = check?.result;
      const status = model.archive_error ? t('版本异常') : !model.active_revision ? t('未发布') : check?.error ? t('检查失败') : result ? labels[result.status] : t('尚未检查');
      return <li key={model.id}>
        <div className="health-model-title"><div><strong>{model.name}</strong><code>{model.id}</code></div>
          <div className="health-model-actions">
            <span className={'health-state ' + (model.archive_error || check?.error || result && result.status !== 'ready' ? 'error' : result?.status === 'ready' ? 'ok' : '')}>{status}</span>
            {openModel && <button className="text-button" aria-label={(result?.status === 'source_unavailable' ? t('配置数据源 ') : t('打开模型 ')) + model.name} onClick={() => openModel(model.id, result?.status === 'source_unavailable' ? 'catalogs' : 'graph')}>
              {result?.status === 'source_unavailable' ? t('配置数据源') : t('打开模型')}
            </button>}
            {model.active_revision && !model.archive_error && <button className="text-button" aria-label={t('检查 {0}', model.name)} disabled={!!checking} onClick={() => void inspect([model])}>{t('检查')}</button>}
          </div>
        </div>
        <p className="muted small">{model.active_revision ? t('发布版本 {0}', model.active_revision.slice(0, 10)) : t('尚无生效版本')}{t(' · 草稿{0}', model.status === 'failed' ? t('校验失败') : model.status === 'passed' ? t('校验通过') : t('未校验'))}
          {check && !model.archive_error && model.active_revision && <>{t(' · 检查于 ')}<time dateTime={new Date(check.at).toISOString()}>{new Date(check.at).toLocaleTimeString(locale)}</time></>}</p>
        {(model.archive_error || check?.error || result && result.status !== 'ready') && <p className="health-advice">{model.archive_error ? t('版本归档无法读取，请在版本历史中恢复原始归档。')
          : check?.error ? t('检查未完成，请查看错误详情后重试。') : advice[result!.status]}</p>}
        {model.status === 'failed' && !model.archive_error && <p className="health-advice">{t('草稿校验失败，请打开模型修正后再发布。')}{model.active_revision && t('当前查询仍使用生效版本。')}</p>}
        {result && <details open={result.status !== 'ready' || undefined}>
          <summary>{t('数据源 {0} / {1} 可用', Object.values(result.sources).filter(Boolean).length, Object.keys(result.sources).length)}
            {!!result.workers?.length && <> · Worker {result.workers.filter(({ probe }) => probe?.status === 'ready').length} / {result.workers.length}{t(' 正常')}</>}</summary>
          <ul className="health-results">
            {Object.entries(result.sources).map(([name, ok]) => <li key={name} className={ok ? '' : 'health-failure'}>{name} · {ok ? t('可用') : t('不可用')}</li>)}
            {result.workers?.map(({ worker, probe }) => <li key={'worker-' + worker} className={probe?.status === 'ready' ? '' : 'health-failure'}>Worker {worker} · {!probe ? t('无法连接') : labels[probe.status]}
              {probe && Object.entries(probe.sources).filter(([, ok]) => !ok).map(([name]) => ' · ' + name).join('')}</li>)}
          </ul>
        </details>}
        {(check?.freshness?.csr || check?.freshness?.auto_refresh_csr) && <p className="small">
          {check.freshness.refresh?.status === 'failed' ? t('CSR 刷新失败{0}', check.freshness.refresh.error ? ' · ' + diagnosticText(check.freshness.refresh.error) : '') : check.freshness.refresh?.status === 'building' ? t('正在刷新 CSR') : check.freshness.csr?.current ? t('CSR 已追平本地副本') : check.freshness.csr ? t('CSR 落后于本地副本') : t('等待首次 CSR 刷新')}
          {check.freshness.csr?.built_at_ms && t(' · 构建于 {0}', new Date(check.freshness.csr.built_at_ms).toLocaleString(locale))}
        </p>}
        {result?.location && <p className="small">{t('异常位置：{0}', result.location.kind === 'table' ? [result.location.table, result.location.column].filter(Boolean).join('.') : result.location.label + ' · ' + (result.location.kind === 'field' ? result.location.field : result.location.column))}</p>}
        {(model.archive_error || check?.error) && <details><summary>{t('错误详情')}</summary><p className="small">{diagnosticText(model.archive_error || check?.error || '')}</p></details>}
      </li>;
    })}</ul>
    {!shown.length && <p className="empty">{onlyProblems ? t('没有已发现的异常，未检查的模型仍需检查。') : t('创建并发布模型后，即可在这里检查运行状态。')}</p>}
  </section>;
}
