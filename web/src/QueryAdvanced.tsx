import { useId, useState, type FormEvent } from 'react';
import { t } from './locale.ts';
import type { FreshnessPolicy } from './api';
import { queryLimitErrors } from './queryParameters';
import { Modal } from './ui';

const ageFields = [
  ['max_observation_age_seconds', t('来源观察最大年龄（秒）')],
  ['max_reconciliation_age_seconds', t('全量对账最大年龄（秒）')],
] as const;

export default function QueryAdvanced({
  language,
  timeout,
  defaultTimeout,
  canExport,
  captureResult,
  freshness,
  recoverable,
  generation,
  running,
  onSelectSnapshot,
  onClearSnapshot,
  apply,
  close,
}: {
  language: 'cypher' | 'sql' | 'gremlin';
  timeout: string;
  defaultTimeout?: number;
  canExport: boolean;
  captureResult: boolean;
  freshness: FreshnessPolicy;
  recoverable: boolean;
  generation?: string;
  running: boolean;
  onSelectSnapshot: () => Promise<void>;
  onClearSnapshot: () => void;
  apply: (next: { timeout: string; captureResult: boolean; freshness: FreshnessPolicy }) => void;
  close: () => void;
}) {
  const formId = useId();
  const [draftTimeout, setDraftTimeout] = useState(timeout);
  const [draftCapture, setDraftCapture] = useState(captureResult);
  const [draftFreshness, setDraftFreshness] = useState<FreshnessPolicy>({ ...freshness });
  const [selectingCsr, setSelectingCsr] = useState(false);

  const limits = queryLimitErrors(draftTimeout, draftFreshness);
  const timeoutValid = !limits.timeout;
  const agesValid = !limits.max_observation_age_seconds && !limits.max_reconciliation_age_seconds;
  const hasAny = Boolean(
    draftTimeout.trim()
    || draftCapture
    || generation
    || draftFreshness.max_observation_age_seconds !== undefined
    || draftFreshness.max_reconciliation_age_seconds !== undefined
    || (language === 'cypher' && draftFreshness.require_current_csr),
  );

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!timeoutValid || !agesValid) return;
    apply({
      timeout: draftTimeout.trim(),
      captureResult: draftCapture,
      freshness: {
        max_observation_age_seconds: draftFreshness.max_observation_age_seconds,
        max_reconciliation_age_seconds: draftFreshness.max_reconciliation_age_seconds,
        require_current_csr: language === 'cypher' ? draftFreshness.require_current_csr : undefined,
      },
    });
    close();
  }

  function resetAll() {
    setDraftTimeout('');
    setDraftCapture(false);
    setDraftFreshness({});
    if (generation) onClearSnapshot();
  }

  return <Modal layout="center" size="medium" title={t('高级执行选项')} description={t('调整单次查询超时、完整结果导出及数据新鲜度策略。')} close={close}>
    <form className="panel-layout" onSubmit={submit}>
      <div className="panel-body dialog-form">
        <fieldset className="form-fields mapping-options">
          <legend>{t('执行与导出')}</legend>
          <label className="query-timeout">{t('超时（毫秒）')}
            <input type="number" min={1} max={60_000} step={1} value={draftTimeout}
              data-initial-focus disabled={running}
              aria-invalid={!timeoutValid} aria-describedby={timeoutValid ? undefined : formId + '-timeout-error'}
              placeholder={defaultTimeout ? String(defaultTimeout) : t('服务器默认')}
              onChange={event => setDraftTimeout(event.target.value)} />
          </label>
          {!timeoutValid && <p className="inline-error" id={formId + '-timeout-error'} role="alert">{t('请输入 1 至 60000 毫秒的整数。')}</p>}
          {canExport && <label className="check-label">
            <input type="checkbox" checked={draftCapture} onChange={event => setDraftCapture(event.target.checked)} />
            {t('保留完整结果')}
          </label>}
        </fieldset>

        <fieldset className="form-fields mapping-options">
          <legend>{t('数据新鲜度与 CSR')}</legend>
          <div className="settings-grid">
            {ageFields.map(([key, label]) => <label key={key}>{label}
              <input type="number" min={1} max={2592000}
                placeholder={t('模型默认')} value={draftFreshness[key] ?? ''}
                aria-invalid={limits[key] || undefined}
                aria-describedby={limits[key] ? formId + '-freshness-error' : undefined}
                onChange={event => {
                  const age = event.target.value ? event.target.valueAsNumber : undefined;
                  setDraftFreshness(old => ({ ...old, [key]: age }));
                }} />
            </label>)}
          </div>
          {!agesValid && <p className="inline-error" id={formId + '-freshness-error'} role="alert">
            {t('来源观察和全量对账的最大年龄须为 1 至 2592000 秒的整数。')}
          </p>}
          {language === 'cypher' && <label className="check-label">
            <input type="checkbox" checked={!!draftFreshness.require_current_csr}
              onChange={event => setDraftFreshness(old => ({ ...old, require_current_csr: event.target.checked }))} />
            {t('要求 CSR 与当前副本一致')}
          </label>}
          {recoverable && language === 'cypher' && <div className="settings-actions">
            {generation ? <>
              <button type="button" className="button compact" disabled={running} onClick={onClearSnapshot}>{t('切换至实时来源')}</button>
              <code className="version-chip" title={generation}>CSR {generation.slice(0, 10)}</code>
            </> : <button type="button" className="button compact" disabled={running || selectingCsr} onClick={async () => {
              setSelectingCsr(true);
              try { await onSelectSnapshot(); } finally { setSelectingCsr(false); }
            }}>{t('使用已发布 CSR')}</button>}
          </div>}
        </fieldset>
      </div>
      <footer className="panel-footer">
        <button type="button" className="text-button scope-mode" disabled={!hasAny || running} onClick={resetAll}>{t('重置默认')}</button>
        <button type="button" className="button" onClick={close}>{t('取消')}</button>
        <button type="submit" className="button primary" disabled={!timeoutValid || !agesValid}>{t('应用')}</button>
      </footer>
    </form>
  </Modal>;
}
