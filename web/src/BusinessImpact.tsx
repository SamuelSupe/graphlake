import { diagnosticText, t } from './locale';
import { useEffect, useState } from 'react';
import { api, ApiError, errorText, type Evidence, type Finding, type ImpactCase, type ImpactReport } from './api';
import { findingName, subjects, taskName } from './taskSuite';
import { Modal } from './ui';

export type ImpactTarget = { action: 'check' | 'publish' | 'rollback'; revision: string; active_revision: string | null };
export const impactLabels = {
  passed: t('通过'), failed: t('失败'), inconclusive: t('不可判定'), not_covered: t('未覆盖'), not_required: t('无需校验'),
};

export function BusinessImpact({ id, target, busy, onReport, confirm, reloadModel, close }: {
  id: string;
  target: ImpactTarget;
  busy: boolean;
  onReport?: (report: ImpactReport | undefined) => void;
  confirm: (reportId: string) => Promise<void>;
  reloadModel: () => void;
  close: () => void;
}) {
  const { action, revision, active_revision } = target;
  const rollback = action === 'rollback';
  const [report, setReport] = useState<ImpactReport>();
  const [error, setError] = useState<Error>();
  const [attempt, setAttempt] = useState(0);
  const checking = !report && !error;
  const modelChanged = error instanceof ApiError
    && (error.code === 'revision_conflict' || error.code === 'active_revision_changed');

  useEffect(() => {
    const controller = new AbortController();
    api<ImpactReport>('/models/' + id + '/impact' + (action === 'rollback' ? '/rollback' : ''), {
      method: 'POST', signal: controller.signal, body: JSON.stringify({ revision, active_revision }),
    }).then(value => {
      if (controller.signal.aborted) return;
      setReport(value);
      onReport?.(value);
    }).catch(cause => {
      if (!controller.signal.aborted) setError(cause instanceof Error ? cause : new Error(errorText(cause)));
    });
    return () => controller.abort();
  }, [id, action, revision, active_revision, attempt, onReport]);

  function retry() {
    setError(undefined);
    setReport(undefined);
    onReport?.(undefined);
    setAttempt(value => value + 1);
  }

  async function activate() {
    if (!allowed || busy || !report?.report_id) return;
    try { await confirm(report.report_id); }
    catch (cause) {
      setReport(undefined);
      setError(cause instanceof Error ? cause : new Error(errorText(cause)));
    }
  }

  const allowed = Boolean(report?.report_id) && (report?.status === 'passed' || report?.status === 'not_required');
  const affected = Object.entries(report?.mapping_changes ?? {});
  return <Modal title={action === 'check' ? t('业务校验') : rollback ? t('回退前业务影响预览') : t('发布前业务影响预览')} size="wide" close={close}>
    <div className="panel-layout">
      <div className="panel-body form-fields business-preview" aria-busy={checking || busy}>
        <dl className="business-versions">
          <div><dt>{t('当前生效版本')}</dt><dd><code>{active_revision ?? t('首次发布')}</code></dd></div>
          <div><dt>{action === 'rollback' ? t('回退目标版本') : t('草稿版本')}</dt><dd><code>{revision}</code></dd></div>
        </dl>
        {checking && <p role="status">{t('正在校验固定场景…')}</p>}
        {error && <p className="notice error" role="alert">{errorText(error)}</p>}
        {report && <>
          <p role="status" className={'notice' + (allowed ? '' : ' error')}>{t('业务校验 · {0}', impactLabels[report.status])}</p>
          {affected.length > 0 && <details className="form-disclosure">
            <summary>{t('变更的映射（{0}）', affected.length)}</summary>
            <ul>{affected.map(([label, fields]) => <li key={label}><code>{label}</code> · {fields.join('、')}</li>)}</ul>
          </details>}
          {report.cases.length > 0 && <section className="form-fields" aria-label={t('逐场景结果')}>
            <h3>{t('逐场景结果')}</h3>
            {report.cases.map(result => <Case key={result.id} result={result} />)}
          </section>}
        </>}
      </div>
      <footer className="panel-footer">
        <button type="button" className="button" data-initial-focus onClick={close}>{action === 'check' ? t('关闭') : t('取消')}</button>
        {!checking && (modelChanged
          ? <button type="button" className="button" disabled={busy} onClick={reloadModel}>{t('重新加载模型')}</button>
          : <button type="button" className="button" disabled={busy} onClick={retry}>{t('重新预览')}</button>)}
        {action !== 'check' && <button type="button" className="button primary" disabled={!allowed || busy} onClick={() => void activate()}>
          {rollback ? busy ? t('正在回退…') : t('确认回退') : busy ? t('正在发布…') : t('确认发布')}
        </button>}
      </footer>
    </div>
  </Modal>;
}

function Case({ result }: { result: ImpactCase }) {
  const same = !result.added.length && !result.removed.length && !result.changed.length
    && JSON.stringify(result.baseline_anchor) === JSON.stringify(result.candidate_anchor)
    && result.baseline_subject === result.candidate_subject;
  return <details className="form-disclosure business-case">
    <summary><span>{result.id}</span><span className={'business-case-status ' + result.status}>{impactLabels[result.status]}</span></summary>
    <div className="form-fields business-case-fields">
      <p className="muted small">{taskName(result.task)} · {t('租户')} <code>{result.tenant_id}</code></p>
      {result.issue && <p className="notice error">{diagnosticText(result.issue)}</p>}
      <div className="business-comparison">
        <Outcome label={t('生效版本')} subject={result.baseline_subject} matches={result.baseline_matches}
          count={result.baseline_findings} evidence={result.baseline_anchor} />
        <Outcome label={t('候选版本')} subject={result.candidate_subject} matches={result.candidate_matches}
          count={result.candidate_findings} evidence={result.candidate_anchor} />
      </div>
      {same && !result.issue && <p className="muted small">{t('结果与证据无变化。')}</p>}
      <Findings label={t('新增结果')} findings={result.added} />
      <Findings label={t('移除结果')} findings={result.removed} />
      {result.changed.length > 0 && <section className="form-fields">
        <h4>{t('结果或证据变化（{0}）', result.changed.length)}</h4>
        {result.changed.map(([before, after], index) => <div className="business-comparison" key={index}>
          <Findings label={t('变更前')} findings={[before]} />
          <Findings label={t('变更后')} findings={[after]} />
        </div>)}
      </section>}
      {result.expected_on_mismatch && <details className="form-disclosure">
        <summary>{t('预期结果')}</summary>
        <Outcome label={t('固定场景预期')} subject={result.expected_on_mismatch.subject} matches={null}
          count={result.expected_on_mismatch.findings.length} evidence={result.expected_on_mismatch.anchor} />
        <Findings label={t('预期发现')} findings={result.expected_on_mismatch.findings} />
      </details>}
      <details className="form-disclosure"><summary>{t('运行参数')}</summary><pre className="business-parameters">{JSON.stringify(result.parameters, null, 2)}</pre></details>
    </div>
  </details>;
}

function Outcome({ label, subject, matches, count, evidence }: {
  label: string; subject: string | null; matches: boolean | null; count: number | null; evidence: Evidence | null;
}) {
  return <section className="business-outcome">
    <h4>{label}</h4>
    <p className="small">{subject ? subjects[subject] ?? subject : t('无可用结果')}
      {count !== null && t(' · {0} 条发现', count)}
      {matches !== null && (matches ? t(' · 符合预期') : t(' · 与预期不符'))}</p>
    {evidence && <EvidenceDetails evidence={evidence} />}
  </section>;
}

function Findings({ label, findings }: { label: string; findings: Finding[] }) {
  if (!findings.length) return null;
  return <section className="form-fields">
    <h4>{t('{0}（{1}）', label, findings.length)}</h4>
    <ol className="business-findings">
      {findings.map((finding, index) => <li key={index}>
        <strong>{findingName(finding.kind)}</strong>
        <dl>{Object.entries(finding.values).map(([name, value]) => <div key={name}><dt>{name}</dt><dd>{value ?? '—'}</dd></div>)}</dl>
        {finding.evidence.map((evidence, position) => <EvidenceDetails key={position} evidence={evidence} />)}
      </li>)}
    </ol>
  </section>;
}

function EvidenceDetails({ evidence }: { evidence: Evidence }) {
  return <dl className="business-evidence">
    <div><dt>{t('证据标签')}</dt><dd>{evidence.label}</dd></div>
    <div><dt>{t('来源系统')}</dt><dd>{evidence.source_system ?? '—'}</dd></div>
    <div><dt>{t('记录键')}</dt><dd><code>{evidence.tenant_id} / {evidence.record_id ?? '—'}</code></dd></div>
    <div><dt>{t('观测时间（ms）')}</dt><dd>{evidence.observed_at_ms ?? '—'}</dd></div>
    <div><dt>{t('来源版本')}</dt><dd>{evidence.source_version ?? '—'}</dd></div>
  </dl>;
}
