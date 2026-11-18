import { t } from './locale.ts';
import { Check, ArrowRight, ChevronDown } from 'lucide-react';
import type { Model, Section, Selection } from './model';
import type { SetupIssue } from './modelWorkflow';

export function ModelSetup({ model, issue, dirty, busy, canPublish, configure, publishAllowed, add, select, save, publish }: {
  configure: boolean; publishAllowed: boolean;
  model: Model; issue?: SetupIssue; dirty: boolean; busy: boolean; canPublish: boolean;
  add: (section: Section) => void; select: (selection: Selection) => void; save: () => void; publish: () => void;
}) {
  const sources = Object.keys(model.sources).length;
  const nodes = Object.keys(model.nodes).length;
  const step = !sources ? 0 : issue ? 1 : dirty ? 2 : 3;
  const labels = [t('添加数据源'), t('定义节点与边'), t('保存草稿'), t('预览并发布')];
  return <section className="model-setup" aria-label={t('建模指引')}>
    <div className="setup-next">
      <details className="setup-progress">
        <summary><span className="sr-only">{t('建模步骤')} </span><span className="setup-fraction">{step + 1} / 4</span>{labels[step]}<ChevronDown size={14} aria-hidden="true" /></summary>
        <ol className="setup-steps" aria-label={t('建模步骤')}>
          {labels.map((label, index) => <li key={label} aria-current={index === step ? 'step' : undefined}>
            <span className="setup-number">{index < step ? <Check size={13} aria-hidden="true" /> : index + 1}</span>{label}
          </li>)}
        </ol>
      </details>
      <div className="setup-guidance"><strong>{issue ? t('下一步') : dirty ? t('映射已填写') : t('草稿已保存')}</strong>
        <p>{issue?.add === 'sources' && !configure ? t('请管理员添加数据源，之后可继续定义节点映射。') : issue?.message ?? (dirty ? t('保存后可校验数据并发布；只有节点的模型也可以查询。') : publishAllowed ? t('发布时会校验当前数据，成功后即可查询。') : t('请具有发布权限的成员校验并发布此草稿。'))}</p></div>
      <div className="actions">
        {sources > 0 && nodes > 0 && !Object.keys(model.edges).length && <button className="text-button" disabled={busy} onClick={() => add('edges')}>{t('添加边（可选）')}</button>}
        {issue ? <button className="button compact" disabled={busy || issue.add === 'sources' && !configure} onClick={() => issue.selection ? select(issue.selection) : add(issue.add!)}>
          {issue.selection ? t('继续配置') : issue.add === 'sources' ? t('添加数据源') : t('添加节点')}<ArrowRight size={15} aria-hidden="true" />
        </button> : dirty ? <button className="button compact" disabled={busy} onClick={save}>{t('保存草稿')}</button>
          : <button className="button compact" disabled={!canPublish} onClick={publish}>{t('预览并发布')}</button>}
      </div>
    </div>
  </section>;
}
