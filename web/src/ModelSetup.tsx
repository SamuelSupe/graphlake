import { Check, ArrowRight } from 'lucide-react';
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
  return <section className="model-setup" aria-label="建模指引">
    <ol className="setup-steps" aria-label="建模步骤">
      {['添加数据源', '定义节点与边', '保存草稿', '校验并发布'].map((label, index) => <li key={label} aria-current={index === step ? 'step' : undefined}>
        <span className="setup-number">{index < step ? <Check size={13} aria-hidden="true" /> : index + 1}</span>{label}
      </li>)}
    </ol>
    <div className="setup-next">
      <div><strong>{issue ? '下一步' : dirty ? '映射已填写' : '草稿已保存'}</strong>
        <p>{issue?.add === 'sources' && !configure ? '请管理员添加数据源，之后可继续定义节点映射。' : issue?.message ?? (dirty ? '保存后可校验数据并发布；只有节点的模型也可以查询。' : publishAllowed ? '发布时会校验当前数据，成功后即可查询。' : '请具有发布权限的成员校验并发布此草稿。')}</p></div>
      <div className="actions">
        {sources > 0 && nodes > 0 && !Object.keys(model.edges).length && <button className="text-button" disabled={busy} onClick={() => add('edges')}>添加边（可选）</button>}
        {issue ? <button className="button compact" disabled={busy || issue.add === 'sources' && !configure} onClick={() => issue.selection ? select(issue.selection) : add(issue.add!)}>
          {issue.selection ? '继续配置' : issue.add === 'sources' ? '添加数据源' : '添加节点'}<ArrowRight size={15} aria-hidden="true" />
        </button> : dirty ? <button className="button compact" disabled={busy} onClick={save}>保存草稿</button>
          : <button className="button compact" disabled={!canPublish} onClick={publish}>校验并发布</button>}
      </div>
    </div>
  </section>;
}
