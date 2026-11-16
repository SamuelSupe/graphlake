import { t } from './locale.ts';
import type { McpModel } from './mcp';

export default function McpScope({ models, targets }: { models: McpModel[]; targets: string[] }) {
  return <>
    {models.length ? <ul className="mcp-models" aria-label={t('账号可用模型')}>{models.map(model => <li key={model.id}>
      <div className="mcp-model-name"><strong>{model.name}</strong><code>{model.id}</code></div>
      <div className="mcp-model-access">
        {model.capabilities.query && <span className="tag">{t('查询')}</span>}
        {model.capabilities.read_model && <span className="tag">{t('读取草稿')}</span>}
        {model.capabilities.modeling && <span className="tag">{t('建模')}</span>}
        {model.capabilities.publish && <span className="tag">{t('发布')}</span>}
        {model.archive_error ? <span className="settings-hint">{t('版本存储异常，暂不可查询')}</span> : !model.active_revision && <span className="settings-hint">{t('未发布，暂不可查询')}</span>}
      </div>
    </li>)}</ul> : <p className="notice">{t('暂无可用模型。需要管理员分配查询、建模或发布权限，并启用对应 MCP 能力；查询还需要已发布的模型。')}</p>}
    {!!targets.length && <div><h3>{t('可新建的模型标识')}</h3><p className="settings-hint">{t('这些标识已预授权；新建时还需选择具有建模权限的已有模板。')}</p><div className="mcp-model-access">{targets.map(id => <code className="tag" key={id}>{id}</code>)}</div></div>}
  </>;
}
