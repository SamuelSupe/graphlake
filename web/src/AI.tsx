import { t } from './locale.ts';
import { useCallback, useEffect, useState } from 'react';
import { Check, Copy } from 'lucide-react';
import { api, errorText } from './api';
import AiToken from './AiToken';
import McpConnection from './McpConnection';
import McpScope from './McpScope';
import { mcpServices, type McpAccess } from './mcp';

const examples = [
  { title: t('查询数据'), capability: 'query' as const, prompt: t('列出我可查询的模型，查看模型结构，再统计各类节点数量。') },
  { title: t('构建模型'), capability: 'modeling' as const, prompt: t('读取我有建模权限的模型，按我的需求修改并校验。先保存草稿，发布前让我确认。') },
];

export default function AI({ locked, authEnabled, openServiceSettings }: {
  locked: boolean; authEnabled: boolean; openServiceSettings?: () => void;
}) {
  const [access, setAccess] = useState<McpAccess>();
  const [error, setError] = useState('');
  const [attempt, reload] = useState(0);
  const [tokenUpdate, setTokenUpdate] = useState<{ value: string }>();
  const [copied, setCopied] = useState('');
  const [copyError, setCopyError] = useState('');
  const changed = useCallback(() => reload(value => value + 1), []);
  useEffect(() => {
    if (locked) { setTokenUpdate(undefined); return; }
    const controller = new AbortController();
    api<McpAccess>('/mcp/access', { signal: controller.signal }).then(value => {
      if (!controller.signal.aborted) { setAccess(value); setError(''); }
    }, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [locked, attempt]);
  const serviceOff = access && mcpServices.every(service => !access.services[service].enabled);
  const availableExamples = serviceOff ? [] : examples.filter(example => access?.scope.models.some(model => model.capabilities[example.capability]));

  async function copyExample(title: string, prompt: string) {
    setCopyError('');
    try { await navigator.clipboard.writeText(prompt); setCopied(title); }
    catch { setCopyError(t('无法访问剪贴板，请选中示例文字手动复制。')); }
  }

  return <main className="workbench">
    <header className="workspace-header"><div className="workspace-title"><h1>AI</h1><span className="settings-hint">{t('Agent 接入指南')}</span></div>
      {access && <div className="workspace-context"><span>{t('当前用户 · {0}', access.owner)}</span></div>}
    </header>
    <div className="mcp-guide">
      <section className="mcp-intro" aria-labelledby="ai-guide-title"><h2 id="ai-guide-title">{t('把 GraphLake 连接到 Agent')}</h2><p>{t('通过 MCP 查询数据、构建模型。AI 的权限与你的账号保持一致。')}</p></section>
      {serviceOff && access.account_allowed && <div className="notice mcp-service-notice">
        <p>{openServiceSettings ? t('请在运行配置中启用 MCP 服务。') : t('MCP 服务尚未启用，请联系管理员开启。')}</p>
        {openServiceSettings && <button type="button" className="button compact" disabled={locked} onClick={openServiceSettings}>{t('前往 MCP 配置')}</button>}
      </div>}
      {error && <p className="notice error" role="alert">{error} <button type="button" className="text-button" onClick={changed}>{t('重试')}</button></p>}
      {!access ? !error && <p role="status">{t('正在读取当前用户的 AI 权限…')}</p> : !access.account_allowed
        ? <p className="notice">{t('当前账号未获准接入 MCP，请联系管理员。')}</p> : <>
        <div className="mcp-setup">
          <AiToken locked={locked} authEnabled={authEnabled} canIssue={access.can_issue} token={tokenUpdate?.value ?? ''}
            issued={value => setTokenUpdate({ value })} />
          <McpConnection snapshot={access} locked={locked} tokenUpdate={tokenUpdate} />
          <section className="mcp-step" aria-labelledby="ai-examples-title">
            <div className="mcp-step-title"><span className="mcp-step-number" aria-hidden="true">3</span><h3 id="ai-examples-title">{t('开始使用')}</h3></div>
            <div className="mcp-step-body"><p className="settings-hint">{availableExamples.length ? t('连接成功后，试着把下面的指令发给 Agent。') : serviceOff ? t('服务启用后，这里会显示你可用的示例。') : t('暂无可用的查询或建模示例，可在下方「我的权限」查看当前能力。')}</p>
              <div className="mcp-examples">{availableExamples.map(example => <div className="mcp-example" key={example.title}>
                <div><h4>{example.title}</h4><p>{example.prompt}</p></div>
                <button type="button" className="button compact" aria-label={t('复制{0}示例', example.title)} onClick={() => void copyExample(example.title, example.prompt)}>{copied === example.title ? <Check size={15} aria-hidden="true" /> : <Copy size={15} aria-hidden="true" />}{copied === example.title ? t('已复制') : t('复制')}</button>
              </div>)}</div>
              <span className="sr-only" role="status">{copied && t('{0}示例已复制', copied)}</span>
              {copyError && <p className="notice error" role="alert">{copyError}</p>}
            </div>
          </section>
        </div>
        <details className="mcp-disclosure"><summary>{t('我的权限')}<span>{access.scope.models.length}{t(' 个可用模型{0}', access.scope.modeling_targets.length ? t(' · {0} 个可新建标识', access.scope.modeling_targets.length) : '')}</span></summary>
          <div className="mcp-disclosure-body"><div className="settings-section-title"><p className="settings-hint">{t('自动继承你的模型角色和租户范围，权限变更后生效。')}</p><button type="button" className="button compact" disabled={locked} onClick={changed}>{t('刷新权限')}</button></div><McpScope models={access.scope.models} targets={access.scope.modeling_targets} /><p className="settings-hint">{t('查询受全局行数和超时限制；发布需要发布权限。个人令牌仅可访问 MCP。')}</p></div>
        </details>
      </>}
    </div>
  </main>;
}
