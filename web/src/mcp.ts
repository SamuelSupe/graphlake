import type { ModelSummary } from './api';

export type McpConfig = { enabled: boolean; query: boolean; modeling: boolean; publish: boolean; max_rows: number; timeout_ms: number };
export type McpSnapshot = { config: McpConfig; revision: string; endpoint: string; protocol_versions: string[]; server_timeout_ms: number };
export type McpModel = ModelSummary & { capabilities: { query: boolean; read_model: boolean; modeling: boolean; publish: boolean } };
export type McpInspection = { owner: string; protocolVersion: string; tools: { name: string; description: string }[]; models: McpModel[]; modelingTargets: string[] };
export const mcpServices = ['query', 'modeling'] as const;
export type McpService = typeof mcpServices[number];
export const mcpServiceLabels: Record<McpService, string> = { query: '查询 MCP', modeling: '建模 MCP' };
type McpServiceStatus = { endpoint: string; enabled: boolean; allowed: boolean };

export type McpAccess = McpSnapshot & {
  owner: string; can_issue: boolean; services: Record<McpService, McpServiceStatus>;
  scope: { models: McpModel[]; modeling_targets: string[] };
};

const versions = ['2025-11-25', '2025-06-18', '2025-03-26'];

// Keep MCP credentials separate from the browser session, including on redirects.
export async function inspectMcp(service: McpService, token: string, signal: AbortSignal, progress: (message: string) => void): Promise<McpInspection> {
  let protocol: string | undefined;
  async function rpc(id: number | undefined, method: string, params?: object) {
    const response = await fetch('/mcp/' + service, {
      method: 'POST', credentials: 'omit', redirect: 'error', signal,
      headers: {
        Authorization: 'Bearer ' + token, 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream',
        ...(protocol ? { 'MCP-Protocol-Version': protocol } : {}),
      },
      body: JSON.stringify({ jsonrpc: '2.0', id, method, params }),
    });
    if (id === undefined && response.status === 202) return;
    const body = await response.json().catch(() => null);
    if (!response.ok) {
      if (response.status === 401) throw new Error('AI 令牌无效或已撤销，或所属账号已停用。请在 AI 页面检查个人令牌。');
      if (response.status === 403) throw new Error('连接被拒绝，请检查当前用户的模型权限和访问地址。');
      if (response.status === 404) throw new Error('MCP 服务未启用，请启用并保存配置后重试。');
      throw new Error(body?.error?.message ?? body?.message ?? '连接失败（HTTP ' + response.status + '），请检查服务和反向代理。');
    }
    if (id === undefined || body?.jsonrpc !== '2.0' || body.id !== id) throw new Error('服务返回了无效的 MCP 响应，请检查服务和反向代理。');
    if (body.error) throw new Error(body.error.message ?? 'MCP 请求失败');
    if (!body.result || typeof body.result !== 'object') throw new Error('MCP 响应缺少结果。');
    return body.result;
  }
  progress('正在验证令牌与协议…');
  const initialized = await rpc(1, 'initialize', { protocolVersion: versions[0], capabilities: {}, clientInfo: { name: 'GraphLake connection test', version: '1.0' } });
  if (!versions.includes(initialized.protocolVersion)) throw new Error('服务返回了不支持的 MCP 协议版本。');
  protocol = initialized.protocolVersion;
  await rpc(undefined, 'notifications/initialized');
  progress('正在读取可用工具与模型权限…');
  const [catalog, listing] = await Promise.all([
    rpc(2, 'tools/list', {}),
    rpc(3, 'tools/call', { name: 'list_models', arguments: {} }),
  ]);
  if (listing.isError) throw new Error(listing.structuredContent?.message ?? '已连接，但读取模型权限失败。');
  const scope = listing.structuredContent;
  if (!Array.isArray(catalog.tools) || !catalog.tools.every((tool: { name?: unknown; description?: unknown }) => typeof tool?.name === 'string' && typeof tool.description === 'string')
    || typeof scope?.owner !== 'string' || !Array.isArray(scope?.models) || !Array.isArray(scope?.modeling_targets)
    || !scope.models.every((model: McpModel) => typeof model?.id === 'string' && typeof model.name === 'string'
      && ['query', 'read_model', 'modeling', 'publish'].every(key => typeof model.capabilities?.[key as keyof McpModel['capabilities']] === 'boolean'))
    || !scope.modeling_targets.every((id: unknown) => typeof id === 'string')) {
    throw new Error('模型权限响应不完整，请确认服务与页面版本一致。');
  }
  return { owner: scope.owner, protocolVersion: protocol!, tools: catalog.tools, models: scope.models, modelingTargets: scope.modeling_targets };
}
