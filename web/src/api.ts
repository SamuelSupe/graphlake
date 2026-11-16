import { diagnosticText, t } from './locale.ts';
export type Status = 'unchecked' | 'passed' | 'failed';

export type ModelSummary = {
  id: string;
  name: string;
  sources: number;
  nodes: number;
  edges: number;
  status: Status;
  active_revision: string | null;
  archive_error?: string;
};

export type SavedModel = { id: string; yaml: string; revision: string; active_revision: string | null; status: Status; archive_error?: string };
export type Column = { name: string; data_type: string; nullable: boolean };
export type ValidationSite = { kind: 'table'; table: string; column: string | null }
  | { kind: 'field'; label: string; field: string; index: number | null }
  | { kind: 'input_column'; label: string; input: number; column: string };
export type Validation = { status: Status; check_data: boolean; labels: Record<string, Column[]>; error?: string; location?: ValidationSite };
export type VersionHistory = { active_revision: string | null; offline_at_ms: number | null; versions: {
  revision: string; action: 'adopt' | 'publish' | 'rollback'; at_ms: number;
}[] };
type SourceProbe = {
  status: 'ready' | 'source_unavailable' | 'mapping_invalid';
  sources: Record<string, boolean>;
  location?: ValidationSite | null;
};
export type ModelReadiness = Omit<SourceProbe, 'status'> & {
  status: SourceProbe['status'] | 'worker_unavailable';
  workers?: { worker: number; probe: SourceProbe | null }[] | null;
};
export type FreshnessPolicy = {
  max_observation_age_seconds?: number;
  max_reconciliation_age_seconds?: number;
  require_current_csr?: boolean;
};
export type SourceFreshness = {
  source: string;
  generation: string | null;
  result_generation?: string;
  observed_at_ms: number | null;
  reconciled_at_ms: number | null;
  deletion_capture: 'unknown' | 'reconciliation' | 'marker_and_reconciliation';
  last_status: string | null;
};
export type ModelFreshness = {
  local_tables: SourceFreshness[];
  auto_refresh_csr: boolean;
  csr: { generation: string; built_at_ms: number | null; current: boolean } | null;
  refresh: { status: string; error?: string } | null;
};
export type Listing = { schemas: string[]; tables: string[]; columns: Column[]; truncated: boolean };
export type PathPreview = { row: number; column: string; items: { kind: 'vertex' | 'edge'; label: string; key: string }[] };
export type QueryResult = {
  csr_generation?: string;
  freshness?: SourceFreshness[];
  model_revision: string;
  paths: PathPreview[];
  graph: GraphFragment;
  graph_snapshot?: string;
  graph_next?: number;
  graph_options: Record<string, unknown>;
  columns: Pick<Column, 'name' | 'data_type'>[];
  rows: (string | null)[][];
  sources: { source: string; version: string; consistency: string }[];
  elapsed_ms: number;
  execution_worker?: string;
  truncated: boolean;
  snapshot: string | null;
  snapshot_complete: boolean;
};

export class ApiError extends Error {
  constructor(message: string, readonly code?: string, readonly location?: ValidationSite, readonly status?: number, readonly readiness?: ModelReadiness, readonly response?: { status?: string }) { super(message); }
}

export const authRequired = 'graphlake:auth-required';

export async function request(path: string, options?: RequestInit): Promise<Response> {
  const response = await fetch('/api/v1' + path, {
    ...options,
    headers: { ...(options?.body ? { 'Content-Type': 'application/json' } : {}), ...options?.headers },
  });
  if (!response.ok) {
    const error = await response.json().catch(() => null);
    const code = error?.code ?? (response.status === 401 && !path.startsWith('/auth/') ? 'auth_required' : undefined);
    if (code === 'auth_required') window.dispatchEvent(new Event(authRequired));
    throw new ApiError(error?.message ?? (code === 'auth_required' ? t('请登录 GraphLake 管理后台') : t('请求失败（{0}）', response.status)), code, error?.location, response.status, error?.sources ? error : undefined, error);
  }
  return response;
}

export async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await request(path, options);
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export function downloadFile(href: string, filename: string) {
  const link = document.createElement('a');
  link.href = href;
  link.download = filename;
  link.click();
}

export function downloadText(text: string, filename: string) {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/yaml;charset=utf-8' }));
  downloadFile(url, filename);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function errorText(error: unknown) {
  return error instanceof Error ? diagnosticText(error.message) : t('操作失败');
}

export type GraphIdentity = { kind: 'node' | 'edge'; label: string; key: string[] };
export type GraphElement = GraphIdentity & {
  id: string;
  properties: Record<string, string | null> | null;
  from?: string;
  to?: string;
  directed?: boolean;
  transient?: boolean;
  property_preview?: boolean;
};
export type GraphFragment = { elements: GraphElement[]; truncated: boolean; steps: [string, string][] };
export type QueryGraphPage = { graph: GraphFragment; next: number | null; incomplete: boolean; model_revision: string };
export type GraphComparison = 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'is_null' | 'is_not_null';
export type NeighborFilter = {
  direction: 'both' | 'in' | 'out';
  edge_label?: string;
  properties: { column: string; op: GraphComparison; value?: string }[];
};
export type GraphSchema = { graph: string; nodes: Record<string, { key: string[]; properties: string[] }>;
  edges: Record<string, { key: string[]; properties: string[]; from: { node: string }; to: { node: string } }> };
export type GraphExploration = {
  selected: GraphElement | null;
  graph: GraphFragment;
  sources: QueryResult['sources'];
  next_cursor: string | null;
  page_cursor: string | null;
  previous_cursor: string | null;
  page_number: number;
};


export type ExplorationMatch = GraphIdentity & { id: string; page: number; cursor: string };
export type ExplorationSearch = { matches: ExplorationMatch[]; total: number };
