export type Gate = { manifest_sha256: string; approved: boolean; ready: boolean; restored_backup?: string };
export type Phase = 'validate' | 'parquet' | 'postgres' | 'control';
export type RestoreJob = { bundle: string; phase: Phase; running: boolean; bound: boolean; failure: string | null };
export type RestoreStatus = { mode: 'restore'; job: RestoreJob | null };
export type RecoveryStatus = Gate | RestoreStatus;

export async function recovery<T>(action: 'status' | 'check' | 'verify' | 'backups' | 'import', token: string, options?: RequestInit): Promise<T> {
  const response = await fetch('/recovery/' + action, {
    method: action === 'status' || action === 'backups' ? 'GET' : 'POST',
    cache: 'no-store', credentials: 'omit', ...options,
    headers: { 'X-GraphLake-Recovery-Token': token, ...(options?.body ? { 'Content-Type': 'application/json' } : {}) },
  });
  if (response.status === 202) return undefined as T;
  const result = await response.json().catch(() => null);
  if (!response.ok) throw new Error(result?.message && response.status !== 404 ? result.message
    : response.status === 404 ? '无法读取恢复状态，请核对目标站和恢复令牌。'
    : '恢复服务暂不可用（' + response.status + '）');
  if (!result) throw new Error('目标站返回了无法识别的恢复响应。');
  return result as T;
}
