import { api, errorText, type ModelSummary } from './api';
import type { BackupSnapshot } from './BackupStatus';
import type { SettingsSnapshot } from './Settings';

export type Readiness = {
  status: string;
  published_models?: number;
  unavailable_models?: number;
};
type Health = { version: string; build: string | null };
type Resource<T> = { data: T; error?: never } | { error: string; data?: never };

async function capture<T>(read: Promise<T>): Promise<Resource<T>> {
  try { return { data: await read }; }
  catch (cause) { return { error: errorText(cause) }; }
}

async function probe(path: string, signal: AbortSignal, degraded = false) {
  const response = await fetch(path, { signal, cache: 'no-store' });
  // /readyz supplies a reason even when it replies with HTTP 503.
  if (!response.ok && !(degraded && response.status === 503)) {
    throw new Error('请求失败（HTTP ' + response.status + '）');
  }
  return response;
}

async function metrics(signal: AbortSignal) {
  const response = await probe('/metrics', signal);
  const lines = (await response.text()).split(/\r?\n/);
  const series = new Map<string, number>();
  for (const line of lines) {
    const sample = /^(\S+)[ \t]+(\S+)/.exec(line);
    if (sample && !sample[1].startsWith('#')) series.set(sample[1], Number(sample[2]));
  }
  function value(name: string) {
    const sample = series.get('graphlake_' + name);
    if (sample === undefined || !Number.isFinite(sample) || sample < 0) {
      throw new Error('服务未返回完整的运行指标');
    }
    return sample;
  }
  const responses = [1, 2, 3, 4, 5].map(code => value('http_requests_total{status_class="' + code + 'xx"}'));
  return {
    at: Date.now(),
    uptime: value('process_uptime_seconds'),
    slots: value('query_slots'),
    active: value('active_queries'),
    waiting: value('waiting_queries'),
    requests: responses.reduce((total, count) => total + count, 0),
    serverErrors: responses[4],
    auditFailures: value('audit_write_failures_total'),
  };
}

export async function readOverview(signal: AbortSignal) {
  const options = { signal };
  const [health, readiness, runtime, models, telemetry, backups] = await Promise.all([
    capture<Health>(probe('/healthz', signal).then(response => response.json())),
    capture<Readiness>(probe('/readyz', signal, true).then(response => response.json())),
    capture(metrics(signal)),
    capture(api<ModelSummary[]>('/models', options)),
    capture(api<SettingsSnapshot>('/settings', options)),
    capture(api<BackupSnapshot>('/backups/status', options)),
  ]);
  return { health, readiness, runtime, models, telemetry, backups, at: Date.now() };
}

export type OverviewSnapshot = Awaited<ReturnType<typeof readOverview>>;
