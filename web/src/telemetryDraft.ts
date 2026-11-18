export type TelemetryConfig = {
  enabled: boolean; endpoint: string | null; service_name: string; instance_id: string | null;
  log_format: 'text' | 'json'; metrics: boolean; logs: boolean; traces: boolean;
  trace_sample_ratio: number; export_interval_ms: number; timeout_ms: number;
  allow_insecure_http: boolean; include_audit_identity: boolean; headers_env: Record<string, string>;
};

type HeaderField = { name: string; variable: string };

export function telemetryHeaderMap(headers: HeaderField[]) {
  return new Map(headers.map(({ name, variable }) => [name.trim().toLowerCase(), variable.trim()]));
}

export function telemetryDraftChanged(saved: TelemetryConfig, form: TelemetryConfig, headers: HeaderField[]) {
  if ((Object.keys(form) as (keyof TelemetryConfig)[]).some(key => key !== 'headers_env' && form[key] !== saved[key])) return true;
  const original = Object.entries(saved.headers_env);
  if (headers.length !== original.length) return true;
  const current = telemetryHeaderMap(headers);
  return original.some(([name, variable]) => current.get(name.toLowerCase()) !== variable);
}
