export const historyWindowMs = 15 * 60_000;
export const maxSampleGapMs = 60_000;

export type RuntimeSample = {
  at: number;
  runtime?: { uptime: number; active: number; waiting: number; requests: number; serverErrors: number };
};
export type ChartPoint = { at: number; values: [number, number] | null };

export function queryTrend(samples: RuntimeSample[]): ChartPoint[] {
  return samples.map(({ at, runtime }) => ({ at, values: runtime ? [runtime.active, runtime.waiting] : null }));
}

export function requestTrend(samples: RuntimeSample[]): ChartPoint[] {
  return samples.map(({ at, runtime }, index) => {
    const previous = samples[index - 1];
    if (!runtime || !previous?.runtime) return { at, values: null };
    const elapsed = at - previous.at;
    const requests = runtime.requests - previous.runtime.requests;
    const errors = runtime.serverErrors - previous.runtime.serverErrors;
    // A different process can start with larger counters, so compare its uptime too.
    const uptimeDelta = (runtime.uptime - previous.runtime.uptime) * 1000;
    if (elapsed <= 0 || elapsed > maxSampleGapMs || requests < 0 || errors < 0 || errors > requests
      || Math.abs(uptimeDelta - elapsed) > 3000) return { at, values: null };
    return { at, values: [requests * 60_000 / elapsed, errors * 60_000 / elapsed] };
  });
}

export function trendsInWindow(samples: RuntimeSample[], minutes: number) {
  const cutoff = (samples.at(-1)?.at ?? 0) - minutes * 60_000;
  return {
    query: queryTrend(samples.filter(sample => sample.at >= cutoff)),
    requests: requestTrend(samples).filter(point => point.at >= cutoff),
  };
}
