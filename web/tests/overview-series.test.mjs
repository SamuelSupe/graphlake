import assert from 'node:assert/strict';
import { test } from 'node:test';
import { requestTrend, trendsInWindow } from '../src/overviewSeries.ts';

function sample(at, uptime, requests, serverErrors) {
  return { at, runtime: { uptime, requests, serverErrors, active: 0, waiting: 0 } };
}

test('overview normalizes HTTP counters by each actual sampling interval', () => {
  const samples = [sample(1000, 10, 100, 3), sample(16_000, 25, 109, 4), sample(46_000, 55, 129, 6)];
  assert.deepEqual(requestTrend(samples).map(point => point.values), [null, [36, 4], [40, 4]]);
});

test('overview leaves gaps for failed scrapes, paused sampling, and process replacement', () => {
  const samples = [
    sample(1000, 10, 100, 3),
    { at: 31_000 },
    sample(61_000, 70, 129, 6),
    sample(151_000, 160, 149, 7),
    sample(181_000, 26, 220, 8),
    sample(211_000, 56, 250, 9),
  ];
  assert.deepEqual(requestTrend(samples).map(point => point.values), [null, null, null, null, null, [60, 2]]);
});

test('shortening the chart window keeps the baseline for its first visible request rate', () => {
  const samples = Array.from({ length: 13 }, (_, i) => sample(i * 30_000, 10 + i * 30, 100 + i * 5, 0));
  const trends = trendsInWindow(samples, 5);
  assert.equal(trends.query[0].at, 60_000);
  assert.deepEqual(trends.requests[0], { at: 60_000, values: [10, 0] });
});
