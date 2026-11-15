import assert from 'node:assert/strict';
import { test } from 'node:test';
import { inspectMcp } from '../src/mcp.ts';

const scope = {
  owner: 'analyst',
  models: [{ id: 'graph', name: 'Graph', active_revision: 'r1', capabilities: { query: true, read_model: false, modeling: false, publish: false } }],
  modeling_targets: [],
};

function server(t, overrides = {}) {
  const calls = [];
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    const message = JSON.parse(options.body);
    calls.push({ url, options, message });
    if (overrides[message.method]) return overrides[message.method](message, options);
    if (message.method === 'notifications/initialized') return new Response(null, { status: 202 });
    const result = {
      initialize: { protocolVersion: '2025-06-18' },
      'tools/list': { tools: [{ name: 'list_models', description: 'Visible models' }, { name: 'query', description: 'Read-only query' }] },
      'tools/call': { isError: false, structuredContent: scope },
    }[message.method];
    return Response.json({ jsonrpc: '2.0', id: message.id, result });
  });
  return calls;
}

test('connection inspection negotiates the protocol and reads real model scope without administrator cookies', async t => {
  const calls = server(t);
  const progress = [];
  const signal = new AbortController().signal;
  const result = await inspectMcp('query', 'disposable-test-token', signal, message => progress.push(message));
  assert.deepEqual(calls.map(call => call.message.method), ['initialize', 'notifications/initialized', 'tools/list', 'tools/call']);
  assert.equal(calls[0].options.headers['MCP-Protocol-Version'], undefined);
  assert.equal(calls[1].message.id, undefined);
  for (const { url, options } of calls) {
    assert.equal(url, '/mcp/query');
    assert.equal(options.credentials, 'omit');
    assert.equal(options.redirect, 'error');
    assert.equal(options.signal, signal);
    assert.equal(options.headers.Authorization, 'Bearer disposable-test-token');
  }
  for (const { options } of calls.slice(1)) assert.equal(options.headers['MCP-Protocol-Version'], '2025-06-18');
  assert.deepEqual(calls[3].message.params, { name: 'list_models', arguments: {} });
  assert.deepEqual(result.models, scope.models);
  assert.deepEqual(result.modelingTargets, []);
  assert.equal(result.protocolVersion, '2025-06-18');
  assert.equal(progress.length, 2);
  assert.ok(!JSON.stringify(result).includes('disposable-test-token'));
});

test('a valid account with no models is distinguished from a failed connection', async t => {
  const calls = server(t, { 'tools/call': message => Response.json({ jsonrpc: '2.0', id: message.id, result: { structuredContent: { owner: 'analyst', models: [], modeling_targets: ['next-model'] } } }) });
  const result = await inspectMcp('modeling', 'test-token', new AbortController().signal, () => {});
  assert.ok(calls.every(call => call.url === '/mcp/modeling'));
  assert.deepEqual(result.models, []);
  assert.deepEqual(result.modelingTargets, ['next-model']);
});

test('connection errors explain expired credentials and non-JSON proxy failures', async t => {
  for (const [status, expected] of [[401, /令牌/], [403, /权限和访问地址/], [404, /启用/], [502, /HTTP 502/]]) {
    await t.test(String(status), async t => {
      const calls = server(t, { initialize: () => new Response('<html>proxy error</html>', { status }) });
      await assert.rejects(inspectMcp('query', 'test-token', new AbortController().signal, () => {}), expected);
      assert.equal(calls.length, 1);
    });
  }
});

test('inspection rejects mismatched responses and tool failures instead of reporting success', async t => {
  for (const [name, response, expected] of [
    ['incorrect request ID', message => ({ jsonrpc: '2.0', id: message.id + 1, result: {} }), /无效的 MCP/],
    ['tool denied', message => ({ jsonrpc: '2.0', id: message.id, result: { isError: true, structuredContent: { message: '权限已变更' } } }), /权限已变更/],
    ['missing capabilities', message => ({ jsonrpc: '2.0', id: message.id, result: { structuredContent: { models: [{ id: 'graph', name: 'Graph' }], modeling_targets: [] } } }), /响应不完整/],
  ]) {
    await t.test(name, async t => {
      const calls = server(t, { 'tools/call': message => Response.json(response(message)) });
      await assert.rejects(inspectMcp('query', 'test-token', new AbortController().signal, () => {}), expected);
    });
  }
});


test('cancelling inspection aborts an in-flight tool request', async t => {
  const controller = new AbortController();
  let started;
  const waiting = new Promise(resolve => { started = resolve; });
  server(t, {
    'tools/list': (_message, options) => new Promise((_resolve, reject) => {
      options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true });
      started();
    }),
  });
  const inspection = inspectMcp('query', 'test-token', controller.signal, () => {});
  await waiting;
  controller.abort();
  await assert.rejects(inspection, { name: 'AbortError' });
});
