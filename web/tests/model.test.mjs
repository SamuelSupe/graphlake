import assert from 'node:assert/strict';
import { test } from 'node:test';
import { appendMappingInput, documentOf, modelIdPattern, modelOf, removeGraphType, rename, validationTargets } from '../src/model.ts';

test('model ID pattern works with the HTML pattern Unicode sets flag', () => {
  const pattern = new RegExp('^(?:' + modelIdPattern + ')$', 'v');
  assert.equal(pattern.test('browser-check_1'), true);
  assert.equal(pattern.test('../outside'), false);
});

test('visual edits preserve advanced YAML and update both edge endpoints', () => {
  const yaml = [
    'graph: finance',
    'sources:',
    '  ledger: { type: postgres, dsn_env: GRAPHLAKE_POSTGRES_DSN }',
    '  big: { type: bigquery, billing_project: finance, location: US, token_env: BIGQUERY_TOKEN, maximum_bytes_billed: 9007199254740995 }',
    'nodes:',
    '  Account:',
    '    key: [tenant, id]',
    '    properties: [name]',
    '    scd2: { valid_from: effective, valid_to: expires }',
    '    inputs:',
    '      - table: ledger.public.accounts',
    '        columns: { id: account_id }',
    '        where: [{ column: tenant, op: eq, value: 9007199254740993 }]',
    '      - table: ledger.public.archive',
    'edges:',
    '  Transfer:',
    '    source: ledger.public.transfers',
    '    key: [tenant, transfer_id]',
    '    from: { node: Account, columns: [tenant, src] }',
    '    to: { node: Account, columns: [tenant, dst] }',
  ].join('\n');
  const doc = documentOf(yaml);
  const before = modelOf(yaml);
  doc.setIn(['nodes', 'Account', 'properties'], ['name', 'status']);
  rename(doc, before, 'nodes', 'Account', 'CustomerAccount');
  const after = modelOf(doc.toString());
  assert.deepEqual(after.nodes.CustomerAccount.inputs, before.nodes.Account.inputs);
  assert.deepEqual(after.nodes.CustomerAccount.scd2, before.nodes.Account.scd2);
  assert.equal(after.sources.ledger.dsn_env, 'GRAPHLAKE_POSTGRES_DSN');
  assert.equal(after.sources.big.maximum_bytes_billed, 9007199254740995n);
  assert.deepEqual(after.nodes.CustomerAccount.properties, ['name', 'status']);
  assert.deepEqual(after.edges.Transfer.from, { node: 'CustomerAccount', columns: ['tenant', 'src'] });
  assert.deepEqual(after.edges.Transfer.to, { node: 'CustomerAccount', columns: ['tenant', 'dst'] });
});


test('a single table becomes an editable multi-table mapping without changing SCD2', () => {
  const doc = documentOf(`graph: finance
nodes:
  Account:
    source: ledger.public.accounts # keep the existing input
    key: [id]
    scd2: { current_flag: version, current_value: 9007199254740993 }`);
  const path = ['nodes', 'Account'];
  const before = modelOf(doc.toString());
  appendMappingInput(doc, path);
  doc.setIn([...path, 'inputs', 1, 'table'], 'lake.archive');
  doc.setIn([...path, 'inputs', 1, 'columns', 'id'], 'archive_id');
  appendMappingInput(doc, path);
  doc.setIn([...path, 'inputs', 2, 'table'], 'ledger.public.legacy');
  const output = doc.toString();
  const after = modelOf(output);
  assert.deepEqual(after.nodes.Account.inputs, [
    { table: 'ledger.public.accounts' },
    { table: 'lake.archive', columns: { id: 'archive_id' } },
    { table: 'ledger.public.legacy' },
  ]);
  assert.equal(after.nodes.Account.source, undefined);
  assert.deepEqual(after.nodes.Account.scd2, before.nodes.Account.scd2);
  assert.match(output, /keep the existing input/);
});

test('removing a node drops both kinds of incident edges and preserves unrelated mappings', () => {
  const yaml = `graph: finance
nodes:
  Account: {key: [id]}
  Merchant: {key: [id], scd2: {current_flag: v, current_value: 9007199254740993}}
edges:
  Pay: {from: {node: Account}, to: {node: Merchant}}
  Refund: {from: {node: Merchant}, to: {node: Account}}
  Loop: {from: {node: Merchant}, to: {node: Merchant}}`;
  const before = modelOf(yaml);
  const doc = documentOf(yaml);
  removeGraphType(doc, before, 'nodes', 'Account');
  const after = modelOf(doc.toString());
  assert.deepEqual(after.nodes, { Merchant: before.nodes.Merchant });
  assert.deepEqual(after.edges, { Loop: before.edges.Loop });
  assert.throws(() => removeGraphType(doc, after, 'nodes', 'Merchant'));
});

test('a missing physical column points to its renamed mapping and input filter', () => {
  const model = modelOf(`graph: finance
nodes:
  Account:
    key: [id]
    inputs:
      - {table: ledger.public.accounts, columns: {id: account_id}}
      - {table: ledger.public.accounts, where: [{column: account_id, op: eq, value: 1}]}
  Merchant: {source: ledger.public.merchants, key: [account_id]}`);
  const result = validationTargets(model, { kind: 'table', table: 'ledger.public.accounts', column: 'account_id' });
  assert.deepEqual(result.map(target => [target.selection.name, JSON.parse(target.control)]),
    [['Account', ['column', 0, 'id']], ['Account', ['filter', 1, 0]]]);
});

test('renaming a source rewrites direct and multi-table references without changing other YAML', () => {
  const doc = documentOf(`sources:
  ledger: {type: postgres, dsn_env: LEDGER_DSN}
  ledger_archive: {type: postgres, dsn_env: ARCHIVE_DSN}
nodes:
  Account:
    source: ledger.public.accounts # main table
  Merchant:
    inputs:
      - {table: ledger.public.merchants, where: [{column: id, op: eq, value: 9007199254740993}]}
      - {table: ledger_archive.public.merchants}
edges:
  Pay: {source: ledger.public.payments}`);
  const before = modelOf(doc.toString());
  assert.throws(() => removeGraphType(doc, before, 'sources', 'ledger'), /替换引用/);
  rename(doc, before, 'sources', 'ledger', 'finance');
  const output = doc.toString();
  const after = modelOf(output);
  assert.equal(after.sources.finance.dsn_env, 'LEDGER_DSN');
  assert.equal(after.nodes.Account.source, 'finance.public.accounts');
  assert.deepEqual(after.nodes.Merchant.inputs, [
    { table: 'finance.public.merchants', where: [{ column: 'id', op: 'eq', value: 9007199254740993n }] },
    { table: 'ledger_archive.public.merchants' },
  ]);
  assert.equal(after.edges.Pay.source, 'finance.public.payments');
  assert.match(output, /# main table/);
});

test('a missing temporal column locates the model field or the mapped physical column', () => {
  const model = modelOf(`nodes:
  Current:
    source: lake.finance.accounts
    scd2: {current_flag: status}
  Combined:
    inputs:
      - {table: lake.finance.accounts, columns: {active: status}}
    scd2: {current_flag: active}`);
  const found = validationTargets(model, { kind: 'table', table: 'lake.finance.accounts', column: 'status' });
  assert.deepEqual(found.map(target => [target.selection.name, JSON.parse(target.control)]),
    [['Current', ['scd2', 'current_flag']], ['Combined', ['column', 0, 'active']]]);
});
