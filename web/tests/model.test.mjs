import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setupIssue } from '../src/modelWorkflow.ts';
import { modelChanges } from '../src/modelChanges.ts';
import { joinSuite, splitSuite } from '../src/taskSuite.ts';
import { addEdgeType, appendMappingInput, documentOf, modelIdPattern, modelOf, removeGraphType, rename, validationTargets } from '../src/model.ts';

test('editing one business scenario preserves exact JSON in untouched fixture tables and cases', () => {
  const raw = '{"tables":{"security.alert":{"rows":[{"big":9223372036854775807,"score":-0.0}]}},"cases":[{"id":"first"},{"id":"second","expected":{"observed_at_ms":9007199254740993}}]}';
  const suite = splitSuite(raw);
  suite.cases[0] = '{"id":"renamed"}';
  const joined = joinSuite(suite);
  assert.match(joined, /"big":9223372036854775807,"score":-0\.0/);
  assert.match(joined, /"id":"second","expected":\{"observed_at_ms":9007199254740993\}/);
  assert.deepEqual(JSON.parse(joined).cases.map(item => item.id), ['renamed', 'second']);
  suite.cases[0] = '{"id":';
  assert.throws(() => joinSuite(suite), SyntaxError);
});

test('model ID pattern works with the HTML pattern Unicode sets flag', () => {
  const pattern = new RegExp('^(?:' + modelIdPattern + ')$', 'v');
  assert.equal(pattern.test('browser-check_1'), true);
  assert.equal(pattern.test('../outside'), false);
});

test('visual edits preserve advanced YAML and update both edge endpoints', () => {
  const yaml = [
    'graph: finance',
    'freshness: {auto_refresh_csr: true} # Keep the YAML-only setting',
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
  const updated = doc.toString();
  const after = modelOf(updated);
  assert.deepEqual(documentOf(updated).getIn(['freshness', 'auto_refresh_csr']), true);
  assert.match(updated, /# Keep the YAML-only setting/);
  assert.deepEqual(after.nodes.CustomerAccount.inputs, before.nodes.Account.inputs);
  assert.deepEqual(after.nodes.CustomerAccount.scd2, before.nodes.Account.scd2);
  assert.equal(after.sources.ledger.dsn_env, 'GRAPHLAKE_POSTGRES_DSN');
  assert.equal(after.sources.big.maximum_bytes_billed, 9007199254740995n);
  assert.deepEqual(after.nodes.CustomerAccount.properties, ['name', 'status']);
  assert.deepEqual(after.edges.Transfer.from, { node: 'CustomerAccount', columns: ['tenant', 'src'] });
  assert.deepEqual(after.edges.Transfer.to, { node: 'CustomerAccount', columns: ['tenant', 'dst'] });
});

test('invalid mapping shapes cannot be mistaken for an empty or incomplete canvas', () => {
  assert.throws(() => modelOf('nodes: []'), /nodes must be a YAML mapping/);
  assert.throws(() => modelOf('edges: {Transfer: true}'), /edges\.Transfer must be a YAML mapping/);
  assert.deepEqual(modelOf('nodes: {Account: {}}').edges, {});
});

test('creating an edge from the canvas keeps its direction and leaves source fields for mapping', () => {
  const yaml = `graph: finance
nodes:
  Account: {source: lake.accounts, key: [tenant, account_id]}
  Customer: {source: lake.customers, key: [customer_id]}
edges: {}
`;
  const doc = documentOf(yaml);
  addEdgeType(doc, modelOf(yaml), 'Owns', 'Customer', 'Account');
  const model = modelOf(doc.toString());
  assert.deepEqual(model.edges.Owns.from, { node: 'Customer', columns: [''] });
  assert.deepEqual(model.edges.Owns.to, { node: 'Account', columns: ['', ''] });
  assert.equal(model.edges.Owns.source, '');
  assert.throws(() => addEdgeType(doc, model, 'Owns', 'Account', 'Customer'), /name already exists/);
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
  assert.throws(() => removeGraphType(doc, before, 'sources', 'ledger'), /Replace the source tables that reference this data source/);
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


test('setup blocks unfinished mappings but allows node-only and keyless multi-table graphs', () => {
  const model = modelOf(`graph: demo
sources: {lake: {type: parquet, tables: {accounts: [a.parquet], transfers: [t.parquet]}}}
nodes: {Account: {source: lake.accounts, key: [id]}}
edges: {}`);
  assert.equal(setupIssue(model), undefined);
  model.nodes.Account.inputs = [];
  assert.equal(setupIssue(model), undefined);
  model.edges.Transfer = { inputs: [{ table: 'lake.transfers' }], from: { node: 'Account', columns: ['src'] }, to: { node: 'Account', columns: ['dst'] } };
  assert.equal(setupIssue(model), undefined);
  model.edges.Transfer.to.columns = [''];
  assert.deepEqual(setupIssue(model)?.selection, { section: 'edges', name: 'Transfer' });
  delete model.edges.Transfer;
  model.nodes.Account.key = [''];
  assert.deepEqual(setupIssue(model)?.selection, { section: 'nodes', name: 'Account' });
  model.nodes = {};
  assert.equal(setupIssue(model)?.add, 'nodes');
});

test('version summary ignores formatting and reports source, endpoint and field changes without losing integer precision', () => {
  const before = `graph: demo
sources: {lake: {type: parquet, version: 9007199254740992}}
nodes: {Account: {key: [id], source: lake.a, properties: [name]}}
edges: {Transfer: {from: {node: Account}, to: {node: Account}}}`;
  const reformatted = documentOf(before).toString();
  assert.deepEqual(modelChanges(before, reformatted), []);
  const doc = documentOf(before);
  doc.setIn(['sources', 'lake', 'version'], 9007199254740993n);
  doc.setIn(['nodes', 'Account', 'properties'], ['name', 'status']);
  doc.deleteIn(['edges', 'Transfer']);
  const changes = modelChanges(before, doc.toString());
  assert.deepEqual(changes.map(({ kind, section, name }) => [kind, section, name]), [
    ['changed', 'Data Sources', 'lake'], ['changed', 'Node', 'Account'], ['removed', 'Edge', 'Transfer'],
  ]);
  assert.deepEqual(changes[1].fields, ['Properties']);
});
