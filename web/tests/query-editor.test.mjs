import assert from 'node:assert/strict';
import { test } from 'node:test';
import { queryLimitErrors, queryParameterError } from '../src/queryParameters.ts';
import { schemaCompletions, typeSampleQuery } from '../src/queryCompletions.ts';

test('model completion includes business keys and safely quotes identifiers for each dialect', () => {
  const schema = { graph: 'demo', nodes: { 'User name': { key: ['tenant_id', 'id'], properties: ['display"name'] } },
    edges: { "Paid'By": { key: ['id'], properties: ['amount'], from: { node: 'User name' }, to: { node: 'User name' } } } };
  assert.equal(schemaCompletions(schema, 'cypher').find(item => item.label === 'User name').apply, '`User name`');
  assert.equal(schemaCompletions(schema, 'sql').find(item => item.label === 'display"name').apply, '"display""name"');
  assert.equal(schemaCompletions(schema, 'gremlin').find(item => item.label === "Paid'By").apply, "'Paid\\'By'");
  const fields = schemaCompletions(schema, 'cypher').filter(item => item.type === 'property');
  assert.equal(fields.filter(item => item.label === 'id').length, 1);
  assert.ok(fields.some(item => item.label === 'tenant_id'));
  assert.deepEqual(schemaCompletions(undefined, 'cypher'), []);
});

test('type samples quote model names and bound relationship paths and node results', () => {
  assert.equal(typeSampleQuery('nodes', 'Account'), 'MATCH (n:Account) RETURN n LIMIT 25');
  assert.equal(typeSampleQuery('edges', 'paid` by'), 'MATCH p = ()-[:`paid`` by`]->() RETURN p LIMIT 25');
});

test('Cypher parameter input accepts optional objects and enforces the API byte limit', () => {
  for (const raw of ['', '  ', '{}', '{"ids":[1,2],"tag":null}']) assert.equal(queryParameterError(raw), '');
  for (const raw of ['{', '[]', 'null', '42', '"text"']) assert.ok(queryParameterError(raw));
  const value = '{"x":"' + '界'.repeat(21842) + '"}  ';
  assert.equal(Buffer.byteLength(value), 65536);
  assert.equal(queryParameterError(value), '');
  assert.ok(queryParameterError(value + ' '));
});

test('query and template limits identify the field to correct and allow server defaults', () => {
  const valid = { timeout: false, max_observation_age_seconds: false, max_reconciliation_age_seconds: false };
  assert.deepEqual(queryLimitErrors('', {}), valid);
  assert.deepEqual(queryLimitErrors('60000', { max_observation_age_seconds: 1, max_reconciliation_age_seconds: 2592000 }), valid);
  assert.deepEqual(queryLimitErrors('60001', { max_observation_age_seconds: 0 }), {
    ...valid, timeout: true, max_observation_age_seconds: true,
  });
  assert.deepEqual(queryLimitErrors('1', { max_observation_age_seconds: 60, max_reconciliation_age_seconds: 1.5 }), {
    ...valid, max_reconciliation_age_seconds: true,
  });
});
