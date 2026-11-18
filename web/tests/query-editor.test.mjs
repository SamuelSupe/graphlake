import assert from 'node:assert/strict';
import { test } from 'node:test';
import { queryParameterError } from '../src/queryParameters.ts';
import { schemaCompletions } from '../src/queryCompletions.ts';

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

test('Cypher parameter input accepts optional objects and enforces the API byte limit', () => {
  for (const raw of ['', '  ', '{}', '{"ids":[1,2],"tag":null}']) assert.equal(queryParameterError(raw), '');
  for (const raw of ['{', '[]', 'null', '42', '"text"']) assert.ok(queryParameterError(raw));
  const value = '{"x":"' + '界'.repeat(21842) + '"}  ';
  assert.equal(Buffer.byteLength(value), 65536);
  assert.equal(queryParameterError(value), '');
  assert.ok(queryParameterError(value + ' '));
});
