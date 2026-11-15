import assert from 'node:assert/strict';
import { test } from 'node:test';
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
