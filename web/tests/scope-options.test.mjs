import assert from 'node:assert/strict';
import { test } from 'node:test';
import { scopeDraft, scopeJson } from '../src/scopeOptions.ts';

test('scope edits preserve large numeric partition IDs and the scalar types in imported JSON', () => {
  const draft = scopeDraft('{"snapshot_time":"2025-01-01","logical_partition":[' +
    '{"column":"id","op":"eq","value":9007199254740993},' +
    '{"column":"status","op":"eq","value":"false"},' +
    '{"column":"score","op":"ge","value":1.0},' +
    '{"column":"deleted","op":"is_null","value":null}]}');
  const serialized = scopeJson({ ...draft, consistent: true });
  assert.match(serialized, /"value":9007199254740993/);
  assert.match(serialized, /"value":"false"/);
  assert.match(serialized, /"value":1\.0/);
  assert.deepEqual(scopeDraft(serialized), { ...draft, consistent: true });
});

test('editing a partition cannot apply an empty, malformed or unsupported predicate', () => {
  assert.throws(() => scopeJson({ time: '', consistent: false, filters: [{ column: 'id', op: 'eq', value: 'no quotes' }] }), /条件 1/);
  assert.throws(() => scopeDraft('{"logical_partition":[{"column":"id","op":"is_null","value":1}]}'));
  assert.throws(() => scopeDraft('{"unrecognized":true}'));
});
