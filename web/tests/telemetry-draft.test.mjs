import assert from 'node:assert/strict';
import test from 'node:test';
import { telemetryDraftChanged } from '../src/telemetryDraft.ts';

test('restoring a telemetry field clears the unsaved state unless another edit remains', () => {
  const saved = { enabled: false, endpoint: null, headers_env: {} };
  const form = { ...saved, enabled: true };
  assert.ok(telemetryDraftChanged(saved, form, []));
  form.enabled = false;
  assert.equal(telemetryDraftChanged(saved, form, []), false);
  assert.ok(telemetryDraftChanged(saved, form, [{ name: '', variable: '' }]));
});

test('header order and HTTP name casing do not mark restored settings dirty, but duplicate rows and changed references do', () => {
  const saved = { headers_env: { authorization: 'AUTH', 'x-tenant': 'TENANT' } };
  const tenant = { name: 'X-Tenant', variable: 'TENANT' };
  const auth = { name: 'Authorization', variable: 'AUTH' };
  assert.equal(telemetryDraftChanged(saved, saved, [tenant, auth]), false);
  assert.ok(telemetryDraftChanged(saved, saved, [auth, { name: 'authorization', variable: 'AUTH' }]));
  assert.ok(telemetryDraftChanged(saved, saved, [tenant, { ...auth, variable: 'OTHER' }]));
});
