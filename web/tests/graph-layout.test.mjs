import assert from 'node:assert/strict';
import { test } from 'node:test';
import { edgeCurve, edgeLanes, packPaths, positions, scene } from '../src/graphLayout.ts';
import { focusNeighborhood, graphLabel, maxElements, mergePage, removeElement, visibleGraphElements } from '../src/graphExploration.ts';

test('path-only steps connect vertices and yield to a returned edge', () => {
  const from = { id: 'from', kind: 'node' };
  const to = { id: 'to', kind: 'node' };
  const edge = { id: 'payment', kind: 'edge', from: from.id, to: to.id };
  const steps = [[from.id, to.id]];
  const path = scene([from, to], steps);
  assert.equal(path.edges.length, 1);
  assert.equal(path.edges[0].element, undefined);
  const merged = scene([from, to, edge], steps);
  assert.deepEqual(merged.edges.map(link => link.element), [edge]);
  const layered = positions(merged, 'layered');
  assert.ok(layered.get(from.id).x < layered.get(to.id).x);
});

test('a full canvas holds the edge with its missing endpoint and resumes after pruning', () => {
  const from = { id: 'from', kind: 'node', properties: null };
  const to = { id: 'to', kind: 'node', properties: null };
  const edge = { id: 'payment', kind: 'edge', from: from.id, to: to.id };
  const fillers = Array.from({ length: maxElements - 2 }, (_, i) => ({ id: String(i), kind: 'node' }));
  const initial = [from, ...fillers];
  const blocked = mergePage(initial, from, [from, to, edge]);
  assert.equal(blocked.added, 0);
  assert.deepEqual(blocked.remaining, [from, to, edge]);
  const pruned = removeElement(blocked.elements, fillers[0].id);
  const resumed = mergePage(pruned, from, blocked.remaining);
  assert.equal(resumed.added, 2);
  assert.equal(resumed.remaining.length, 0);
  assert.deepEqual(focusNeighborhood(resumed.elements, [], from.id).map(item => item.id), ['from', 'to', 'payment']);
  assert.deepEqual(removeElement(resumed.elements, to.id).map(item => item.id), pruned.map(item => item.id));
});

test('a held page keeps only the suffix that still needs to join the canvas', () => {
  const from = { id: 'from', kind: 'node' };
  const to = { id: 'to', kind: 'node' };
  const other = { id: 'other', kind: 'node' };
  const first = { id: 'first', kind: 'edge', from: from.id, to: to.id };
  const second = { id: 'second', kind: 'edge', from: from.id, to: other.id };
  const fillers = Array.from({ length: maxElements - 3 }, (_, i) => ({ id: String(i), kind: 'node' }));
  const partial = mergePage([from, ...fillers], from, [to, other, first, second]);
  assert.deepEqual(partial.remaining, [other, second]);
  const focused = [from];
  const resumed = mergePage(focused, from, partial.remaining);
  assert.deepEqual(resumed.elements.map(item => item.id), ['from', 'other', 'second']);
});

test('type visibility hides incident edges and keeps entity identity independent from its business name', () => {
  const merchant = { id: 'm', kind: 'node', label: 'Merchant', properties: { name: 'Shop', title: '', code: '0001' } };
  const account = { id: 'a', kind: 'node', label: 'Account' };
  const payment = { id: 'p', kind: 'edge', label: 'Payment', from: 'a', to: 'm' };
  const orphan = { id: 'o', kind: 'edge', label: 'Payment', from: 'm', to: 'unknown' };
  const elements = [merchant, account, payment, orphan];
  assert.equal(graphLabel(merchant), 'Shop');
  assert.equal(graphLabel(merchant, 'code'), '0001');
  assert.equal(graphLabel(merchant, 'title'), 'Merchant');
  assert.equal(graphLabel(merchant, null), 'Merchant');
  assert.deepEqual(visibleGraphElements(elements, new Set(['edge:Payment'])), [merchant, account]);
  assert.deepEqual(visibleGraphElements(elements, new Set(['node:Merchant'])), [account]);
  assert.deepEqual(visibleGraphElements(elements, new Set(['edge:Merchant'])), elements);
});

test('a detail read hydrates an existing edge even when no neighbors are returned', () => {
  const edge = { id: 'p', kind: 'edge', label: 'Payment', from: 'a', to: 'm', directed: true, properties: null };
  const detail = { id: edge.id, kind: 'edge', label: edge.label, properties: { amount: '55.000' } };
  const merged = mergePage([edge], detail, []);
  assert.deepEqual(merged.elements, [{ ...edge, properties: detail.properties, property_preview: undefined }]);
  assert.equal(merged.added, 0);
});

test('dense rendering retains each subpath and the search anchors of reversed parallel edges', () => {
  const a = { x: 0, y: 0 }, b = { x: 100, y: 0 };
  const edges = edgeLanes([{ id: 'first', from: 'a', to: 'b' }, { id: 'second', from: 'b', to: 'a' },
    { id: 'third', from: 'a', to: 'b' }]);
  const curves = edges.map(edge => edgeCurve(edge, edge.from === 'a' ? a : b, edge.to === 'a' ? a : b));
  const paths = curves.map(curve => curve.path);
  assert.equal(new Set(curves.map(curve => curve.label.y)).size, 3);
  const packed = packPaths(paths, 2);
  assert.equal(packed.length, 2);
  assert.equal(packed.join(' '), paths.join(' '));
});
