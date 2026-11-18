import assert from 'node:assert/strict';
import { test } from 'node:test';
import { edgeCurve, edgeLanes, packPaths, positions, scene } from '../src/graphLayout.ts';
import { canvasColumns, connectedNodeOrder, placeNodes } from '../src/modelCanvasGeometry.ts';

test('model auto layout places connected node types together in the presence of cycles and isolated types', () => {
  const names = ['Account', 'Product', 'Invoice', 'Supplier', 'Region'];
  const links = [
    { from: 'Invoice', to: 'Account' }, { from: 'Account', to: 'Invoice' },
    { from: 'Product', to: 'Supplier' }, { from: 'Product', to: 'NotConfigured' },
  ];
  assert.deepEqual(connectedNodeOrder(names, links), ['Account', 'Invoice', 'Product', 'Supplier', 'Region']);
});

test('model canvas has room for long labels at phone, split pane, and desktop widths', () => {
  const names = Array.from({ length: 7 }, (_, index) => 'LongNodeTypeName' + index);
  for (const width of [264, 360, 500, 1000]) {
    const columns = canvasColumns(width);
    const height = Math.max(280, Math.ceil(names.length / columns) * 124 + 95);
    for (const layout of ['spread', 'grid']) {
      const points = [...placeNodes(names, width, height, columns, layout).values()];
      points.forEach(([x, y], index) => {
        assert.ok(x >= 74 && x <= width - 74);
        for (const [previousX, previousY] of points.slice(0, index)) {
          assert.ok(Math.abs(x - previousX) >= 148 || Math.abs(y - previousY) >= 64);
        }
      });
    }
  }
});
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

test('parallel transfer labels have separate pointer targets in every orientation', () => {
  const start = { x: 0, y: 0 };
  const lanes = edgeLanes([{ id: '100', from: 'a', to: 'b' }, { id: '200', from: 'a', to: 'b' },
    { id: '300', from: 'b', to: 'a' }]);
  for (const end of [{ x: 160, y: 0 }, { x: 0, y: 160 }, { x: 130, y: -80 }]) {
    const labels = lanes.map(edge => edge.from === 'a' ? edgeCurve(edge, start, end).label : edgeCurve(edge, end, start).label);
    labels.forEach((label, i) => {
      for (const other of labels.slice(0, i)) {
        assert.ok(Math.abs(label.x - other.x) >= 120 || Math.abs(label.y - other.y) >= 24);
      }
    });
  }
});

test('a three-account transfer cycle leaves all three relationships visible in radial layout', () => {
  const vertices = ['a', 'b', 'c'].map(id => ({ id, kind: 'node' }));
  const edges = [['a', 'b'], ['b', 'c'], ['c', 'a']].map(([from, to]) => ({ id: from + to, from, to }));
  const points = positions({ vertices, edges }, 'radial');
  for (const [index, edge] of edges.entries()) {
    const a = points.get(edge.from), b = points.get(edge.to), c = points.get(vertices[(index + 2) % 3].id);
    const length = Math.hypot(b.x - a.x, b.y - a.y);
    const distance = Math.abs((b.x - a.x) * (a.y - c.y) - (a.x - c.x) * (b.y - a.y)) / length;
    assert.ok(distance > 15, 'an edge should pass clear of the unrelated account');
  }
});
