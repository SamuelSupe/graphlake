import type { GraphElement, GraphIdentity, QueryResult } from './api';

export const graphType = ({ kind, label }: Pick<GraphIdentity, 'kind' | 'label'>) => kind + ':' + label;
const displayFields = ['name', 'display_name', 'displayName', 'title', 'full_name', 'fullName'];

export function graphLabel(element: GraphElement, field?: string | null) {
  const props = element.properties;
  const value = field === null ? undefined : field !== undefined ? props?.[field]
    : props && props[displayFields.find(name => props[name]?.trim()) ?? ''];
  return value?.trim() || element.label;
}

export function visibleGraphElements(elements: GraphElement[], hiddenTypes: ReadonlySet<string>) {
  if (!hiddenTypes.size) return elements;
  const hiddenNodes = new Set(elements.filter(element => element.kind === 'node' && hiddenTypes.has(graphType(element)))
    .map(element => element.id));
  return elements.filter(element => !hiddenTypes.has(graphType(element))
    && !hiddenNodes.has(element.from ?? '') && !hiddenNodes.has(element.to ?? ''));
}

export const maxElements = 3000;

export function searchCanvas(elements: GraphElement[], query: string): GraphElement[] {
  const search = query.trim().toLowerCase();
  if (!search) return [];
  const separator = search.indexOf('=');
  if (separator > 0) {
    const key = search.slice(0, separator).trim();
    const value = search.slice(separator + 1).trim();
    return elements.filter(element => Object.entries(element.properties ?? {}).some(([name, text]) =>
      name.toLowerCase() === key && (text ?? 'null').toLowerCase().includes(value)));
  }
  return elements.filter(element => [element.label, ...element.key, ...Object.values(element.properties ?? {})]
    .some(value => value?.toLowerCase().includes(search)));
}

function merge(previous: GraphElement | undefined, value: GraphElement): GraphElement {
  const preferEndpoints = value.directed === true || previous?.directed !== true;
  const keepProperties = previous?.properties != null && !previous.property_preview;
  return {
    ...previous, ...value,
    properties: keepProperties ? previous.properties : value.properties ?? previous?.properties ?? null,
    property_preview: keepProperties ? false : value.properties ? value.property_preview : previous?.property_preview,
    from: preferEndpoints ? value.from ?? previous?.from : previous?.from,
    to: preferEndpoints ? value.to ?? previous?.to : previous?.to,
    directed: preferEndpoints ? value.directed ?? previous?.directed : previous?.directed,
  };
}

export function appendGraph(elements: GraphElement[], incoming: GraphElement[]) {
  const canvas = new Map(elements.map(element => [element.id, element]));
  for (const element of incoming) canvas.set(element.id, merge(canvas.get(element.id), element));
  return canvas.size <= maxElements ? [...canvas.values()] : undefined;
}

export function combineVersions(before: QueryResult['sources'], after: QueryResult['sources']) {
  const sources = new Map(before.map(source => [source.source, source]));
  for (const source of after) {
    const previous = sources.get(source.source);
    // A PostgreSQL transaction token can advance while mapped relation versions stay equal.
    if (previous && previous.version !== source.version && source.consistency !== 'PostgreSQL repeatable read') return undefined;
    sources.set(source.source, source);
  }
  return [...sources.values()];
}

export function mergePage(elements: GraphElement[], selected: GraphElement, incoming: GraphElement[]) {
  const canvas = new Map(elements.map(element => [element.id, element]));
  const received = new Map(incoming.map(element => [element.id, element]));
  const add = (value: GraphElement) => canvas.set(value.id, merge(canvas.get(value.id), value));
  if (canvas.has(selected.id)) add(selected);
  const edges = incoming.filter(element => element.kind === 'edge');
  for (const [index, edge] of edges.entries()) {
    const endpoints = [...new Set([edge.from, edge.to])].filter((id): id is string => !!id);
    const missing = endpoints.filter(id => !canvas.has(id));
    if (missing.some(id => !received.has(id)) || canvas.size + missing.length + Number(!canvas.has(edge.id)) > maxElements) {
      const remaining = edges.slice(index);
      const required = new Set(remaining.flatMap(edge => [edge.from, edge.to]));
      const nodes = incoming.filter(element => element.kind === 'node' && required.has(element.id));
      return { elements: [...canvas.values()], added: canvas.size - elements.length, remaining: [...nodes, ...remaining] };
    }
    for (const id of endpoints) {
      const endpoint = received.get(id);
      if (endpoint) add(endpoint);
    }
    add(edge);
  }
  return { elements: [...canvas.values()], added: canvas.size - elements.length, remaining: [] };
}

export function removeElement(elements: GraphElement[], id: string) {
  return elements.filter(element => element.id !== id && element.from !== id && element.to !== id);
}

export function focusNeighborhood(elements: GraphElement[], steps: [string, string][], id: string) {
  const neighborhood = new Set([id]);
  for (const element of elements) {
    if (element.from === id || element.to === id) {
      neighborhood.add(element.id);
      if (element.from) neighborhood.add(element.from);
      if (element.to) neighborhood.add(element.to);
    }
  }
  for (const [from, to] of steps) {
    if (from === id) neighborhood.add(to);
    if (to === id) neighborhood.add(from);
  }
  return elements.filter(element => neighborhood.has(element.id));
}
