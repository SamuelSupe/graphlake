import { t } from './locale.ts';
import { isMap, isSeq, parseDocument } from 'yaml';

export const tasks = [
  { id: 'offboarded_account_exposure', name: t('离职账号残留授权'), description: t('核对残留授权与离职后访问记录。') },
  { id: 'alert_identity_device_context', name: t('告警关联身份与设备'), description: t('追溯告警时点的身份归属、设备活动和共享 IP 线索。') },
] as const;

export const taskName = (id: string) => tasks.find(task => task.id === id)?.name ?? id;

export type SuiteDraft = { tables: string; cases: string[] };
export const emptySuite: SuiteDraft = { tables: '{}', cases: [] };

export function splitSuite(content: string): SuiteDraft {
  JSON.parse(content);
  const doc = parseDocument(content, { intAsBigInt: true });
  const tables = doc.get('tables', true);
  const cases = doc.get('cases', true);
  if (doc.errors.length || !isMap(doc.contents) || doc.contents.items.length !== 2 || !isMap(tables) || !isSeq(cases)) {
    throw new Error(t('套件需要包含 tables 对象和 cases 数组。'));
  }
  // Keep the original JSON tokens when editing other sections, including 64-bit integers.
  const raw = (range?: readonly number[] | null) => range ? content.slice(range[0], range[1]) : 'null';
  return { tables: raw(tables.range), cases: cases.items.map(item => isMap(item) ? raw(item.range) : 'null') };
}

export function joinSuite(suite: SuiteDraft) {
  const content = `{\n  "tables": ${suite.tables},\n  "cases": [\n${suite.cases.join(',\n')}\n  ]\n}`;
  JSON.parse(content);
  return content;
}

export function caseName(raw: string, index: number) {
  try {
    const value = JSON.parse(raw);
    if (typeof value?.id === 'string' && value.id) return value.id;
  } catch { /* Keep the case selectable while editing incomplete JSON. */ }
  return t('场景 {0}', index + 1);
}
