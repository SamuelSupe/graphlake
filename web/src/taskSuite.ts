import { t } from './locale.ts';
import { isMap, isSeq, parseDocument, type Document } from 'yaml';

const windowParameters = [
  { key: 'window_start', label: t('窗口开始时间'), time: true },
  { key: 'window_end', label: t('窗口结束时间'), time: true },
];

export const tasks = [
  {
    id: 'offboarded_account_exposure', name: t('离职账号残留授权'), description: t('核对残留授权与离职后访问记录。'),
    parameters: [
      { key: 'namespace', label: t('身份命名空间') },
      { key: 'person_id', label: t('人员标识') },
      { key: 'as_of', label: t('检查时点'), time: true },
      ...windowParameters,
    ],
  },
  {
    id: 'alert_identity_device_context', name: t('告警关联身份与设备'), description: t('追溯告警时点的身份归属、设备活动和共享 IP 线索。'),
    parameters: [{ key: 'alert_id', label: t('告警标识') }, ...windowParameters],
  },
] as const;

export const taskName = (id: string) => tasks.find(task => task.id === id)?.name ?? id;
export const subjects: Record<string, string> = { found: t('找到主体'), not_found: t('未找到主体'), no_departure_at_as_of: t('指定时点尚未离职') };
const findingNames: Record<string, string> = {
  residual_grant: t('残留直接授权'),
  residual_role_grant: t('残留角色授权'),
  successful_access_after_departure: t('离职后访问成功'),
  failed_access_after_departure: t('离职后访问失败'),
  unknown_access_outcome: t('离职后访问结果未知'),
  direct_account: t('告警直接记录的账号'),
  direct_device: t('告警直接记录的设备'),
  identity_assignment: t('告警时点的身份归属'),
  device_correlation: t('通过账号关联的设备'),
  account_device_correlation: t('通过设备关联的账号'),
  shared_ip_correlation: t('共享 IP 线索'),
};
export const findingName = (kind: string) => Object.hasOwn(findingNames, kind) ? findingNames[kind] : kind;

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

export function caseSummary(raw: string, index: number) {
  let name = t('场景 {0}', index + 1);
  let task = '';
  try {
    const value = JSON.parse(raw);
    if (typeof value?.id === 'string' && value.id) name = value.id;
    if (typeof value?.task === 'string') task = value.task;
  } catch { /* Keep the case selectable while editing incomplete JSON. */ }
  return { name, task };
}

export function scenarioDocument(raw: string): Document.Parsed | undefined {
  try {
    JSON.parse(raw);
    const doc = parseDocument(raw, { intAsBigInt: true });
    if (!doc.errors.length && isMap(doc.contents)) return doc;
  } catch { /* The JSON editor keeps invalid edits available for repair. */ }
}

export function editScenarioField(raw: string, doc: Document.Parsed, path: string[], value: string) {
  const map = path.length === 1 ? doc.contents : doc.getIn(path.slice(0, -1), true);
  const key = path[path.length - 1];
  if (!isMap(map) || !map.range) return raw;
  const field = map.get(key, true);
  const token = JSON.stringify(value);
  // Rewrite only the edited field so 64-bit evidence and other JSON literals survive.
  if (field?.range) return raw.slice(0, field.range[0]) + token + raw.slice(field.range[1]);
  const end = map.range[1] - 1;
  const entry = JSON.stringify(key) + ': ' + token;
  return raw.slice(0, end) + (map.items.length ? ', ' : '') + entry + raw.slice(end);
}
