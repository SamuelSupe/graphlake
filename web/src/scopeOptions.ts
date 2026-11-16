import { t } from './locale.ts';
import { isScalar, parseDocument } from 'yaml';
function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new Error(t('请输入有效的范围对象'));
  return value as Record<string, unknown>;
}

export const comparisons = [
  ['eq', '='], ['ne', '≠'], ['lt', '<'], ['le', '≤'], ['gt', '>'], ['ge', '≥'],
  ['is_null', t('为空')], ['is_not_null', t('非空')],
] as const;

export type PartitionDraft = { column: string; op: string; value: string };
export type ScopeDraft = { time: string; consistent: boolean; filters: PartitionDraft[] };

export function nullCheck(op: string) {
  return op === 'is_null' || op === 'is_not_null';
}

export function scopeDraft(raw: string): ScopeDraft {
  if (!raw.trim()) return { time: '', consistent: false, filters: [] };
  const json = JSON.parse(raw);
  if (Object.keys(object(json)).some(
    key => !['snapshot_time', 'require_consistent_snapshot', 'logical_partition'].includes(key),
  )) throw new Error(t('请输入图查询范围对象'));
  const doc = parseDocument(raw, { intAsBigInt: true });
  if (doc.errors.length) throw new Error(t('请输入有效的图查询范围 JSON'));
  const value = object(doc.toJS());
  const { snapshot_time: time, require_consistent_snapshot: consistent, logical_partition: rules } = value;
  if (time != null && typeof time !== 'string' || consistent !== undefined && typeof consistent !== 'boolean'
    || rules !== undefined && !Array.isArray(rules)) throw new Error(t('图查询范围字段格式不正确'));
  const filters = (Array.isArray(rules) ? rules : []).map((rule, index) => {
    const values = object(rule);
    const { column, op, value: item } = values;
    if (typeof column !== 'string' || typeof op !== 'string' || !comparisons.some(([code]) => code === op)
      || Object.keys(values).some(key => !['column', 'op', 'value'].includes(key))) throw new Error(t('逻辑分区条件格式不正确'));
    const scalar = doc.getIn(['logical_partition', index, 'value'], true);
    const numeric = typeof item === 'bigint' || typeof item === 'number';
    const formatted = numeric
      ? (isScalar(scalar) && scalar.source) || item.toString()
      : JSON.stringify(item) ?? '';
    if (nullCheck(op) && item != null) throw new Error(t('空值判断无需填写比较值'));
    return { column, op, value: formatted };
  });
  return { time: time ?? '', consistent: consistent ?? false, filters };
}

export function scopeJson({ time, consistent, filters }: ScopeDraft) {
  const fields: string[] = [];
  if (time.trim()) fields.push('"snapshot_time":' + JSON.stringify(time.trim()));
  if (consistent) fields.push('"require_consistent_snapshot":true');
  if (filters.length) {
    const rules = filters.map(({ column, op, value }, index) => {
      if (!column.trim()) throw new Error(t('请输入条件 {0} 的列名', (index + 1)));
      if (!nullCheck(op)) {
        let scalar: unknown;
        try { scalar = JSON.parse(value); } catch { /* Show the field error below. */ }
        if (typeof scalar !== 'string' && typeof scalar !== 'boolean' && !(typeof scalar === 'number' && Number.isFinite(scalar))) {
          throw new Error(t('条件 {0} 需要数字、布尔值或带双引号的文本', (index + 1)));
        }
      }
      return '{"column":' + JSON.stringify(column.trim()) + ',"op":' + JSON.stringify(op) + ',"value":' + (nullCheck(op) ? 'null' : value.trim()) + '}';
    });
    fields.push('"logical_partition":[' + rules.join(',') + ']');
  }
  return fields.length ? '{' + fields.join(',') + '}' : '';
}
