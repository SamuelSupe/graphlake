import { t } from './locale.ts';

export function queryParameterError(input: string): string {
  if (!input.trim()) return '';
  if (new TextEncoder().encode(input).length > 64 * 1024) return t('Cypher 参数不能超过 64 KiB。');
  try {
    const value: unknown = JSON.parse(input);
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) return '';
  } catch {
    return t('参数不是有效的 JSON，请检查引号、逗号和括号。');
  }
  return t('参数需要是 JSON 对象，例如 {"limit": 10}。');
}
