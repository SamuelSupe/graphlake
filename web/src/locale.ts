import { english } from './english.ts';
import { translateDiagnostic } from './englishDiagnostics.ts';

export type Locale = 'zh-CN' | 'en';
const key = 'graphlake:locale';
const defaultLocale: Locale = 'en';

function supported(value: string | null): value is Locale {
  return value === 'zh-CN' || value === 'en';
}

function loadLocale(): Locale {
  if (typeof window === 'undefined') return defaultLocale;
  const url = new URL(window.location.href);
  const requested = url.searchParams.get('lang');
  if (supported(requested)) {
    try {
      window.localStorage.setItem(key, requested);
      url.searchParams.delete('lang');
      window.history.replaceState(window.history.state, '', url);
    } catch { /* Keep the language in the URL when storage is unavailable. */ }
    return requested;
  }
  try {
    const saved = window.localStorage.getItem(key);
    if (supported(saved)) return saved;
  } catch { /* Use the console's default. */ }
  return defaultLocale;
}

export const locale = loadLocale();
if (typeof document !== 'undefined') document.documentElement.lang = locale;

export function changeLocale(next: string) {
  if (!supported(next) || next === locale) return;
  // A normal navigation honors the workspace's existing unsaved-change guard.
  const url = new URL(window.location.href);
  url.searchParams.set('lang', next);
  window.location.replace(url);
}

export function t(message: string, ...values: unknown[]): string {
  const text = locale === 'en' && Object.hasOwn(english, message) ? english[message] : message;
  return values.length ? text.replace(/\{(\d+)\}/g, (placeholder, index: string) =>
    Number(index) < values.length ? String(values[Number(index)] ?? '') : placeholder) : text;
}

export function diagnosticText(message: string): string {
  return locale === 'en' ? translateDiagnostic(message) ?? t(message) : message;
}
