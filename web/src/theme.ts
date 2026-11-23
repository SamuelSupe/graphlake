import { useSyncExternalStore } from 'react';

export type Theme = 'light' | 'dark';

const STORAGE_KEY = 'graphlake:theme';
const THEME_EVENT = 'graphlake-theme-change';

function isTheme(value: string | null): value is Theme {
  return value === 'light' || value === 'dark';
}

function readTheme(): Theme {
  if (typeof window === 'undefined') return 'light';
  try {
    const saved = window.localStorage.getItem(STORAGE_KEY);
    if (isTheme(saved)) return saved;
  } catch { /* Fall back to default light theme when storage is unavailable. */ }
  return 'light';
}

let currentTheme: Theme = readTheme();

function applyTheme(theme: Theme) {
  if (typeof document !== 'undefined') {
    document.documentElement.dataset.theme = theme;
  }
}

applyTheme(currentTheme);

export function getTheme(): Theme {
  return currentTheme;
}

export function setTheme(next: Theme) {
  if (next === currentTheme) return;
  currentTheme = next;
  applyTheme(next);
  try {
    window.localStorage.setItem(STORAGE_KEY, next);
  } catch { /* Keep theme active for this session. */ }
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(THEME_EVENT, { detail: next }));
  }
}

export function toggleTheme() {
  setTheme(currentTheme === 'dark' ? 'light' : 'dark');
}

function subscribe(listener: () => void) {
  if (typeof window === 'undefined') return () => {};
  const onChange = () => listener();
  const onStorage = (event: StorageEvent) => {
    if (event.key !== STORAGE_KEY || !isTheme(event.newValue)) return;
    currentTheme = event.newValue;
    applyTheme(currentTheme);
    listener();
  };
  window.addEventListener(THEME_EVENT, onChange);
  window.addEventListener('storage', onStorage);
  return () => {
    window.removeEventListener(THEME_EVENT, onChange);
    window.removeEventListener('storage', onStorage);
  };
}

export function useTheme(): Theme {
  return useSyncExternalStore(subscribe, getTheme, () => 'light');
}
