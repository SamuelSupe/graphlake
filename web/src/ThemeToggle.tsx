import { Moon, Sun } from 'lucide-react';
import { t } from './locale.ts';
import { toggleTheme, useTheme } from './theme.ts';

export function ThemeToggle({ compact = false }: { compact?: boolean }) {
  const theme = useTheme();
  const isDark = theme === 'dark';
  const label = isDark ? t('切换到浅色模式') : t('切换到深色模式');
  return <button type="button" className={'theme-toggle' + (compact ? ' is-compact' : '')}
    onClick={toggleTheme} aria-label={label} title={label} aria-pressed={isDark}>
    {isDark ? <Sun size={16} aria-hidden="true" /> : <Moon size={16} aria-hidden="true" />}
    {!compact && <span>{isDark ? t('浅色模式') : t('深色模式')}</span>}
  </button>;
}
