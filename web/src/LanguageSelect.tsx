import { changeLocale, locale, t } from './locale.ts';

export function LanguageSelect() {
  return <label className="language-picker">
    <span className="sr-only">{t('界面语言')}</span>
    <select value={locale} onChange={event => changeLocale(event.currentTarget.value)}>
      <option value="en" lang="en">English</option>
      <option value="zh-CN" lang="zh-CN">简体中文</option>
    </select>
  </label>;
}
