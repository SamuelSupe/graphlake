import assert from 'node:assert/strict';
import { test } from 'node:test';

function browser(test, initialUrl, preference) {
  let current = new URL(initialUrl);
  let saved = preference;
  let navigation;
  globalThis.window = {
    location: {
      get href() { return current.href; },
      replace(url) { navigation = url.href; },
    },
    localStorage: {
      getItem() { return saved; },
      setItem(_key, value) { saved = value; },
    },
    history: {
      replaceState(_state, _unused, url) { current = new URL(url); },
    },
  };
  globalThis.document = { documentElement: { lang: '' } };
  test.after(() => { delete globalThis.window; delete globalThis.document; });
  return {
    get url() { return current.href; },
    get saved() { return saved; },
    get navigation() { return navigation; },
  };
}

test('a first visit defaults to English and a language switch preserves the current route', async test => {
  const state = browser(test, 'http://localhost/models/demo/query?view=graph#results', null);
  const english = await import('../src/locale.ts?first-visit');
  assert.equal(english.locale, 'en');
  assert.equal(document.documentElement.lang, 'en');
  assert.equal(english.t('查询 {0}', '客户'), 'Query 客户');

  english.changeLocale('zh-CN');
  assert.equal(state.navigation, 'http://localhost/models/demo/query?view=graph&lang=zh-CN#results');
  assert.equal(state.saved, null);

  const after = browser(test, state.navigation, state.saved);
  const chinese = await import('../src/locale.ts?after-switch');
  assert.equal(chinese.locale, 'zh-CN');
  assert.equal(document.documentElement.lang, 'zh-CN');
  assert.equal(after.url, state.url);
  assert.equal(after.saved, 'zh-CN');

  browser(test, after.url, after.saved);
  const reload = await import('../src/locale.ts?after-reload');
  assert.equal(reload.locale, 'zh-CN');
});

test('a saved locale applies to diagnostics while retaining their identifiers', async test => {
  browser(test, 'http://localhost/?lang=unsupported', 'en');
  const { locale, diagnosticText } = await import('../src/locale.ts?saved-locale');
  assert.equal(locale, 'en');
  assert.equal(diagnosticText('用户名或密码错误'), 'Incorrect username or password');
  assert.equal(diagnosticText('图模型中没有此属性 · 客户编号'), 'This property is not in the graph model · 客户编号');
  assert.equal(diagnosticText('Connector returned vendor error E42'), 'Connector returned vendor error E42');
});
