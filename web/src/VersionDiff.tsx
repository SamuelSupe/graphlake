import { t } from './locale.ts';
import { useMemo, useState } from 'react';
import { diffLines } from 'diff';
import { modelChanges } from './modelChanges';

export function VersionDiff({ before, after, release = false }: { before: string; after: string; release?: boolean }) {
  const [unchanged, setUnchanged] = useState(false);
  const diff = useMemo(() => {
    let changes;
    try { changes = modelChanges(before, after); } catch { /* Raw YAML remains available when a draft cannot be parsed. */ }
    const parts = before.split('\n').length + after.split('\n').length > 8000 ? undefined
      : diffLines(before, after, { timeout: 80, maxEditLength: 3000 });
    let oldLine = 1, newLine = 1;
    const rows = parts?.flatMap(part => {
      const lines = part.value.split('\n');
      if (lines.at(-1) === '') lines.pop();
      return lines.map(value => ({ value, kind: part.added ? 'added' : part.removed ? 'removed' : 'same',
        oldLine: part.added ? undefined : oldLine++, newLine: part.removed ? undefined : newLine++ }));
    });
    return { changes, rows };
  }, [before, after]);
  const hasChanges = before !== after;
  return <div className="version-diff">
    <h3>{release ? t('本次发布的变更') : t('历史版本 → 当前草稿')}</h3>
    {diff.changes ? diff.changes.length ? <ul className="change-summary">{diff.changes.map(change => <li key={change.section + ':' + change.name}>
      <span className={'change-kind ' + change.kind}>{{ added: t('新增'), removed: t('删除'), changed: t('修改') }[change.kind]}</span>
      <span>{change.section} <strong>{change.name}</strong>{change.fields.length > 0 && <small> · {change.fields.join(t('、'))}</small>}</span>
    </li>)}</ul> : <p className="muted">{hasChanges ? t('模型内容相同，仅格式或注释有变化。') : release ? t('与上一条发布记录一致。') : t('与当前草稿一致。')}</p>
      : <p className="notice">{t('无法解析模型，请按文本差异核对。')}</p>}
    {hasChanges && diff.rows && <>
      <label className="check-label"><input type="checkbox" checked={unchanged} onChange={event => setUnchanged(event.target.checked)} />{t('显示未修改行')}</label>
      <div className="diff-scroll" role="region" aria-label={t('YAML 逐行差异')} tabIndex={0}>
        <table className="diff-table"><caption className="sr-only">{t('YAML 逐行差异')}</caption>
          <thead><tr><th scope="col">{t('变更前')}</th><th scope="col">{t('变更后')}</th><th scope="col">{t('变化')}</th><th scope="col">YAML</th></tr></thead>
          <tbody>{diff.rows.map((row, index) => (unchanged || row.kind !== 'same') && <tr key={index} className={row.kind}>
            <td>{row.oldLine}</td><td>{row.newLine}</td><td>{row.kind === 'added' ? '+' : row.kind === 'removed' ? '−' : ''}</td><td><code>{row.value || ' '}</code></td>
          </tr>)}</tbody>
        </table>
      </div>
    </>}
    {hasChanges && !diff.rows && <p className="notice">{t('差异较大，请展开完整 YAML 对照；变更摘要仍可用于检查模型变化。')}</p>}
    <details className="full-yaml"><summary>{t('完整 YAML 对照')}</summary><div className="yaml-comparison">
      <label>{release ? t('变更前') : t('历史版本')}<textarea readOnly value={before} rows={12} className="mono" /></label>
      <label>{release ? t('变更后') : t('当前草稿')}<textarea readOnly value={after} rows={12} className="mono" /></label>
    </div></details>
  </div>;
}
