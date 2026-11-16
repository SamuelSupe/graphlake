import { t } from './locale.ts';
import { useId, useState } from 'react';
import { Database, FolderOpen, Plus, Search, Settings2 } from 'lucide-react';
import { ConnectorIcon } from './ConnectorIcon';
import { connectorChoices, connectorDetails } from './connectors';
import { record, sourceLabel, sourceUsers, text, type Model } from './model';

export function Catalogs({ model, selected, configure, browse }: {
  model: Model;
  selected?: string;
  configure: (name: string) => void;
  browse: (name: string) => void;
}) {
  const [search, setSearch] = useState('');
  const query = search.trim().toLowerCase();
  const sources = Object.entries(model.sources);
  const matches = sources.filter(([name, source]) => `${name} ${sourceLabel(text(source.type))}`.toLowerCase().includes(query));
  return <section className="listing catalog-list" aria-label={t('已配置的数据源')}>
    <div className="catalog-section-heading"><h2>{t('已配置的数据源 ')}<span className="catalog-count">{sources.length}</span></h2>
      <p className="muted small">{t('配置当前模型的连接，浏览可用于建模的表与字段。')}</p></div>
    {!!sources.length && <search className="catalog-search" aria-label={t('查找已配置的数据源')}>
      <label className="search"><Search size={17} aria-hidden="true" /><span className="sr-only">{t('搜索已配置的数据源')}</span>
        <input type="search" placeholder={t('搜索名称或连接器类型')} value={search} onChange={event => setSearch(event.target.value)} /></label>
      <span className="muted small" role="status">{matches.length} / {sources.length}{t(' 个数据源')}</span>
    </search>}
    {matches.length ? <div className="source-card-grid">
      {matches.map(([name, source]) => {
        const uses = sourceUsers(model, name).length;
        return <article className="source-card" key={name} data-selected={selected === name || undefined}>
          <header><ConnectorIcon type={text(source.type)} provider={text(record(source.storage).provider)} />
            <div><h3>{name}</h3><span className="muted small">{sourceLabel(text(source.type))}</span></div></header>
          <p className="muted small">{uses ? t('用于 {0} 个节点或边类型', uses) : t('尚未用于节点或边类型')}</p>
          <footer>
            <button className="button compact" aria-haspopup="dialog" onClick={() => configure(name)}>
              <Settings2 size={15} aria-hidden="true" />{t('配置')}<span className="sr-only"> {name}</span></button>
            <button className="text-button" aria-haspopup="dialog" onClick={() => browse(name)}><FolderOpen size={15} aria-hidden="true" />{t('浏览表与字段')}<span className="sr-only"> {name}</span></button>
          </footer>
        </article>;
      })}
    </div> : sources.length ? <div className="catalog-empty"><p>{t('没有匹配的数据源')}</p>
      <button className="text-button" onClick={() => setSearch('')}>{t('清除搜索')}</button></div> : <div className="catalog-empty"><Database size={24} aria-hidden="true" /><p>{t('还没有数据源')}</p>
      <p className="muted small">{t('使用「添加数据源」为此模型选择连接器。')}</p></div>}
  </section>;
}

export function ConnectorGallery({ add, active }: { add: (type: string) => void; active: boolean }) {
  const [search, setSearch] = useState('');
  const [group, setGroup] = useState(t('全部'));
  const filterId = useId();
  const query = search.trim().toLowerCase();
  const matching = connectorChoices.filter(([type, label]) =>
    `${type} ${label} ${connectorDetails[type]?.group} ${connectorDetails[type]?.description}`.toLowerCase().includes(query));
  const groups = [t('全部'), ...new Set(connectorChoices.map(([type]) => connectorDetails[type].group))];
  const choices = matching.filter(([type]) => group === t('全部') || connectorDetails[type].group === group);
  return <div className="connector-gallery">
    <search className="connector-search" aria-label={t('查找连接器')}>
      <label className="search"><Search size={17} aria-hidden="true" /><span className="sr-only">{t('搜索连接器')}</span>
        <input type="search" data-initial-focus={active || undefined} placeholder={t('搜索数据库、文件格式或连接器')} value={search} onChange={event => setSearch(event.target.value)} /></label>
      <fieldset className="connector-filters"><legend className="sr-only">{t('连接器类别')}</legend>
        {groups.map(category => <label className="connector-filter" key={category}>
          <input type="radio" name={filterId} value={category} checked={group === category} onChange={() => setGroup(category)} />
          <span>{category}</span><span className="filter-count">{matching.filter(([type]) => category === t('全部') || connectorDetails[type].group === category).length}</span>
        </label>)}
      </fieldset>
      <p className="muted small" role="status">{t('显示 {0} 个连接器', choices.length)}</p>
    </search>
    <div className="source-card-grid">
      {choices.map(([type, label]) => {
        const details = connectorDetails[type];
        return <article className="source-card connector-card" key={type}>
          <header><ConnectorIcon type={type} /><div><h3>{label}</h3><span className="muted small">{details?.group}</span></div></header>
          <p className="muted small">{details?.description}</p>
          <footer><button className="button compact" onClick={() => add(type)}>
            <Plus size={15} aria-hidden="true" />{t('配置')}<span className="sr-only"> {label}</span></button></footer>
        </article>;
      })}
    </div>
    {!choices.length && <div className="catalog-empty"><p>{t('没有匹配的连接器')}</p><p className="muted small">{t('试试其他名称，或清除类别与关键词。')}</p>
      <button className="text-button" onClick={() => { setSearch(''); setGroup(t('全部')); }}>{t('清除筛选')}</button></div>}
  </div>;
}
