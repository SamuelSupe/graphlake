import { t } from './locale.ts';
import { useId, useMemo, useRef, useState } from 'react';
import { Box, ChevronRight, Database, GitFork, FolderOpen, Search, Settings2 } from 'lucide-react';
import type { GraphSelection } from './GraphCanvas';
import { ConnectorIcon } from './ConnectorIcon';
import { connectorChoices, connectorDetails } from './connectors';
import { record, mappedTypes, sourceLabel, text, type Model } from './model';

export function Catalogs({ model, search, setSearch, selected, configure, browse, openMapping }: {
  model: Model;
  search: string;
  setSearch: (value: string) => void;
  selected?: string;
  configure: (name: string) => void;
  browse: (name: string) => void;
  openMapping: (selection: GraphSelection) => void;
}) {
  const searchInput = useRef<HTMLInputElement>(null);
  const query = search.trim().toLowerCase();
  const sources = useMemo(() => Object.entries(model.sources).map(([name, source]) => ({
    name, source, types: mappedTypes(model, name),
  })), [model]);
  const matches = sources.filter(({ name, source, types }) => [name, sourceLabel(text(source.type)), ...types.map(type => type.name)]
    .some(value => value.toLowerCase().includes(query)));
  function typeLink(type: GraphSelection) {
    const Icon = type.section === 'nodes' ? Box : GitFork;
    return <button key={type.section + ':' + type.name} className="mapping-link" onClick={() => openMapping(type)}
      aria-label={type.section === 'nodes' ? t('查看节点 {0} 的映射', type.name) : t('查看边 {0} 的映射', type.name)}>
      <Icon size={14} aria-hidden="true" />{type.name}</button>;
  }
  return <section className="listing catalog-list" aria-label={t('已配置的数据源')}>
    <div className="catalog-section-heading">
      <div>
        <h2>{t('已配置的数据源 ')}<span className="catalog-count">{sources.length}</span></h2>
        <p className="muted small">{t('配置当前模型的连接，浏览可用于建模的表与字段。')}</p>
      </div>
      {!!sources.length && <search className="catalog-search" aria-label={t('查找已配置的数据源')}>
        <label className="search"><Search size={17} aria-hidden="true" /><span className="sr-only">{t('搜索已配置的数据源')}</span>
          <input ref={searchInput} type="search" placeholder={t('搜索数据源、连接器或映射类型')} value={search} onChange={event => setSearch(event.target.value)} /></label>
        <span className="muted small" role="status">{matches.length} / {sources.length}{t(' 个数据源')}</span>
      </search>}
    </div>
    {matches.length ? <div className="catalog-sources">
      {matches.map(({ name, source, types }) => {
        const displayed = query && types.length > 3
          ? [...types.filter(type => type.name.toLowerCase().includes(query)), ...types.filter(type => !type.name.toLowerCase().includes(query))]
          : types;
        return <article className="source-card" key={name} data-selected={selected === name || undefined}>
          <header><ConnectorIcon type={text(source.type)} provider={text(record(source.storage).provider)} />
            <div><h3>{name}</h3><span className="muted small">{sourceLabel(text(source.type))}</span></div></header>
          {types.length ? <div className="source-mappings"><small>{t('映射类型')}</small>
            <div>{displayed.slice(0, 3).map(typeLink)}</div>
            {types.length > 3 && <details><summary>{t('另有 {0} 个类型', types.length - 3)}</summary><div>{displayed.slice(3).map(typeLink)}</div></details>}
          </div> : <p className="muted small">{t('尚未用于节点或边类型')}</p>}
          <footer>
            <button className="button compact" aria-haspopup="dialog" onClick={() => configure(name)}>
              <Settings2 size={15} aria-hidden="true" />{t('配置')}<span className="sr-only"> {name}</span></button>
            <button className="text-button" aria-haspopup="dialog" onClick={() => browse(name)}><FolderOpen size={15} aria-hidden="true" />{t('浏览表与字段')}<span className="sr-only"> {name}</span></button>
          </footer>
        </article>;
      })}
    </div> : sources.length ? <div className="catalog-empty"><p>{t('没有匹配的数据源')}</p>
      <button className="text-button" onClick={() => { setSearch(''); searchInput.current?.focus(); }}>{t('清除搜索')}</button></div> : <div className="catalog-empty"><Database size={24} aria-hidden="true" /><p>{t('还没有数据源')}</p>
      <p className="muted small">{t('使用「添加数据源」为此模型选择连接器。')}</p></div>}
  </section>;
}

export function ConnectorGallery({ add, active }: { add: (type: string, trigger: HTMLButtonElement) => void; active: boolean }) {
  const searchInput = useRef<HTMLInputElement>(null);
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
        <input ref={searchInput} type="search" data-initial-focus={active || undefined} placeholder={t('搜索数据库、文件格式或连接器')} value={search} onChange={event => setSearch(event.target.value)} /></label>
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
        return <button type="button" className="connector-card" key={type} onClick={event => add(type, event.currentTarget)}
          aria-label={t('配置 {0}', label)} aria-describedby={filterId + '-' + type}>
          <ConnectorIcon type={type} />
          <span className="connector-copy"><strong>{label}</strong>
            <span className="small muted" id={filterId + '-' + type}>{details.description}</span></span>
          <ChevronRight size={16} aria-hidden="true" />
        </button>;
      })}
    </div>
    {!choices.length && <div className="catalog-empty"><p>{t('没有匹配的连接器')}</p><p className="muted small">{t('试试其他名称，或清除类别与关键词。')}</p>
      <button className="text-button" onClick={() => { setSearch(''); setGroup(t('全部')); searchInput.current?.focus(); }}>{t('清除筛选')}</button></div>}
  </div>;
}
