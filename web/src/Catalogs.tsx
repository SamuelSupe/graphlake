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
  return <section className="listing catalog-list" aria-label="已配置的数据源">
    <div className="catalog-section-heading"><h2>已配置的数据源 <span className="catalog-count">{sources.length}</span></h2>
      <p className="muted small">配置当前模型的连接，浏览可用于建模的表与字段。</p></div>
    {!!sources.length && <search className="catalog-search" aria-label="查找已配置的数据源">
      <label className="search"><Search size={17} aria-hidden="true" /><span className="sr-only">搜索已配置的数据源</span>
        <input type="search" placeholder="搜索名称或连接器类型" value={search} onChange={event => setSearch(event.target.value)} /></label>
      <span className="muted small" role="status">{matches.length} / {sources.length} 个数据源</span>
    </search>}
    {matches.length ? <div className="source-card-grid">
      {matches.map(([name, source]) => {
        const uses = sourceUsers(model, name).length;
        return <article className="source-card" key={name} data-selected={selected === name || undefined}>
          <header><ConnectorIcon type={text(source.type)} provider={text(record(source.storage).provider)} />
            <div><h3>{name}</h3><span className="muted small">{sourceLabel(text(source.type))}</span></div></header>
          <p className="muted small">{uses ? `用于 ${uses} 个节点或边类型` : '尚未用于节点或边类型'}</p>
          <footer>
            <button className="button compact" aria-haspopup="dialog" onClick={() => configure(name)}>
              <Settings2 size={15} aria-hidden="true" />配置<span className="sr-only"> {name}</span></button>
            <button className="text-button" aria-haspopup="dialog" onClick={() => browse(name)}><FolderOpen size={15} aria-hidden="true" />浏览表与字段<span className="sr-only"> {name}</span></button>
          </footer>
        </article>;
      })}
    </div> : sources.length ? <div className="catalog-empty"><p>没有匹配的数据源</p>
      <button className="text-button" onClick={() => setSearch('')}>清除搜索</button></div> : <div className="catalog-empty"><Database size={24} aria-hidden="true" /><p>还没有数据源</p>
      <p className="muted small">选择下方连接器，填写配置后添加到当前模型。</p></div>}
  </section>;
}

export function ConnectorGallery({ add, disabled = false }: { add: (type: string) => void; disabled?: boolean }) {
  const [search, setSearch] = useState('');
  const [group, setGroup] = useState('全部');
  const filterId = useId();
  const query = search.trim().toLowerCase();
  const matching = connectorChoices.filter(([type, label]) =>
    `${type} ${label} ${connectorDetails[type]?.group} ${connectorDetails[type]?.description}`.toLowerCase().includes(query));
  const groups = ['全部', ...new Set(connectorChoices.map(([type]) => connectorDetails[type].group))];
  const choices = matching.filter(([type]) => group === '全部' || connectorDetails[type].group === group);
  return <div className="connector-gallery">
    <search className="connector-search" aria-label="查找连接器">
      <label className="search"><Search size={17} aria-hidden="true" /><span className="sr-only">搜索连接器</span>
        <input type="search" placeholder="搜索数据库、文件格式或连接器" value={search} onChange={event => setSearch(event.target.value)} /></label>
      <fieldset className="connector-filters"><legend className="sr-only">连接器类别</legend>
        {groups.map(category => <label className="connector-filter" key={category}>
          <input type="radio" name={filterId} value={category} checked={group === category} onChange={() => setGroup(category)} />
          <span>{category}</span><span className="filter-count">{matching.filter(([type]) => category === '全部' || connectorDetails[type].group === category).length}</span>
        </label>)}
      </fieldset>
      <p className="muted small" role="status">显示 {choices.length} 个连接器</p>
    </search>
    <div className="source-card-grid">
      {choices.map(([type, label]) => {
        const details = connectorDetails[type];
        return <article className="source-card connector-card" key={type}>
          <header><ConnectorIcon type={type} /><div><h3>{label}</h3><span className="muted small">{details?.group}</span></div></header>
          <p className="muted small">{details?.description}</p>
          <footer><button className="button compact" aria-haspopup="dialog" disabled={disabled} onClick={() => add(type)}>
            <Plus size={15} aria-hidden="true" />配置<span className="sr-only"> {label}</span></button></footer>
        </article>;
      })}
    </div>
    {!choices.length && <div className="catalog-empty"><p>没有匹配的连接器</p><p className="muted small">试试其他名称，或清除类别与关键词。</p>
      <button className="text-button" onClick={() => { setSearch(''); setGroup('全部'); }}>清除筛选</button></div>}
  </div>;
}
