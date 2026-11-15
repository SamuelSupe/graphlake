import { Database, FolderOpen } from 'lucide-react';
import { sourceLabel, text, type Model } from './model';

export function Catalogs({ model, selected, configure, browse }: {
  model: Model;
  selected?: string;
  configure: (name: string) => void;
  browse: (name: string) => void;
}) {
  if (!Object.keys(model.sources).length) return <div className="catalog-list listing"><div className="directory-empty panel-table">
    <Database size={28} aria-hidden="true" /><h2>还没有数据源</h2><p>点击「添加数据源」建立连接，再为节点和边选择来源表。</p>
  </div></div>;
  return <div className="listing catalog-list">
    <div className="panel-table directory"><table className="sources-table" role="table" aria-label="模型数据源">
      <thead role="rowgroup"><tr role="row"><th scope="col">名称</th><th scope="col">连接器</th><th scope="col" className="align-right">操作</th></tr></thead>
      <tbody role="rowgroup">{Object.entries(model.sources).map(([name, source]) =>
        <tr key={name} role="row" className="directory-row" data-selected={selected === name || undefined}>
          <th scope="row" role="rowheader" className="directory-name"><button className="model-name" aria-pressed={selected === name}
            onClick={() => configure(name)}><span className="model-icon"><Database size={16} aria-hidden="true" /></span><span>{name}</span></button></th>
          <td role="cell" className="directory-kind"><span className="connector-chip">{sourceLabel(text(source.type))}</span></td>
          <td role="cell" className="directory-actions"><div className="actions catalog-actions">
            <button className="text-button" onClick={() => browse(name)}><FolderOpen size={16} />浏览表与字段<span className="sr-only"> {name}</span></button>
          </div></td>
        </tr>)}
      </tbody>
    </table></div>
  </div>;
}
