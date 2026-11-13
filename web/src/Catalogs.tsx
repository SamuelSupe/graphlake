import { ArrowRight, Database, FolderOpen } from 'lucide-react';
import { record, sourceLabel, text, type Model } from './model';

export function Catalogs({ model, selected, configure, browse, add }: {
  model: Model;
  selected?: string;
  configure: (name: string) => void;
  browse: (name: string) => void;
  add: () => void;
}) {
  return <div className="listing catalog-list">
    <div className="panel-table directory"><table className="sources-table" role="table" aria-label="模型数据源">
      <thead role="rowgroup"><tr role="row"><th scope="col">名称</th><th scope="col">连接器</th><th scope="col">详情</th><th scope="col" className="align-right">操作</th></tr></thead>
      <tbody role="rowgroup">{Object.entries(model.sources).map(([name, source]) => {
        const kind = text(source.type);
        return <tr key={name} role="row" className="directory-row" data-selected={selected === name || undefined}>
          <th scope="row" role="rowheader" className="directory-name"><button className="model-name" aria-pressed={selected === name} onClick={() => configure(name)}><span className="model-icon"><Database size={16} aria-hidden="true" /></span><span>{name}</span></button></th>
          <td role="cell" className="directory-kind"><span className="connector-chip">{sourceLabel(kind)}</span></td>
          <td role="cell" className="directory-detail">{kind === 'postgres' ? <code>{text(source.dsn_env) || '待配置'}</code>
            : kind === 'parquet' ? Object.keys(record(source.tables)).length + ' 张映射表' : 'YAML 配置'}</td>
          <td role="cell" className="directory-actions"><div className="actions catalog-actions">
            <button className="text-button" onClick={() => browse(name)}><FolderOpen size={16} />浏览表与字段<span className="sr-only"> {name}</span></button>
            <button className="text-button" onClick={() => configure(name)}>编辑 <ArrowRight size={15} /><span className="sr-only"> {name}</span></button>
          </div></td>
        </tr>;
      })}
      {!Object.keys(model.sources).length && <tr><td colSpan={4}><div className="empty catalog-empty">
        <p>此模型还没有数据源。</p><button className="button" onClick={add}>添加数据源</button>
      </div></td></tr>}
      </tbody>
    </table></div>
  </div>;
}
