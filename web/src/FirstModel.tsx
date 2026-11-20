import { useState } from 'react';
import { Activity, ArrowLeft, ArrowRight, Database, FileUp, Network, ReceiptText, RefreshCw, UserRound } from 'lucide-react';
import { t } from './locale';
import { Modal } from './ui';

const customers = new Map([['C1', 'Alice'], ['C2', 'Bob'], ['C3', 'Carol']]);
const orders = [
  { id: 'O101', buyer: 'C1' },
  { id: 'O100', buyer: 'C1' },
  { id: 'O102', buyer: 'C3' },
];
const payments = [
  { id: 'P1', order: 'O100', payer: 'C1' },
  { id: 'P2', order: 'O101', payer: 'C2' },
];

function OrderPath({ order }: { order: typeof orders[number] }) {
  const payment = payments.find(row => row.order === order.id);
  return <div className="example-path" role="group" aria-label={t('订单关系')}>
    <div><UserRound aria-hidden="true" /><strong>{customers.get(order.buyer)}</strong><small>{t('下单人')}</small></div>
    <ArrowLeft aria-hidden="true" />
    <div><ReceiptText aria-hidden="true" /><strong>{order.id}</strong><small>{t('订单')}</small></div>
    {payment ? <ArrowRight aria-hidden="true" /> : <span aria-hidden="true" className="example-gap" />}
    <div className={payment ? '' : 'example-missing'}>{payment ? <UserRound aria-hidden="true" /> : <ReceiptText aria-hidden="true" />}<strong>{payment ? customers.get(payment.payer) : '—'}</strong><small>{payment ? t('付款人') : t('无付款记录')}</small></div>
  </div>;
}

function OrderExample({ close, connect }: { close: () => void; connect: () => void }) {
  const [selected, setSelected] = useState(orders[0].id);
  const order = orders.find(row => row.id === selected)!;
  const payment = payments.find(row => row.order === selected);
  return <Modal title={t('探索订单关系')} description={t('内置样例数据，仅在当前浏览器中查看。')} layout="center" size="medium" close={close}>
    <div className="panel-body example-body">
      <label className="example-order">{t('选择订单')}<select data-initial-focus value={selected} onChange={event => setSelected(event.target.value)}>
        {orders.map(row => <option key={row.id}>{row.id}</option>)}
      </select></label>
      <div className="example-result" aria-live="polite" aria-atomic="true">
        <OrderPath order={order} />
        <p>{!payment ? t('{0} 下单，付款表中还没有这笔订单的记录。', customers.get(order.buyer)!)
          : payment.payer === order.buyer ? t('{0} 下单并付款。', customers.get(order.buyer)!)
          : t('{0} 下单，{1} 代付。', customers.get(order.buyer)!, customers.get(payment.payer)!)}</p>
      </div>
      <details className="example-evidence"><summary>{t('查看来源记录')}</summary>
        <dl><dt>{t('订单表')}</dt><dd><code>order_id = {order.id} · buyer_id = {order.buyer}</code></dd>
          <dt>{t('付款表')}</dt><dd>{payment ? <code>payment_id = {payment.id} · order_id = {payment.order} · payer_id = {payment.payer}</code> : t('没有匹配的付款记录')}</dd>
          <dt>{t('客户表')}</dt><dd>{[...new Set([order.buyer, ...(payment ? [payment.payer] : [])])].map(id =>
            <code key={id}>{id} · {customers.get(id)}</code>)}</dd></dl>
      </details>
    </div>
    <footer className="panel-footer"><button className="button" onClick={close}>{t('返回图模型')}</button>
      <button className="button primary" onClick={connect}>{t('连接数据源')}</button></footer>
  </Modal>;
}

export default function FirstModel({ create, connect, importModel, importError, retry, refreshing, health }: {
  create: () => void; connect: () => void;
  importModel: (file: File) => void; importError: string; retry: () => void; refreshing: boolean; health: () => void;
}) {
  const [example, setExample] = useState(false);
  function startConnection() {
    setExample(false);
    connect();
  }
  return <main className="workbench model-directory first-model">
    <header className="first-heading"><h1>{t('图模型')}</h1><p>{t('把不同来源的记录连起来，查看它们之间的关系。')}</p></header>
    <section className="first-example" aria-labelledby="example-question">
      <div><p className="first-kicker">{t('内置订单示例')}</p><h2 id="example-question">{t('谁下了单，谁付了款？')}</h2>
        <p>{t('从一笔订单出发，沿两条关系找到下单人和付款人。')}</p>
        <button className="button primary" onClick={() => setExample(true)}>{t('探索订单示例')}<ArrowRight size={16} aria-hidden="true" /></button>
      </div>
      <OrderPath order={orders[0]} />
    </section>
    <section className="first-own" aria-labelledby="first-own-title"><h2 id="first-own-title">{t('使用自己的数据')}</h2>
      <div className="first-actions">
        <button className="first-action" onClick={startConnection}>
          <Database aria-hidden="true" /><span><strong>{t('连接数据源')}</strong><small>{t('先选数据库或文件，再用这份连接建立模型草稿。')}</small></span><ArrowRight aria-hidden="true" />
        </button>
        <label className="first-action"><FileUp aria-hidden="true" /><span><strong>{t('导入 YAML')}</strong><small>{t('从已有的图模型定义继续。')}</small></span><ArrowRight aria-hidden="true" />
          <input type="file" className="sr-only" aria-label={t('导入 YAML')} accept=".yaml,.yml" onChange={event => {
            const file = event.target.files?.[0]; event.target.value = ''; if (file) importModel(file);
          }} />
        </label>
      </div>
      {importError && <p className="notice error" role="alert">{importError}</p>}
      <button className="text-button first-blank" onClick={create}><Network size={15} aria-hidden="true" />{t('新建空白模型')}</button>
    </section>
    <footer className="first-footer"><button className="text-button" onClick={retry} disabled={refreshing}><RefreshCw size={15} aria-hidden="true" />{t('刷新模型列表')}</button>
      <button className="text-button" onClick={health}><Activity size={15} aria-hidden="true" />{t('运行健康')}</button></footer>
    {example && <OrderExample close={() => setExample(false)} connect={startConnection} />}
  </main>;
}
