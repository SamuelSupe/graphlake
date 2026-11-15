import { Cloud, Database, FileJson, FileSpreadsheet } from 'lucide-react';

const logos = import.meta.glob<string>('./assets/connectors/*.svg', {
  eager: true,
  import: 'default',
  query: '?url&no-inline',
});

export function ConnectorIcon({ type, provider }: { type: string; provider?: string }) {
  const name = type === 'object_storage' && provider ? 'storage-' + provider : type.replace(/_native$/, '');
  const logo = logos['./assets/connectors/' + name + '.svg'];
  const Icon = type === 'csv' ? FileSpreadsheet : type === 'jsonl' ? FileJson : type === 'object_storage' ? Cloud : Database;
  return <span className="connector-icon" aria-hidden="true">
    {logo ? <img src={logo} alt="" width={30} height={30} decoding="async" /> : <Icon size={22} />}
  </span>;
}
