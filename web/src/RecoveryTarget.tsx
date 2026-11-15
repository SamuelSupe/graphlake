import { useState, type FormEvent } from 'react';
import { ArrowUpRight } from 'lucide-react';
import { Modal } from './ui';

export default function RecoveryTarget({ local, close }: { local: boolean; close: () => void }) {
  const [address, setAddress] = useState(local ? window.location.origin : '');
  const [error, setError] = useState('');

  function open(event: FormEvent) {
    event.preventDefault();
    try {
      const site = new URL(address.trim());
      if (site.protocol !== 'https:' || site.username || site.password || site.search || site.hash
        || !['/', '/recovery', '/recovery/'].includes(site.pathname)) throw new Error();
      window.open(site.origin + '/recovery', '_blank', 'noopener,noreferrer');
      close();
    } catch {
      setError('请输入目标站的 HTTPS 地址。');
    }
  }

  return <Modal title="恢复备份" description="在目标站选择恢复点、检查并确认恢复。" close={close}>
    <form className="panel-layout" onSubmit={open}>
      <div className="panel-body form-fields">
        <label>目标站地址<input type="url" required data-initial-focus placeholder="https://recovery.example.com"
          aria-invalid={!!error} value={address} autoCapitalize="none" spellCheck={false}
          onChange={event => { setAddress(event.target.value); setError(''); }} /></label>
        {error && <p className="inline-error" role="alert">{error}</p>}
      </div>
      <footer className="panel-footer">
        <button type="button" className="button" onClick={close}>取消</button>
        <button type="submit" className="button primary">打开目标站<ArrowUpRight size={15} aria-hidden="true" /></button>
      </footer>
    </form>
  </Modal>;
}
