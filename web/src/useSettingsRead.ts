import { useEffect, useEffectEvent, useRef, useState } from 'react';
import { api, errorText } from './api';

export function useSettingsRead<T>(path: string, active: boolean, readOnEntry: boolean,
  receive: (value: T, explicit: boolean) => void, fail: (message: string) => void) {
  const request = useRef<AbortController | null>(null);
  const [loading, setLoading] = useState(false);

  async function read(explicit: boolean) {
    if (!active || request.current) return;
    const controller = new AbortController();
    request.current = controller;
    setLoading(true);
    try {
      const value = await api<T>(path, { signal: controller.signal });
      if (!controller.signal.aborted) receive(value, explicit);
    } catch (cause) {
      if (!controller.signal.aborted) fail(errorText(cause));
    } finally {
      if (!controller.signal.aborted) { request.current = null; setLoading(false); }
    }
  }

  const enter = useEffectEvent(() => { if (readOnEntry) void read(false); });
  useEffect(() => {
    if (active) enter();
    else setLoading(false);
    return () => { request.current?.abort(); request.current = null; };
  }, [active, path]);

  return { loading, reload: () => read(true) };
}
