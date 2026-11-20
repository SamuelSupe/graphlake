import { useEffect, useMemo, useState } from 'react';
import { api, ApiError, errorText, type VersionHistory } from './api.ts';
import { releaseRows, versionsPerPage, type Archive } from './modelChanges';

// A rollback can make the same revision appear repeatedly. Compare events in
// journal order, while fetching their immutable archives only once per page.
export function useVersionPage(id: string, history: VersionHistory | undefined, index: number) {
  const start = index - index % versionsPerPage;
  const [archives, setArchives] = useState<Map<string, Archive>>(new Map());
  const revisions = useMemo(() => [...new Set(history?.versions.slice(start, start + versionsPerPage + 1)
    .map(entry => entry.revision))], [history, start]);
  useEffect(() => {
    const controller = new AbortController();
    setArchives(new Map());
    Promise.all(revisions.map(async revision => {
      try {
        const { yaml } = await api<{ yaml: string }>('/models/' + id + '/history/' + encodeURIComponent(revision), { signal: controller.signal });
        return [revision, { yaml }] as const;
      } catch (cause) {
        const repair = cause instanceof ApiError && cause.code === 'model_archive_unavailable' ? revision : undefined;
        return [revision, { error: errorText(cause), repair }] as const;
      }
    })).then(entries => { if (!controller.signal.aborted) setArchives(new Map<string, Archive>(entries)); });
    return () => controller.abort();
  }, [id, revisions]);

  const rows = useMemo(() => releaseRows(history, start, archives), [history, start, archives]);
  return { rows, selected: rows[index - start], start };
}
