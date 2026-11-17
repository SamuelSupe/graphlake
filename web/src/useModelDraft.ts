import { useEffect, useState, type SetStateAction } from 'react';
import { YAMLParseError } from 'yaml';
import { errorText } from './api';
import { modelOf } from './model';

function inspectYaml(yaml: string) {
  try {
    return { yaml, model: modelOf(yaml) };
  } catch (cause) {
    return { yaml, error: errorText(cause), line: cause instanceof YAMLParseError ? cause.linePos?.[0].line : undefined };
  }
}

export function useModelDraft(initial: string) {
  const [{ yaml, preview }, setDraft] = useState(() => ({ yaml: initial, preview: inspectYaml(initial) }));
  const current = preview.yaml === yaml ? preview : undefined;

  useEffect(() => {
    if (current) return;
    const timer = window.setTimeout(() => {
      const result = inspectYaml(yaml);
      setDraft(state => state.yaml === yaml ? { yaml, preview: result } : state);
    }, 250);
    return () => window.clearTimeout(timer);
  }, [yaml, current]);

  function setYaml(value: SetStateAction<string>) {
    setDraft(state => {
      const next = typeof value === 'function' ? value(state.yaml) : value;
      return next === state.yaml ? state : { yaml: next, preview: inspectYaml(next) };
    });
  }

  function editText(value: string) {
    setDraft(state => ({ ...state, yaml: value }));
  }

  // Actions inspect the exact text they will use, including edits made within the pause.
  function inspectCurrent() {
    if (current) return current;
    const result = inspectYaml(yaml);
    setDraft({ yaml, preview: result });
    return result;
  }

  return { yaml, current, preview, setYaml, editText, inspectCurrent };
}
