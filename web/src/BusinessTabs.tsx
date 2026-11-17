export function BusinessTabs<T extends string>({ id, label, selected, choices, change }: {
  id: string;
  label: string;
  selected: T;
  choices: readonly { id: T; label: string }[];
  change: (id: T) => void;
}) {
  return <div className="workspace-tabs business-tabs" role="tablist" aria-label={label}>
    {choices.map((choice, position) => <button key={choice.id} id={id + '-' + choice.id} type="button" role="tab"
      aria-controls={id + '-panel'} aria-selected={selected === choice.id} tabIndex={selected === choice.id ? 0 : -1}
      onClick={() => change(choice.id)} onKeyDown={event => {
        const next = event.key === 'ArrowRight' ? (position + 1) % choices.length
          : event.key === 'ArrowLeft' ? (position + choices.length - 1) % choices.length
          : event.key === 'Home' ? 0 : event.key === 'End' ? choices.length - 1 : null;
        if (next === null) return;
        event.preventDefault();
        change(choices[next].id);
        document.getElementById(id + '-' + choices[next].id)?.focus();
      }}>{choice.label}</button>)}
  </div>;
}
