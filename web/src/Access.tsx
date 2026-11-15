import { useEffect, useState, type FormEvent } from 'react';
import { Plus, Shield } from 'lucide-react';
import { api, errorText, type ModelSummary } from './api';
import { Modal } from './ui';
import { modelIdPattern } from './model';

export const roles = {
  query: '查询',
  export: '导出',
  modeler: '建模',
  publisher: '发布',
  auditor: '只读审计',
} as const;
export type Role = keyof typeof roles;
export type Grants = Record<string, Role[]>;
type User = { username: string; version: string; disabled: boolean; service?: boolean; roles: string[] };
type RolePartition = { column: string; op: 'eq'; value: string | number | boolean };
type ModelRole = { version: string; grants: Grants; members: string[]; partitions?: Record<string, RolePartition> };

export function Access({ models }: { models: ModelSummary[] }) {
  const [users, setUsers] = useState<User[]>();
  const [groups, setGroups] = useState<Record<string, ModelRole>>();
  const [groupEdit, setGroupEdit] = useState<string>();
  const [edit, setEdit] = useState<User | 'new'>();
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    const options = { signal: controller.signal };
    Promise.all([api<User[]>('/users', options), api<Record<string, ModelRole>>('/roles', options)])
      .then(([directory, modelRoles]) => { setUsers(directory); setGroups(modelRoles); }, cause => {
      if (!controller.signal.aborted) setError(errorText(cause));
    });
    return () => controller.abort();
  }, [attempt]);
  function changed(message: string) {
    setNotice(message);
    setUsers(undefined);
    setGroups(undefined);
    setEdit(undefined);
    setGroupEdit(undefined);
    setAttempt(value => value + 1);
  }
  return <main className="workbench">
    <header className="workspace-header">
      <div className="workspace-title"><h1>权限管理</h1></div>
      <div className="workspace-context">
        <button className="button compact" onClick={() => setEdit('new')} disabled={!users}><Plus size={16} />新建成员</button>
        <button className="button primary compact" onClick={() => setGroupEdit('')} disabled={!groups}><Plus size={16} />新建角色</button>
      </div>
    </header>
    <div className="listing access-page">
      <p role="status" className={notice ? 'notice' : 'sr-only'}>{notice}</p>
      {error && <p className="notice error" role="alert">{error}{' '}
        <button className="text-button" onClick={() => { setError(''); setAttempt(value => value + 1); }}>重新读取</button>
      </p>}
      <h2>角色</h2>
      <div className="panel-table access-table">
        <table>
          <caption className="sr-only">角色权限与成员</caption>
          <thead><tr><th scope="col">角色</th><th scope="col">模型权限</th><th scope="col">成员</th><th scope="col">操作</th></tr></thead>
          <tbody>{Object.entries(groups ?? {}).map(([name, group]) => <tr key={name}>
            <th scope="row"><code>{name}</code></th>
            <td>{Object.keys(group.grants).length ? <ul className="access-grants">{Object.entries(group.grants).map(([model, assigned]) =>
              <li key={model}><code>{model}</code> {assigned.map(role => roles[role]).join('、')}{group.partitions?.[model] && <small> · {group.partitions[model].column} = {JSON.stringify(group.partitions[model].value)}</small>}</li>)}</ul> : '暂无权限'}</td>
            <td>{group.members.length ? group.members.join('、') : '暂无成员'}</td>
            <td><button className="button compact" aria-label={'编辑角色 ' + name} onClick={() => setGroupEdit(name)}>编辑</button></td>
          </tr>)}
          {groups && !Object.keys(groups).length && <tr><td colSpan={4}>暂无自定义角色</td></tr>}
          </tbody>
        </table>
      </div>
      <h2>成员</h2>
      <div className="panel-table access-table">
        <table>
          <caption className="sr-only">成员及所属角色</caption>
          <thead><tr><th scope="col">成员</th><th scope="col">状态</th><th scope="col">所属角色</th><th scope="col">操作</th></tr></thead>
          <tbody>
            <tr><th scope="row"><Shield size={16} aria-hidden="true" /> <code>admin</code></th><td>启用</td><td>系统管理员 · 全部模型</td><td>内置账号</td></tr>
            {users?.map(user => <tr key={user.username}>
              <th scope="row"><code>{user.service ? 'svc/' : ''}{user.username}</code>{user.service && <small> · 服务账号</small>}</th>
              <td><span className={'status ' + (user.disabled ? 'failed' : 'passed')}>{user.disabled ? '停用' : '启用'}</span></td>
              <td>{user.roles.length ? user.roles.join('、') : '暂无角色'}</td>
              <td><button className="button compact" aria-label={'编辑成员 ' + user.username} onClick={() => setEdit(user)}>编辑</button></td>
            </tr>)}
          </tbody>
        </table>
      </div>
      {!users && !error && <p className="empty" role="status">正在读取成员…</p>}
    </div>
    {edit && <Member item={edit === 'new' ? undefined : edit} close={() => setEdit(undefined)}
      removed={name => changed(name + ' 已删除')} saved={user => changed(user.disabled ? user.username + ' 已停用' : user.username + ' 的账号已保存')} />}
    {groupEdit !== undefined && <RoleEditor name={groupEdit} item={groups && Object.hasOwn(groups, groupEdit) ? groups[groupEdit] : undefined} models={models} users={users ?? []}
      close={() => setGroupEdit(undefined)} saved={changed} />}
  </main>;
}

function Member({ item, close, removed, saved }: {
  item?: User; close: () => void; removed: (name: string) => void; saved: (user: User) => void;
}) {
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [service, setService] = useState(item?.service ?? false);
  async function remove() {
    if (!item || !window.confirm('删除 ' + item.username + ' 并将其从所有角色移除？')) return;
    setError('');
    setBusy(true);
    try {
      await api('/users/' + encodeURIComponent(item.username), { method: 'DELETE', body: JSON.stringify(item.version) });
      removed(item.username);
    } catch (cause) { setError(errorText(cause)); }
    finally { setBusy(false); }
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const username = item?.username ?? String(data.get('username'));
    const secret = String(data.get('secret') ?? '');
    if (secret !== String(data.get('confirm') ?? '')) {
      setError(service ? '两次输入的令牌不一致' : '两次输入的密码不一致');
      return;
    }
    setError('');
    setBusy(true);
    try {
      saved(await api<User>('/users/' + encodeURIComponent(username), {
        method: 'PUT', body: JSON.stringify({
          version: item?.version, service, disabled: data.has('disabled'),
          ...(service ? { token: secret || undefined } : { password: secret || undefined }),
        }),
      }));
    } catch (cause) { setError(errorText(cause)); }
    finally { setBusy(false); }
  }

  return <Modal title={item ? '编辑成员 · ' + item.username : '新建成员'} close={() => { if (!busy) close(); }}>
    <form className="dialog-form access-form" method="post" onSubmit={submit} aria-busy={busy}>
      {!item && <fieldset className="access-members">
        <legend>账号类型</legend>
        <div className="access-role-grid">
          <label className="access-check"><input type="radio" name="kind" value="user" checked={!service} onChange={() => setService(false)} />人工账号</label>
          <label className="access-check"><input type="radio" name="kind" value="service" checked={service} onChange={() => setService(true)} />服务账号</label>
        </div>
      </fieldset>}
      <label htmlFor="access-username">{service ? '服务账号名称' : '用户名'}<input id="access-username" name="username"
        defaultValue={item?.username} readOnly={!!item} autoComplete={service ? 'off' : 'username'}
        aria-describedby={service ? 'access-service-help' : undefined} pattern={modelIdPattern} maxLength={64}
        required data-initial-focus={!item || undefined} /></label>
      {service && <p className="muted" id="access-service-help">客户端用户名为 <code>svc/{item?.username ?? '账号名称'}</code>。</p>}
      <div className="access-passwords" key={String(service)}>
        <label htmlFor="access-secret">{service ? (item ? '新令牌（选填）' : '初始令牌') : (item ? '新密码（选填）' : '初始密码')}
          <input id="access-secret" name="secret" type="password" autoComplete={service ? 'off' : 'new-password'}
            minLength={service ? 32 : 15} maxLength={256} required={!item} aria-describedby="access-password-help" /></label>
        <label htmlFor="access-confirm">{service ? '确认令牌' : '确认密码'}<input id="access-confirm" name="confirm"
          type="password" autoComplete={service ? 'off' : 'new-password'} maxLength={256} required={!item} /></label>
      </div>
      <p className="muted" id="access-password-help">{service ? '使用随机生成的 32 至 256 个可打印 ASCII 字符。' : '密码长度为 15 至 128 个字符。'}{item && '留空保留原凭据。'}</p>
      <label className="access-check"><input type="checkbox" name="disabled" defaultChecked={item?.disabled} />停用账号</label>
      <p className="muted">将账号加入角色后才可访问对应的模型。</p>
      {item && <p className="muted">{service ? '保存后会关闭此账号的现有连接。' : '保存后该成员需要重新登录。'}</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      <div className="dialog-actions">{item && <button type="button" className="button danger" disabled={busy} onClick={() => void remove()}>删除成员</button>}<button type="button" className="button" onClick={close} disabled={busy}>取消</button>
        <button type="submit" className="button primary" disabled={busy}>{busy ? '保存中…' : '保存成员'}</button></div>
    </form>
  </Modal>;
}

function RoleEditor({ name, item, models, users, close, saved }: {
  name: string; item?: ModelRole; models: ModelSummary[]; users: User[];
  close: () => void; saved: (message: string) => void;
}) {
  const [grants, setGrants] = useState<Grants>(item?.grants ?? {});
  const [scopes, setScopes] = useState<Record<string, { column: string; value: string }>>(() =>
    Object.fromEntries(Object.entries(item?.partitions ?? {}).map(([id, scope]) => [id, {
      column: scope.column, value: JSON.stringify(scope.value),
    }])));
  const [members, setMembers] = useState(item?.members ?? []);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const labels = new Map(models.map(model => [model.id, model.name]));
  for (const id of Object.keys(grants)) if (!labels.has(id)) labels.set(id, id);

  function change(id: string, role: Role, checked: boolean) {
    setGrants(previous => {
      const next = { ...previous };
      const assigned = (Object.hasOwn(previous, id) ? previous[id] : []).filter(value => value !== role);
      if (checked) assigned.push(role);
      if (assigned.length) next[id] = assigned;
      else delete next[id];
      return next;
    });
  }
  async function persist(key: string, remove: boolean) {
    setBusy(true);
    setError('');
    try {
      const partitions = Object.fromEntries(Object.entries(scopes)
        .filter(([id, term]) => Object.hasOwn(grants, id) && grants[id].includes('query') && (term.column.trim() || term.value.trim()))
        .map(([id, term]) => {
          if (!term.column.trim()) throw new Error('请填写 ' + id + ' 的租户字段。');
          let value: unknown;
          try { value = JSON.parse(term.value); } catch { throw new Error('请为 ' + id + ' 输入 JSON 字符串、数字或布尔值。'); }
          if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean'
            || typeof value === 'number' && (!Number.isFinite(value) || Number.isInteger(value) && !Number.isSafeInteger(value))) {
            throw new Error(id + ' 的租户值须为字符串、布尔值或安全范围内的数字。');
          }
          return [id, { column: term.column.trim(), op: 'eq', value }];
        }));
      await api('/roles/' + encodeURIComponent(key), {
        method: remove ? 'DELETE' : 'PUT',
        body: JSON.stringify(remove ? item?.version : { version: item?.version, grants, members, partitions }),
      });
      saved(key + (remove ? ' 已删除' : ' 已保存'));
    } catch (cause) { setError(errorText(cause)); }
    finally { setBusy(false); }
  }

  return <Modal title={item ? '编辑角色 · ' + name : '新建角色'} close={() => { if (!busy) close(); }}>
    <form className="dialog-form access-form" aria-busy={busy} onSubmit={event => {
      event.preventDefault();
      void persist(name || String(new FormData(event.currentTarget).get('role')), false);
    }}>
      <label>角色名称<input name="role" defaultValue={name} readOnly={!!item}
        pattern={modelIdPattern} maxLength={64} required data-initial-focus={!item || undefined} /></label>
      <fieldset className="access-members">
        <legend>加入角色的成员</legend>
        <div className="access-role-grid">{users.map(user => <label key={user.username} className="access-check">
          <input type="checkbox" checked={members.includes(user.username)} onChange={event => {
            const checked = event.target.checked;
            setMembers(previous => checked ? [...previous, user.username] : previous.filter(value => value !== user.username));
          }} />{user.service ? 'svc/' : ''}{user.username}{user.disabled && '（已停用）'}
        </label>)}</div>
        {!users.length && <p className="muted">新建成员后可以加入角色。</p>}
      </fieldset>
      <div className="access-scopes">
        <h3>按模型授权</h3>
        <p className="muted">查询可运行语句和探索图。导出还需同一模型的查询权限。建模可编辑草稿。发布可切换生效版本。只读审计可查看配置和版本历史。</p>
        {[...labels].map(([id, name]) => <fieldset key={id}>
          <legend>{name} <code>{name !== id && id}</code></legend>
          <div className="access-role-grid">
            {Object.entries(roles).map(([key, label]) => {
              const role = key as Role;
              return <label className="access-check" key={role}><input type="checkbox" checked={Object.hasOwn(grants, id) && grants[id].includes(role)}
                onChange={event => change(id, role, event.target.checked)} />{label}</label>;
            })}
          </div>
          {Object.hasOwn(grants, id) && grants[id].includes('query') && <details className="role-partition" open={!!scopes[id]?.column || undefined}>
            <summary>限定租户范围</summary>
            <div className="access-passwords">
              <label>租户字段<input name={id + '-partition-column'} value={scopes[id]?.column ?? ''}
                onChange={event => {
                  const column = event.currentTarget.value;
                  setScopes(previous => ({ ...previous, [id]: { column, value: previous[id]?.value ?? '' } }));
                }} /></label>
              <label>租户值（JSON）<input name={id + '-partition-value'} value={scopes[id]?.value ?? ''}
                onChange={event => {
                  const value = event.currentTarget.value;
                  setScopes(previous => ({ ...previous, [id]: { column: previous[id]?.column ?? '', value } }));
                }} /></label>
            </div>
            <p className="muted">节点、边及端点业务键须包含同一租户字段。两项均留空表示查询整个模型。</p>
          </details>}
        </fieldset>)}
        {!labels.size && <p className="muted">创建图模型后可分配模型权限。</p>}
      </div>
      {item && <p className="muted">角色变更后，受影响的成员需重新登录。</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      {confirmDelete && <p role="status">删除角色后会撤销该角色授予的全部权限。</p>}
      <div className="dialog-actions">
        {item && <button type="button" className="button danger" disabled={busy} onClick={() => {
          if (confirmDelete) void persist(name, true); else setConfirmDelete(true);
        }}>{confirmDelete ? '确认删除' : '删除角色'}</button>}
        <button type="button" className="button" onClick={confirmDelete ? () => setConfirmDelete(false) : close} disabled={busy}>取消</button>
        {!confirmDelete && <button type="submit" className="button primary" disabled={busy}>{busy ? '保存中…' : '保存角色'}</button>}
      </div>
    </form>
  </Modal>;
}
