"""Verify online local-table backups and recovery using the disposable Linux fixtures."""

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from cluster_scaling_benchmark import allocate_ports, init_credentials, ok

GRAPH_QUERY = 'SELECT id, target, version FROM "Account" ORDER BY id'
MODEL = '/api/v1/models/accounts'
TABLE = MODEL + '/local-tables/accounts'
BEFORE = [['1', '2', '11'], ['2', '2', '10'], ['3', '3', '10']]
AFTER = [['2', '2', '10'], ['3', '3', '10'], ['4', '3', '13']]


def until(check, seconds=30):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        result = check()
        if result:
            return result
        time.sleep(.06)
    raise TimeoutError(f'timed out waiting for {check.__name__}')


def pg_env(dsn):
    addr = urlsplit(dsn)
    return {**os.environ, 'PGHOST': addr.hostname, 'PGPORT': str(addr.port or 5432),
            'PGUSER': unquote(addr.username), 'PGPASSWORD': unquote(addr.password),
            'PGDATABASE': unquote(addr.path[1:]),
            'PGSSLMODE': parse_qs(addr.query).get('sslmode', ['disable'])[0]}


def postgres(dsn, *args):
    return subprocess.check_output(['psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', *args],
                                   env=pg_env(dsn)).decode().strip()


@contextmanager
def locked_table(dsn, table):
    with subprocess.Popen(['psql', '-XqAt', '-v', 'ON_ERROR_STOP=1'],
                          env=pg_env(dsn),
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as cli:
        cli.stdin.write(f'BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;\n\\echo locked\n')
        cli.stdin.flush()
        assert cli.stdout.readline().strip() == 'locked'
        try:
            yield
        finally:
            cli.stdin.write('ROLLBACK;\n\\q\n')
            cli.stdin.flush()


class Site:
    def __init__(self, args, root, source_dsn, *, target=False):
        self.cli = args.cli
        self.root = root
        self.source_dsn = source_dsn
        self.port, self.worker_port = allocate_ports(2)
        self.public = f'https://127.0.0.1:{self.port}'
        self.base = f'http://127.0.0.1:{self.port}'
        self.cookie = ''
        self.read_retries = []
        self.password = secrets.token_hex(16)
        self.token = secrets.token_hex(32)
        self.env = {**os.environ, 'S6_WORKER_TOKEN': secrets.token_hex(32),
                    'GRAPHLAKE_RECOVERY_TOKEN': self.token}
        if target and os.environ.get('GRAPHLAKE_RECOVERY_TEST_AWS_ENDPOINT'):
            self.env['AWS_ENDPOINT'] = os.environ['GRAPHLAKE_RECOVERY_TEST_AWS_ENDPOINT']
        etcd = os.environ.get('GRAPHLAKE_RECOVERY_TEST_ETCD', args.etcd) if target else args.etcd
        root.mkdir()
        (root / 'assets').mkdir()
        (root / 'assets/index.html').write_text('<title>GraphLake recovery</title>')
        init_credentials(args.cli, root, self.password)
        self.config = root.with_suffix('.cluster.yaml')
        prefix = getattr(args, 's3_prefix', None)
        namespace = f'{prefix}/' if prefix else ''
        self.config.write_text(f'''name: s6-{uuid4().hex}
metadata: {etcd}
storage: s3://{args.bucket}/{namespace}s6-{uuid4().hex}
token_env: S6_WORKER_TOKEN
workers: ['http://127.0.0.1:{self.worker_port}']
distributed_partitions: 2
''')

    def __enter__(self):
        self.stack = ExitStack()
        return self

    def __exit__(self, *exc):
        self.stack.close()

    def start(self, marker, **env):
        self.stack.enter_context(process_with_env(
            [self.cli, '--cluster-config', self.config, '--port', self.worker_port, 'worker',
             '--source-dir', self.root], self.root / f'worker-{marker}.log', self.env,
             'GraphLake worker '))
        return self.coordinator(marker, **env)

    def coordinator(self, marker, *, slots=1, standby=False, **env):
        self.cookie = ''
        return self.stack.enter_context(process_with_env(
            [self.cli, '--cluster-config', self.config, '--port', self.port, '--catalog-dir', self.root,
             '--assets-dir', self.root / 'assets', '--coordinator-failover', '--public-url', self.public,
             '--query-slots', str(slots)],
            self.root / f'coordinator-{marker}.log',
            {**self.env, 'S6_SOURCE_DSN': self.source_dsn, **env},
            'GraphLake Coordinator waiting for etcd leadership' if standby else ('GraphLake admin ', 'GraphLake restore ')))

    def call(self, path, body=None, *, method=None, login=False, match=None):
        headers = {'Host': self.public.split('//')[1], 'Origin': self.public, 'Content-Type': 'application/json'}
        if match:
            headers['If-Match'] = '"' + match + '"'
        if path.startswith('/recovery/'):
            headers['X-GraphLake-Recovery-Token'] = self.token
        elif self.cookie:
            headers['Cookie'] = self.cookie
        request = Request(self.base + path, data=None if body is None else json.dumps(body).encode(),
                          method=method, headers=headers)
        try:
            response = urlopen(request, timeout=60)
        except HTTPError as exc:
            response = exc
        with response:
            payload = response.read()
            if login and response.status == 200:
                self.cookie = response.headers['Set-Cookie'].split(';', 1)[0]
            return response.status, json.loads(payload) if payload else None

    def login(self):
        ok(self.call('/api/v1/auth/login', {'username': 'admin', 'password': self.password}, login=True))

    def read(self, path, request=None):
        start = time.monotonic()
        retries = 0
        while True:
            status, result = self.call(path, request)
            if (status != 503 or not isinstance(result, dict) or result.get('code') != 'coordinator_leadership_uncertain'
                    or time.monotonic() - start >= 5):
                result = ok((status, result))
                if retries:
                    self.read_retries.append((retries, time.monotonic() - start))
                return result
            retries += 1
            if request is not None:
                request = {**request, 'request_id': str(uuid4())}
            time.sleep(.1)

    def table(self):
        return self.read(MODEL + '/local-tables')[0]

    def query(self, revision):
        return self.read(MODEL + '/query', {
            'request_id': str(uuid4()), 'model_revision': revision, 'language': 'sql', 'query': GRAPH_QUERY,
        })['rows']

    def load(self, mode='incremental', *, wait=True):
        task = ok(self.call(TABLE + '/load', {'mode': mode}))
        if not wait:
            return task
        def completed():
            last = self.table()['history'][0]
            return last if last['id'] == task['id'] and last['status'] != 'running' else None
        last = until(completed)
        assert last['status'] == 'succeeded', last
        return last

    def csr(self, previous=None):
        def current():
            csr = self.read(MODEL + '/freshness')['csr']
            return csr if csr and csr['current'] and csr['generation'] != previous else None
        return until(current)


@contextmanager
def process_with_env(args, log, env, prefix):
    with log.open('w') as output:
        with subprocess.Popen(list(map(str, args)), stdout=output, stderr=subprocess.STDOUT, env=env) as child:
            try:
                def started():
                    if child.poll() is not None:
                        raise RuntimeError(log.read_text())
                    return any(line.startswith(prefix) for line in log.read_text().splitlines())
                until(started)
                yield child
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()


def run(args, root):
    upstream = os.environ['GRAPHLAKE_TEST_POSTGRES']
    suffix = uuid4().hex[:12]
    table = 's6_accounts_' + suffix
    db = 's6_restore_' + suffix
    target_db = urlsplit(os.environ.get('GRAPHLAKE_RECOVERY_TEST_POSTGRES', upstream))
    target_base = urlunsplit(target_db)
    destination = urlunsplit(target_db._replace(path='/' + db))
    archives = root / 'archives'
    archives.mkdir(mode=0o700)
    inbox = root / 'inbox'
    bundle = inbox / 'checkpoint'
    bundle.mkdir(parents=True)
    postgres(upstream, '-c', f'''CREATE TABLE {table} (
      id bigint PRIMARY KEY, target bigint NOT NULL, version bigint NOT NULL, deleted boolean NOT NULL);
      INSERT INTO {table} VALUES (1,2,10,false),(2,2,10,false),(3,3,10,false);''')
    try:
        postgres(target_base, '-c', f'CREATE DATABASE {db}')
        try:
            exercise(args, root, upstream, destination, table, archives, inbox, bundle)
        finally:
            postgres(target_base, '-c', f'DROP DATABASE {db} WITH (FORCE)')
    finally:
        postgres(upstream, '-c', f'DROP TABLE {table}')


def exercise(args, root, upstream, destination, table, archives, inbox, bundle):
    relation = 'public.' + table
    with Site(args, root / 'origin', upstream) as src:
        yaml = f"""graph: Account access
freshness: {{auto_refresh_csr: true}}
sources:
  input: {{type: postgres, dsn_env: S6_SOURCE_DSN}}
nodes:
  Account: {{source: input.{relation}, key: [id], properties: [target, version, deleted]}}
edges:
  Grant: {{source: input.{relation}, key: [id], from: {{node: Account, columns: [id]}}, to: {{node: Account, columns: [target]}}}}
"""
        (src.root / 'accounts.yaml').write_text(yaml)
        src.start('origin', GRAPHLAKE_MANUAL_BACKUP_DIR=str(archives))
        src.login()
        draft = ok(src.call(MODEL))
        replica = ok(src.call(MODEL + '/local-tables', {
            'model_revision': draft['revision'], 'name': 'accounts', 'source_table': 'input.' + relation,
            'columns': ['id', 'target', 'version', 'deleted'], 'key_columns': ['id'],
            'watermark_column': 'version', 'lookback': 5, 'deleted_column': 'deleted',
            'interval_seconds': 1800, 'reconcile_interval_seconds': 3600, 'mode': 'incremental',
        }))
        src.load('full')
        postgres(upstream, '-c', f'UPDATE {table} SET version=11 WHERE id=1')
        initial = src.load()
        assert initial['committed_generation']
        yaml = yaml.replace('nodes:', f"  replica: {{type: parquet, tables: {{accounts: ['{replica['reference_path']}']}}}}\nnodes:")
        yaml = yaml.replace('source: input.' + relation, 'source: replica.accounts')
        version = ok(src.call(MODEL, {'yaml': yaml}, method='PUT', match=draft['revision']))
        published = ok(src.call(MODEL + '/publish', {
            'revision': version['revision'], 'active_revision': draft['active_revision'], 'mutation_id': str(uuid4()),
        }))
        revision = published['active_revision']
        before = src.query(revision)
        assert before == BEFORE, before
        subprocess.run([os.environ.get('GRAPHLAKE_TEST_PG_DUMP', 'pg_dump'), '-Fc', '-t', table, '-f', bundle / 'postgres.dump'],
                       env=pg_env(upstream), check=True)
        fingerprint = ok_fingerprint(src, relation)
        with locked_table(upstream, table):
            running = src.load(wait=False)
            assert src.table()['history'][0]['status'] == 'running'
            start = time.monotonic()
            exported = ok(src.call('/api/v1/backups/export', {}))
            elapsed = time.monotonic() - start
            assert src.table()['history'][0]['status'] == 'running'
            cli_file = archives / 'scheduled.json'
            subprocess.run([args.cli, '--cluster-config', src.config, 'backup', 'export', '--file', cli_file],
                           env={**src.env, 'GRAPHLAKE_BACKUP_CATALOG_DIR': str(src.root)}, check=True)
            subprocess.run([args.cli, 'backup', 'verify', '--file', cli_file], check=True)
        control = archives / exported['file']
        shutil.copy2(control, bundle / 'control.json')
        sidecar = bundle / exported['local_directory']
        shutil.copytree(archives / exported['local_directory'], sidecar)
        offline = subprocess.run([args.cli, 'backup', 'verify', '--file', bundle / 'control.json'],
                                 check=True, capture_output=True, text=True)
        assert '1 local tables' in offline.stdout
        backup = json.loads(control.read_text())
        state = backup['state']['local_tables']
        history = state['tables']['accounts/accounts']['history']
        assert history[0]['id'] == running['id'] and history[0]['status'] == 'interrupted'
        assert history[1]['committed_generation'] == initial['committed_generation']
        assert len(state['files']) >= 3 and all('keys.sqlite' not in name for name in state['files'])
        assert upstream not in control.read_text()
        def released():
            return src.table()['history'][0]['status'] != 'running'
        until(released)
        postgres(upstream, '-c', f"""UPDATE {table} SET version=12, deleted=true WHERE id=1;
                 INSERT INTO {table} VALUES (4,3,13,false)""")
        src.load()
        assert src.query(revision) == AFTER
        archive = bundle / 'postgres.dump'
        (bundle / 'manifest.json').write_text(json.dumps({
            'format': 1, 'control_sha256': backup['sha256'],
            'postgres': {'dump_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(), 'dsn_env': 'S6_TARGET_DSN',
                         'relations': fingerprint},
            'canaries': [{'model': 'accounts', 'revision': revision, 'language': 'sql',
                          'query': GRAPH_QUERY, 'expected_rows': before}],
        }))
        print(json.dumps({'backup_while_sync_running': round(elapsed, 3), 'before_rows': before,
                          'archive_bytes': sum(file.stat().st_size for file in sidecar.rglob('*.parquet')),
                          'archives': len(state['files']), 'checkpoint_ms': backup['created_at_ms'],
                          'generation': initial['committed_generation']}), flush=True)

    pg_restore = shutil.which(os.environ.get('GRAPHLAKE_TEST_PG_RESTORE', 'pg_restore'))
    assert pg_restore
    wrapper = root / 'pg_restore'
    block = root / 'block-restore'
    active = root / 'restore-blocked'
    wrapper.write_text(f'''#!/bin/sh
if [ "$1" = --single-transaction ] && [ -f '{block}' ]; then
  touch '{active}'
  exec sleep 90
fi
exec '{pg_restore}' "$@"
''')
    wrapper.chmod(0o700)
    block.touch()
    offline_source = 'postgresql://reader@127.0.0.1:1/unavailable?sslmode=disable'
    with Site(args, root / 'target', offline_source, target=True) as dst:
        environment = {'GRAPHLAKE_RESTORE_INBOX': str(inbox), 'GRAPHLAKE_RESTORE_POSTGRES_DSN_ENV': 'S6_TARGET_DSN',
                       'S6_TARGET_DSN': destination, 'GRAPHLAKE_RESTORE_PG_RESTORE': str(wrapper)}
        coordinator = dst.start('first', **environment)
        checked = ok(dst.call('/recovery/check', {'bundle': 'checkpoint'}))
        assert checked['local_tables'] == 1, checked
        request = {'bundle': 'checkpoint', 'manifest_sha256': checked['manifest_sha256']}
        corrupt = next(sidecar.glob('*.parts/*.parquet'))
        saved = corrupt.with_suffix('.saved')
        corrupt.rename(saved)
        def failed():
            job = ok(dst.call('/recovery/status'))['job']
            return job if job and job['failure'] else None
        ok(dst.call('/recovery/import', request))
        failure = until(failed)
        assert 'local backup file is missing' in failure['failure'], failure
        assert not (dst.root / '.graphlake-local').exists()
        shutil.copyfile(saved, corrupt)
        with corrupt.open('ab') as bad:
            bad.write(b'corruption')
        ok(dst.call('/recovery/import', request))
        failure = until(failed)
        assert 'checksum mismatch' in failure['failure'], failure
        assert not (dst.root / '.graphlake-local').exists()
        saved.replace(corrupt)
        ok(dst.call('/recovery/import', request))
        until(active.exists)
        assert (dst.root / '.graphlake-local/state.json').is_file()
        status = ok(dst.call('/recovery/status'))
        assert status['job']['phase'] == 'postgres' and status['job']['running'], status
        coordinator.send_signal(signal.SIGTERM)
        coordinator.wait(timeout=15)
        block.unlink()
        dst.coordinator('retry', **environment)
        interrupted = until(failed)
        assert interrupted['bound'] and '中断' in interrupted['failure'], interrupted
        assert dst.call(MODEL)[0] == 503
        start = time.monotonic()
        ok(dst.call('/recovery/import', request))
        def gate():
            try:
                status = ok(dst.call('/recovery/status'))
                if (status.get('job') or {}).get('failure'):
                    raise AssertionError(status)
                return status if 'approved' in status else None
            except URLError:
                return None
        assert until(gate)['approved'] is False
        installed_part = next((dst.root / '.graphlake-local').glob('*.parts/*.parquet'))
        hidden = installed_part.with_suffix('.held')
        installed_part.rename(hidden)
        rejected, detail = dst.call('/recovery/verify', {})
        assert rejected >= 400 and 'local backup file is missing' in detail['message'], detail
        assert not ok(dst.call('/recovery/status'))['approved']
        hidden.replace(installed_part)
        ok(dst.call('/recovery/verify', {}))
        assert until(lambda: ok(dst.call('/recovery/status'))['ready'])
        rto = time.monotonic() - start
        dst.login()
        restored = dst.table()
        assert restored['schedule_paused'] and restored['interval_seconds'] == 1800
        assert restored['reconcile_interval_seconds'] == 3600 and restored['next_reconcile_at_ms'] is None
        assert restored['generation'] == initial['committed_generation'] and restored['watermark']['value'] == '11'
        assert restored['history'] == history
        assert dst.query(revision) == before
        head = dst.csr()
        dst.stack.close()
        dst.stack = ExitStack()
        dst.source_dsn = upstream
        dst.start('resume', **environment)
        dst.login()
        delta = dst.load()
        digest = hashlib.sha256(json.dumps(dst.query(revision), separators=(',', ':')).encode()).hexdigest()
        assert dst.query(revision) == AFTER
        assert dst.table()['schedule_paused'] and dst.table()['next_run_at_ms'] is None
        csr = dst.csr(head['generation'])
        labels = ok(dst.call(MODEL + '/query', {
            'model_revision': revision, 'request_id': str(uuid4()), 'freshness': {'require_current_csr': True},
            'csr': {'generation': csr['generation'], 'operation': {'kind': 'wcc', 'max_iterations': 12}},
        }))['rows']
        assert len(labels) == len(AFTER), labels
        resumed = ok(dst.call(TABLE, {'revision': dst.table()['revision'], 'mode': 'incremental',
                                     'interval_seconds': 1800, 'reconcile_interval_seconds': 3600,
                                     'timeout_seconds': 3600}, method='PUT'))
        assert not resumed['schedule_paused'] and resumed['next_run_at_ms'] and resumed['next_reconcile_at_ms']
        print(json.dumps({'missing_and_corrupt_rejected': True, 'interruption': 'postgres', 'retry_s': round(rto, 3),
                          'offline_query_matched': True, 'resumed_rows': AFTER, 'resumed_sha256': digest,
                          'resumed_sync_rows': delta['loaded_rows'], 'resumed_generation': delta['committed_generation'],
                          'csr_generation': csr['generation'], 'csr_rows': len(labels)}), flush=True)

    only_local = inbox / 'local-only'
    shutil.copytree(bundle, only_local, ignore=shutil.ignore_patterns('postgres.dump'))
    manifest = json.loads((only_local / 'manifest.json').read_text())
    del manifest['postgres']
    (only_local / 'manifest.json').write_text(json.dumps(manifest))
    with Site(args, root / 'cli-target', offline_source, target=True) as dst:
        command = [args.cli, '--cluster-config', dst.config, '--catalog-dir', dst.root, 'backup', 'restore',
                   '--file', only_local / 'control.json', '--recovery-dir', only_local]
        part = next((only_local / exported['local_directory']).glob('*.parts/*.parquet'))
        moved = part.with_suffix('.saved')
        part.rename(moved)
        incomplete = subprocess.run(command, env=dst.env, capture_output=True, text=True)
        assert incomplete.returncode and 'local backup file is missing' in incomplete.stderr, incomplete.stderr
        assert not (dst.root / '.graphlake-local').exists()
        ordinary = subprocess.run([args.cli, '--cluster-config', dst.config, '--catalog-dir', dst.root,
                                   '--assets-dir', dst.root / 'assets', '--port', str(dst.port),
                                   '--coordinator-failover', '--public-url', dst.public],
                                  env=dst.env, capture_output=True, text=True, timeout=15)
        assert ordinary.returncode and 'reserved for CLI recovery' in ordinary.stderr, ordinary.stderr
        moved.replace(part)
        subprocess.run(command, env=dst.env, check=True)
        dst.start('cli', GRAPHLAKE_RECOVERY_DIR=str(only_local))
        assert dst.call(MODEL)[0] == 503
        ok(dst.call('/recovery/verify', {}))
        assert until(lambda: ok(dst.call('/recovery/status'))['ready'])
        dst.login()
        assert dst.query(revision) == BEFORE
        print(json.dumps({'cli_backup_and_local_only_restore': True, 'cli_recovery_reserved_after_failure': True}), flush=True)


def ok_fingerprint(site, table):
    return json.loads(subprocess.check_output(
        [site.cli, 'backup', 'fingerprint-postgres', '--dsn-env', 'S6_SOURCE_DSN', '--relation', table],
        env={**site.env, 'S6_SOURCE_DSN': site.source_dsn}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cli', type=lambda path: Path(path).resolve())
    parser.add_argument('--etcd', default=os.environ.get('GRAPHLAKE_TEST_ETCD', 'http://127.0.0.1:22379'))
    parser.add_argument('--bucket', default=os.environ.get('GRAPHLAKE_TEST_BUCKET', 'graphlake-ci'))
    parser.add_argument('--work-dir', type=Path)
    opts = parser.parse_args()
    if opts.work_dir:
        run(opts, opts.work_dir)
    else:
        with tempfile.TemporaryDirectory() as directory:
            run(opts, Path(directory))
