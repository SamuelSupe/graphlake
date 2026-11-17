"""Exercise managed local tables against disposable PostgreSQL, etcd and S3 services."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from urllib.parse import quote, unquote, urlsplit
from uuid import uuid4

from local_backup_recovery import Site, MODEL, GRAPH_QUERY, postgres, process_with_env, until
from cluster_scaling_benchmark import ok, call


class Hold:
    def __init__(self, predicate):
        self.predicate = predicate
        self.seen = threading.Event()
        self.resume = threading.Event()
        self.done = threading.Event()
        self.request = None
        self.result = None

    def take(self, method, path, body):
        if self.seen.is_set() or not self.predicate(method, path, body):
            return False
        self.request = body
        self.seen.set()
        assert self.resume.wait(60), path
        return True

    def response(self, payload):
        self.result = payload
        self.done.set()


@contextmanager
def proxy(upstream, rules, *, offline=None, reject=None):
    dest = urlsplit(upstream)

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            if offline is not None and offline.is_set():
                self.close_connection = True
                return
            if reject is not None and reject(self.command, unquote(urlsplit(self.path).path)):
                self.send_error(403)
                return
            data = self.rfile.read(int(self.headers.get('Content-Length', 0)))
            held = next((rule for rule in rules if rule.take(self.command, self.path, data)), None)
            client = HTTPConnection(dest.hostname, dest.port, timeout=12)
            try:
                client.request(self.command, self.path, data, dict(self.headers))
                with client.getresponse() as response:
                    # etcd keepalive sends an open stream after the first message.
                    payload = response.readline() if self.path == '/v3/lease/keepalive' else response.read()
                    if held:
                        held.response(payload)
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in {'transfer-encoding', 'connection', 'content-length'}:
                            self.send_header(key, value)
                    self.send_header('Content-Length', response.getheader('Content-Length', '0')
                                     if self.command == 'HEAD' else str(len(payload)))
                    self.end_headers()
                    if payload:
                        self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass  # The intentionally delayed request can outlive its old client.
            finally:
                client.close()
        do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = handle_request

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f'http://127.0.0.1:{server.server_port}'
        finally:
            for rule in rules:
                rule.resume.set()
            server.shutdown()
            thread.join()


def b64(value):
    if isinstance(value, str):
        value = value.encode()
    return base64.b64encode(value).decode()


def v3(endpoint, operation, request):
    return ok(call(endpoint, '/v3/' + operation, request))


def lookup(args, key):
    return v3(args.etcd, 'kv/range', {'key': b64(key)}).get('kvs', [])


def records(args, prefix):
    return v3(args.etcd, 'kv/range', {'key': b64(prefix), 'range_end': b64(prefix[:-1] + '0')}).get('kvs', [])


def value(record):
    return json.loads(base64.b64decode(record['value']))


def head_transaction(state_key, old):
    def matches(method, path, body):
        if method != 'POST' or path != '/v3/kv/txn':
            return False
        request = json.loads(body)
        for item in request.get('success', []):
            put = item.get('request_put', {})
            if put.get('key') == b64(state_key):
                record = value(put)
                if record.get('head') and record['head']['generation'] != old and record['pending'] is None:
                    return True
        return False
    return matches


def s3_command(args, key):
    return ['curl', '--silent', '--max-time', '4', '--aws-sigv4', 'aws:amz:us-east-1:s3',
            '--user', os.environ['AWS_ACCESS_KEY_ID'] + ':' + os.environ['AWS_SECRET_ACCESS_KEY'],
            os.environ['AWS_ENDPOINT'] + '/' + args.bucket + '/' + quote(key, safe='/')]


def object_exists(args, key):
    result = subprocess.run([*s3_command(args, key), '--head', '--output', '/dev/null',
                             '--write-out', '%{http_code}'], check=True, capture_output=True, text=True)
    assert result.stdout in {'200', '404'}, result.stdout
    return result.stdout == '200'


def manifest_exists(args, key):
    return object_exists(args, key + '/manifest.json')


def data_keys(args, prefix):
    raw = subprocess.check_output([*s3_command(args, prefix + '/manifest.json'), '--fail'])
    return {file['location'].split('/', 3)[3] for file in json.loads(raw)['files'].values()}


def worker(site, root, config, env, hidden, *, isolate, label):
    pid_file = root / (label + '.pid')
    command = [site.cli, '--cluster-config', config, '--port', str(site.worker_port), 'worker',
               '--source-dir', root]
    if isolate:
        names = ','.join(k for k in env if k.startswith('AWS_') or k in {
            'S6_WORKER_TOKEN', 'GRAPHLAKE_CLUSTER_S3_BACKEND'})
        command = ['sudo', '-n', '--preserve-env=' + names, 'unshare', '--mount', '--propagation', 'private',
                   'sh', '-ec', 'for hidden in "$1" "$2"; do mount -t tmpfs tmpfs "$hidden"; '
                   'test -z "$(ls -A "$hidden")"; done; echo $$ > "$3"; shift 3; exec "$@"', 'worker', *hidden, pid_file, *command]
    return process_with_env(command, root / f'{label}.log', env, 'GraphLake worker ')


def create_data(args, root):
    dsn = os.environ['GRAPHLAKE_TEST_POSTGRES']
    relation = 'managed_' + uuid4().hex[:12]
    postgres(dsn, '-c', f'''CREATE TABLE {relation}(id bigint PRIMARY KEY, target bigint, version bigint NOT NULL);
            INSERT INTO {relation} VALUES (1,2,10),(2,2,10),(3,3,10);''')
    try:
        exercise(args, root, dsn, relation)
    finally:
        postgres(dsn, '-c', f'DROP TABLE {relation}')


def exercise(args, root, dsn, relation):
    worker_hold = []
    export_hold = []
    writes = []
    s3 = os.environ['AWS_ENDPOINT']
    failure = {'write_prefix': None, 'read_paths': set()}

    def reject_sync(method, path):
        path = urlsplit(path).path
        return ((method == 'PUT' and failure['write_prefix'] is not None
                 and path.startswith(failure['write_prefix']))
                or (method == 'GET' and path in failure['read_paths']))

    with Site(args, root / 'origin', dsn) as original, Site(args, root / 'standby', dsn) as standby, \
         proxy(args.etcd, writes) as old_etcd, proxy(s3, worker_hold) as worker_s3, \
         proxy(s3, export_hold) as export_s3, proxy(s3, [], reject=reject_sync) as sync_s3:
        config = original.config.read_text()
        cluster = config.splitlines()[0].split(': ')[1]
        storage = urlsplit(config.splitlines()[2].split(': ', 1)[1]).path.strip('/')
        keyroot = '/graphlake/v1/' + cluster + '/local-table'
        objectroot = f'{storage}/graphlake-v1/{cluster}/local-tables'
        config += 'managed_local_tables: true\nlocal_snapshot_retention_seconds: 1\nlocal_snapshot_gc_grace_seconds: 1\n'
        standby.config.write_text(config)
        standby.env = {**original.env}
        if args.standby_s3_backend:
            standby.env['GRAPHLAKE_CLUSTER_S3_BACKEND'] = args.standby_s3_backend
        standby.password = original.password
        shutil.copy2(original.root / ".graphlake-auth/password.phc", standby.root / ".graphlake-auth/password.phc")
        original.config.write_text(config.replace(args.etcd, old_etcd, 1))
        worker_root = root / 'worker-only'
        worker_root.mkdir()
        initial_worker = original.stack.enter_context(worker(original, worker_root, standby.config,
            {**original.env, 'AWS_ENDPOINT': worker_s3}, [original.root, standby.root],
            isolate=args.isolate_worker, label='worker'))
        yaml = f'''graph: Object-backed accounts
sources:
  input: {{type: postgres, dsn_env: S6_SOURCE_DSN}}
nodes:
  Account: {{source: input.public.{relation}, key: [id], properties: [target, version]}}
edges:
  Grant: {{source: input.public.{relation}, key: [id], from: {{node: Account, columns: [id]}}, to: {{node: Account, columns: [target]}}}}
'''
        (original.root / 'accounts.yaml').write_text(yaml)
        coordinator_tmp = root / 'coordinator-tmp'
        coordinator_tmp.mkdir()
        old_coordinator = original.coordinator('original', AWS_ENDPOINT=sync_s3, TMPDIR=str(coordinator_tmp))
        original.login()
        draft = ok(original.call(MODEL))
        local = ok(original.call(MODEL + '/local-tables', {
            'model_revision': draft['revision'], 'name': 'accounts', 'source_table': 'input.public.' + relation,
            'columns': ['id', 'target', 'version'], 'key_columns': ['id'], 'watermark_column': 'version',
            'mode': 'incremental', 'lookback': 1,
        }))
        location = local['reference_path']
        assert location.startswith('local-table://'), local
        table = location.split('://')[1]
        state_key = keyroot + '/state/' + table
        first = original.load('full')['committed_generation']
        assert list(coordinator_tmp.glob('graphlake-managed-sync-*/**/*.parquet'))
        assert list(coordinator_tmp.glob('graphlake-managed-sync-*/**/*.keys.sqlite'))
        assert not list((original.root / '.graphlake-local').glob('managed-sync-*'))
        assert not list((original.root / '.graphlake-local').rglob('*.sqlite'))
        before = value(lookup(args, state_key)[0])['head']
        assert before['generation'] == first
        prefix = objectroot + '/' + table + '/'
        old_parts = data_keys(args, prefix + before['upload'])
        failure['write_prefix'] = '/' + args.bucket + '/' + prefix
        try:
            failed = original.load('full', wait=False)

            def finished_failure():
                last = original.table()['history'][0]
                return last if last['id'] == failed['id'] and last['status'] != 'running' else None

            assert until(finished_failure)['status'] == 'failed'
        finally:
            failure['write_prefix'] = None
        assert value(lookup(args, state_key)[0])['head'] == before
        failure['read_paths'] = {'/' + args.bucket + '/' + key for key in old_parts}
        try:
            assert original.load()['total_rows'] == 3
        finally:
            failure['read_paths'] = set()
        assert value(lookup(args, state_key)[0])['head'] == before
        print(json.dumps({'failed_upload_retried_without_downloading_committed_parts': True}), flush=True)
        original_yaml = yaml.replace('nodes:', f"  replica: {{type: parquet, tables: {{accounts: ['{location}']}}}}\nnodes:")
        original_yaml = original_yaml.replace('source: input.public.' + relation, 'source: replica.accounts')
        version = ok(original.call(MODEL, {'yaml': original_yaml}, method='PUT', match=draft['revision']))
        revision = ok(original.call(MODEL + '/publish', {
            'revision': version['revision'], 'active_revision': draft['active_revision']}))['active_revision']

        def old_data(method, path, _):
            return method == 'GET' and urlsplit(path).path.endswith('.parquet') and before['upload'] in path
        work = Hold(old_data)
        archive = Hold(old_data)
        worker_hold.append(work)
        export_hold.append(archive)
        package = root / 'control.json'
        query_before = [['1', '2', '10'], ['2', '2', '10'], ['3', '3', '10']]
        with ThreadPoolExecutor(max_workers=2) as pool:
            query = pool.submit(original.query, revision)
            backup = pool.submit(subprocess.run, [
                original.cli, '--cluster-config', original.config, '--catalog-dir', original.root,
                'backup', 'export', '--file', package,
            ], env={**original.env, 'AWS_ENDPOINT': export_s3}, check=True, capture_output=True)
            try:
                assert work.seen.wait(15), 'the query must be reading G1'
                assert archive.seen.wait(15), 'the backup must be reading G1'
                assert len(records(args, keyroot + '/pin/' + table + '/')) >= 2
                postgres(dsn, '-c', f'UPDATE {relation} SET version=11,target=3 WHERE id=1')
                middle = original.load('full')
                middle_head = value(lookup(args, state_key)[0])['head']
                middle_parts = data_keys(args, prefix + middle_head['upload'])
                postgres(dsn, '-c', f'UPDATE {relation} SET version=12,target=2 WHERE id=1')
                active = original.load('full')
                assert middle['committed_generation'] != active['committed_generation']
                until(lambda: not manifest_exists(args, objectroot + '/' + table + '/' + middle_head['upload']))
                assert manifest_exists(args, objectroot + '/' + table + '/' + before['upload'])
                until(lambda: all(not object_exists(args, key) for key in middle_parts))
                assert all(object_exists(args, key) for key in old_parts)
                assert not query.done() and not backup.done()
            finally:
                work.resume.set()
                archive.resume.set()
            assert query.result() == query_before
            assert backup.result().returncode == 0
        subprocess.run([args.cli, 'backup', 'verify', '--file', package], check=True)
        backed = json.loads(package.read_text())['state']['local_tables']
        assert backed['tables']['accounts/accounts']['archived_generation'] == first
        assert original.query(revision)[0] == ['1', '2', '12']
        # The completed query's lease deliberately has a deadline buffer.
        # Release its test-only lease early to exercise the following GC cycle.
        for item in records(args, keyroot + '/pin/' + table + '/'):
            if value(item)['head']['generation'] == first:
                v3(args.etcd, 'lease/revoke', {'ID': item['lease']})
        until(lambda: not manifest_exists(args, objectroot + '/' + table + '/' + before['upload']))
        until(lambda: all(not object_exists(args, key) for key in old_parts))
        print(json.dumps({'worker_mount_namespace': args.isolate_worker, 'pinned_query': first,
                          'pinned_backup': first, 'unpinned_snapshot_deleted_during_reads': True,
                          'old_snapshot_deleted_after_release': True}), flush=True)

        postgres(dsn, '-c', f'UPDATE {relation} SET version=13,target=3 WHERE id=1')
        frozen = Hold(head_transaction(state_key, active['committed_generation']))
        writes.append(frozen)
        running = original.load(wait=False)
        assert frozen.seen.wait(12), 'expected old coordinator to finish uploading'
        candidate = next(value(put)['head'] for op in json.loads(frozen.request)['success']
                         if (put := op.get('request_put', {})).get('key') == b64(state_key))
        assert manifest_exists(args, prefix + candidate['upload'])
        orphan_parts = {key for key in data_keys(args, prefix + candidate['upload'])
                        if '/' + candidate['upload'] + '/' in key}
        assert orphan_parts
        before_revision = lookup(args, state_key)[0]['mod_revision']
        assert before_revision == next(comp['mod_revision'] for comp in json.loads(frozen.request)['compare']
                                       if comp['key'] == b64(state_key))
        owner = lookup(args, '/graphlake/v1/' + cluster + '/coordinator/leader')[0]
        v3(args.etcd, 'lease/revoke', {'ID': owner['lease']})
        frozen.resume.set()
        assert frozen.done.wait(6)
        assert not json.loads(frozen.result).get('succeeded'), frozen.result
        assert lookup(args, state_key)[0]['mod_revision'] == before_revision
        old_coordinator.wait(timeout=15)
        assert not list(coordinator_tmp.glob('graphlake-managed-sync-*'))
        start = time.monotonic()
        standby.coordinator('promoted')
        standby.login()
        stopped = until(lambda: standby.table() if standby.table()['history'][0]['status'] == 'interrupted' else None)
        assert stopped['history'][0]['id'] == running['id']
        assert stopped['generation'] == active['committed_generation']
        assert standby.query(revision)[0] == ['1', '2', '12']
        previous = data_keys(args, prefix + value(lookup(args, state_key)[0])['head']['upload'])
        recovered = standby.load()
        current = data_keys(args, prefix + value(lookup(args, state_key)[0])['head']['upload'])
        assert current & previous, 'incremental sync should reuse immutable parts'
        assert recovered['committed_generation'] != candidate['generation']
        assert standby.query(revision)[0] == ['1', '3', '13']
        until(lambda: not manifest_exists(args, objectroot + '/' + table + '/' + candidate['upload']))

        until(lambda: all(not object_exists(args, key) for key in orphan_parts))
        assert all(object_exists(args, key) for key in current)
        csr = ok(standby.call(MODEL + '/csr', {'model_revision': revision}))
        strict = ok(standby.call(MODEL + '/query', {
            'model_revision': revision, 'request_id': str(uuid4()), 'freshness': {'require_current_csr': True},
            'csr': {'generation': csr['generation'], 'operation': {'kind': 'wcc', 'max_iterations': 12}},
        }))
        assert len(strict['rows']) == 3
        print(json.dumps({'stale_transaction_rejected_at_same_record_revision': True,
                          'interrupted_run': running['id'], 'recovered_generation': recovered['committed_generation'],
                          'promote_retry_and_gc_s': round(time.monotonic() - start, 3),
                          'orphan_candidate_reclaimed': True, 'csr_rows': len(strict['rows'])}), flush=True)


        selected = value(lookup(args, state_key)[0])['head']
        work_lost = Hold(lambda method, path, _: method == 'GET' and urlsplit(path).path.endswith('.parquet')
                         and selected['upload'] in path)
        worker_hold.append(work_lost)
        query_id = str(uuid4())
        request = {'model_revision': revision, 'request_id': query_id, 'language': 'sql', 'query': GRAPH_QUERY}
        with ThreadPoolExecutor(max_workers=1) as pool:
            running_query = pool.submit(standby.call, MODEL + '/query', request)
            try:
                assert work_lost.seen.wait(10), 'the original Worker must be reading the pinned object'
                if args.isolate_worker:
                    pid = (worker_root / 'worker.pid').read_text().strip()
                    subprocess.run(['sudo', '-n', 'kill', '-KILL', pid], check=True)
                else:
                    initial_worker.kill()
                initial_worker.wait(timeout=12)
                standby.stack.enter_context(worker(original, worker_root, standby.config,
                    {**standby.env, 'AWS_ENDPOINT': worker_s3}, [original.root, standby.root],
                    isolate=args.isolate_worker, label='worker-retry'))
                postgres(dsn, '-c', f'UPDATE {relation} SET version=14,target=2 WHERE id=1')
                standby.load()
            finally:
                work_lost.resume.set()
            result = ok(running_query.result())
            assert result['rows'][0] == ['1', '3', '13'], result
            assert standby.query(revision)[0] == ['1', '2', '14']
        job = value(lookup(args, '/graphlake/v1/' + cluster + '/jobs/' + query_id)[0])
        assert job['retried'], job
        print(json.dumps({'worker_retry_original_generation': selected['generation'],
                          'worker_retry_and_new_query_matched': True}), flush=True)

    backup = json.loads(package.read_text())
    (root / 'manifest.json').write_text(json.dumps({
        'format': 1, 'control_sha256': backup['sha256'],
        'canaries': [{'model': 'accounts', 'revision': revision, 'language': 'sql',
                      'query': GRAPH_QUERY, 'expected_rows': query_before}],
    }))
    offline = 'postgresql://reader@127.0.0.1:1/unavailable?sslmode=disable'
    with Site(args, root / 'restored', offline, target=True) as restored:
        if args.standby_s3_backend:
            restored.env['GRAPHLAKE_CLUSTER_S3_BACKEND'] = args.standby_s3_backend
        with restored.config.open('a') as config:
            config.write('managed_local_tables: true\n')
        subprocess.run([restored.cli, '--cluster-config', restored.config, '--catalog-dir', restored.root,
                        'backup', 'restore', '--file', package, '--recovery-dir', root],
                       env=restored.env, check=True)
        restored_worker = root / 'restore-worker-only'
        restored_worker.mkdir()
        restored.stack.enter_context(worker(restored, restored_worker, restored.config, restored.env,
            [restored.root, root / 'origin'], isolate=args.isolate_worker, label='worker'))
        restored.coordinator('restored', GRAPHLAKE_RECOVERY_DIR=str(root))
        ok(restored.call('/recovery/verify', {}))
        assert until(lambda: ok(restored.call('/recovery/status'))['ready'])
        restored.login()
        assert restored.query(revision) == query_before
        assert restored.table()['generation'] == first and restored.table()['schedule_paused']
        print(json.dumps({'restored_backup_generation': first, 'restored_s3_query_matched': True,
                          'writer_backend': original.env.get('GRAPHLAKE_CLUSTER_S3_BACKEND', 'object_store'),
                          'standby_and_restore_backend': restored.env.get('GRAPHLAKE_CLUSTER_S3_BACKEND', 'object_store')}),
              flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cli', type=lambda path: Path(path).resolve())
    parser.add_argument('--etcd', default=os.environ.get('GRAPHLAKE_TEST_ETCD', 'http://127.0.0.1:22379'))
    parser.add_argument('--bucket', default=os.environ.get('GRAPHLAKE_TEST_BUCKET', 'graphlake-ci'))
    parser.add_argument('--isolate-worker', action='store_true')
    parser.add_argument('--standby-s3-backend', choices=['object_store', 'opendal'])
    parser.add_argument('--s3-prefix', help='extra prefix inside the test bucket')
    parser.add_argument('--work-dir', type=Path)
    args = parser.parse_args()
    if args.work_dir:
        create_data(args, args.work_dir)
    else:
        with tempfile.TemporaryDirectory() as root:
            create_data(args, Path(root))
