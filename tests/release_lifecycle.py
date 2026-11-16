"""Exercise a published-to-candidate migration and managed-table soak on disposable Linux fixtures."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from errno import ENOSPC
from hashlib import sha256
import json
import os
from pathlib import Path
import random
import re
import secrets
import shutil
import subprocess
import threading
import time
from urllib.request import Request, urlopen
from uuid import uuid4

from cluster_scaling_benchmark import allocate_ports, ok
from local_backup_recovery import GRAPH_QUERY, MODEL, Site, locked_table, postgres, process_with_env, until
from managed_local_tables import lookup, proxy, s3_command, value


def digest(rows):
    return sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()


def local(site, name):
    return next(table for table in site.read(MODEL + '/local-tables') if table['name'] == name)


def load(site, name, mode='incremental', *, wait=True):
    run = ok(site.call(MODEL + '/local-tables/' + name + '/load', {'mode': mode}))
    if not wait:
        return run

    def finished():
        last = local(site, name)['history'][0]
        return last if last['id'] == run['id'] and last['status'] != 'running' else None

    last = until(finished, seconds=90)
    assert last['status'] == 'succeeded', last
    return last


def publish(site, yaml):
    draft = ok(site.call(MODEL))
    version = ok(site.call(MODEL, {'yaml': yaml}, method='PUT', match=draft['revision']))
    return ok(site.call(MODEL + '/publish', {
        'revision': version['revision'], 'active_revision': draft['active_revision'],
    }))['active_revision']


@contextmanager
def small_volume(path):
    path.mkdir()
    subprocess.run(['sudo', '-n', 'mount', '-t', 'tmpfs', '-o', 'size=16m,mode=1777', 'tmpfs', path], check=True)
    try:
        yield path
    finally:
        subprocess.run(['sudo', '-n', 'umount', path], check=True)


def activate(site, name, source, yaml, *, legacy=False):
    draft = ok(site.call(MODEL))
    table = ok(site.call(MODEL + '/local-tables', {
        'model_revision': draft['revision'], 'name': name, 'source_table': source,
        'columns': ['id', 'target', 'version'], 'key_columns': ['id'], 'watermark_column': 'version',
        'mode': 'incremental', 'timeout_seconds': 12,
        **({} if legacy else {'lookback': 1}),
    }))
    first = load(site, name, 'full')
    location = table['path']
    if not location.startswith('local-table://'):
        location = '.graphlake-local/' + Path(location).name
    model = yaml.replace('nodes:', f"  replica: {{type: parquet, tables: {{accounts: ['{location}']}}}}\nnodes:")
    return publish(site, model.replace('source: ' + source, 'source: replica.accounts')), location, first


class Exercise:
    def __init__(self, args, root):
        self.args = args
        self.root = root
        self.dsn = os.environ['GRAPHLAKE_TEST_POSTGRES']
        self.relation = 's7_accounts_' + uuid4().hex[:12]
        self.source = 'input.public.' + self.relation
        self.yaml = f"""graph: Release acceptance accounts
sources:
  input: {{type: postgres, dsn_env: S6_SOURCE_DSN}}
nodes:
  Account: {{source: {self.source}, key: [id], properties: [target, version]}}
edges:
  Grant: {{source: {self.source}, key: [id], from: {{node: Account, columns: [id]}}, to: {{node: Account, columns: [target]}}}}
"""
        self.rows = [[str(i), str(i % args.rows + 1), '1'] for i in range(1, args.rows + 1)]

    def report(self, event, **data):
        print(json.dumps({'event': event, 'at_epoch': time.time(), **data}, sort_keys=True), flush=True)

    def assert_rows(self, site, revision):
        rows = site.query(revision)
        assert rows == self.rows, (digest(rows), digest(self.rows))
        return digest(rows)

    def run(self):
        postgres(self.dsn, '-c', f'''CREATE TABLE {self.relation}(id bigint PRIMARY KEY, target bigint NOT NULL,
            version bigint NOT NULL); INSERT INTO {self.relation}
            SELECT i, i % {self.args.rows} + 1, 1 FROM generate_series(1, {self.args.rows}) AS i;''')
        try:
            self.upgrade()
        finally:
            postgres(self.dsn, '-c', f'DROP TABLE {self.relation}')

    def upgrade(self):
        args, root = self.args, self.root
        with ExitStack() as processes, Site(args, root / 'catalog', self.dsn) as site:
            ports = [site.worker_port, *allocate_ports(1)]
            site.config.write_text(re.sub(r'^workers:.*$', 'workers: ' +
                json.dumps([f'http://127.0.0.1:{port}' for port in ports]), site.config.read_text(), flags=re.M))
            (site.root / 'accounts.yaml').write_text(self.yaml)
            workers = [None, None]
            running = None
            runtime_env = {}
            roots = [root / f'worker-{n}-only' for n in range(2)]
            for folder in roots:
                folder.mkdir()

            def coordinator(cli, label, *, rolling=False):
                nonlocal running
                site.cli = cli
                if rolling:
                    candidate = site.coordinator(label, slots=3, standby=True, **runtime_env)
                    assert candidate.poll() is None and running.poll() is None
                start = time.monotonic()
                if running is not None:
                    running.terminate()
                    running.wait(timeout=15)
                if rolling:
                    running = candidate
                    log = site.root / f'coordinator-{label}.log'
                    until(lambda: 'GraphLake admin ' in log.read_text())
                else:
                    running = site.coordinator(label, slots=3, **runtime_env)
                site.login()
                return round(time.monotonic() - start, 3)

            def worker(index, cli, label, *, independent=False, hard=False):
                if workers[index] is not None:
                    if hard:
                        workers[index].kill()
                    else:
                        workers[index].terminate()
                    workers[index].wait(timeout=15)
                worker_root = roots[index] if independent else site.root
                workers[index] = processes.enter_context(process_with_env(
                    [cli, '--cluster-config', site.config, '--port', ports[index], '--query-slots', '3', 'worker',
                     '--source-dir', worker_root], root / f'{label}.log', site.env, 'GraphLake worker '))
                health_request = Request(f'http://127.0.0.1:{ports[index]}/internal/v1/identity',
                    headers={'Authorization': 'Bearer ' + site.env['S6_WORKER_TOKEN']})
                with urlopen(health_request, timeout=5) as health:
                    assert health.status == 200

            def checkpoint(label, revision, **details):
                self.report(label, rows_sha256=self.assert_rows(site, revision), **details)

            if args.previous:
                for i in range(2):
                    worker(i, args.previous, f'previous-worker-{i}')
                coordinator(args.previous, 'previous')
                legacy, legacy_path, _ = activate(site, 'accounts', self.source, self.yaml, legacy=True)
                checkpoint('published_baseline', legacy, version=version(args.previous), rows=len(self.rows))
                # The legacy schedule is manual; no new-format writer runs while old peers serve.
                saved = root / 'pre-upgrade'
                saved.mkdir()
                shutil.copytree(site.root / '.graphlake-local', saved / '.graphlake-local',
                                ignore=shutil.ignore_patterns('lock'))
                legacy_yaml = self.yaml.replace('nodes:',
                    f"  replica: {{type: parquet, tables: {{accounts: ['{legacy_path}']}}}}\nnodes:")
                (saved / 'accounts.yaml').write_text(legacy_yaml.replace('source: ' + self.source, 'source: replica.accounts'))
                baseline = [row[:] for row in self.rows]
                for i in range(2):
                    worker(i, args.cli, f'rolling-worker-{i}')
                    checkpoint('worker_rolling_upgrade', legacy, worker=i)
                duration = coordinator(args.cli, 'legacy-candidate', rolling=True)
                checkpoint('candidate_reads_legacy', legacy, handoff_s=duration)
                state_file = '.graphlake-local/state.json'
                assert (site.root / state_file).read_bytes() == (saved / state_file).read_bytes()
                for i in range(2):
                    worker(i, args.previous, f'rollback-worker-{i}')
                duration = coordinator(args.previous, 'rolled-back', rolling=True)
                checkpoint('rollback_before_activation', legacy, handoff_s=duration)
                for i in range(2):
                    worker(i, args.cli, f'candidate-worker-{i}')
                coordinator(args.cli, 'candidate-before-migration')
                checkpoint('legacy_restart', legacy)
                table = local(site, 'accounts')
                assert table['ready'] and table['history'][0]['status'] == 'succeeded'
                assert Path(table['path']).is_file(), table

            with site.config.open('a') as handle:
                handle.write('managed_local_tables: true\nlocal_snapshot_retention_seconds: 4\n'
                             'local_snapshot_gc_grace_seconds: 2\n')
            for i in range(2):
                worker(i, args.cli, f'managed-worker-{i}', independent=True)
            offline = threading.Event()
            if args.inject_faults:
                runtime_env['AWS_ENDPOINT'] = processes.enter_context(proxy(os.environ['AWS_ENDPOINT'], [], offline=offline))
                runtime_env['TMPDIR'] = str(processes.enter_context(small_volume(root / 'sync-volume')))
            coordinator(args.cli, 'managed')
            revision, location, first = activate(site, 'managed', self.source, self.yaml)
            assert location.startswith('local-table://'), location
            checkpoint('managed_cutover', revision, generation=first['committed_generation'],
                       independent_worker_roots=True, version=version(args.cli))

            before = local(site, 'managed')['generation']
            id_ = 1
            postgres(self.dsn, '-c', f'UPDATE {self.relation} SET version=2 WHERE id={id_}')
            with locked_table(self.dsn, self.relation):
                pending = load(site, 'managed', wait=False)
                assert local(site, 'managed')['history'][0]['id'] == pending['id']
                start = time.monotonic()
                running.kill()
                running.wait(timeout=12)
                running = None
                coordinator(args.cli, 'recovered')
                until(lambda: local(site, 'managed')['history'][0]['status'] == 'interrupted')
                assert local(site, 'managed')['generation'] == before
                checkpoint('coordinator_kill_preserved_generation', revision, interrupted_run=pending['id'])
            retried = load(site, 'managed')
            self.rows[id_ - 1][2] = '2'
            checkpoint('coordinator_retry', revision, interruption_to_retry_s=round(time.monotonic() - start, 3),
                       generation=retried['committed_generation'])
            worker(0, args.cli, 'worker-kill-retry', independent=True, hard=True)
            checkpoint('worker_kill_restarted', revision)

            if args.inject_faults:
                self.faults(site, revision, Path(runtime_env['TMPDIR']), offline)
                # Measure the steady state over the same direct S3 path used by the Workers.
                runtime_env.clear()
                coordinator(args.cli, 'steady-state')
                checkpoint('steady_state_started', revision)

            token = secrets.token_hex(24)
            ok(site.call('/api/v1/users/monitor', {'service': True, 'token': token, 'disabled': False},
                         method='PUT'))
            ok(site.call('/api/v1/roles/monitor', {'members': ['monitor'], 'grants': {'accounts': ['query']}},
                         method='PUT'))
            self.monitor(site, token, expect_csr_missing=True)
            self.soak(site, running, revision, location.rsplit('/', 1)[1])
            self.monitor(site, token, expect_csr_missing=False)
            package = root / 'after-soak.json'
            subprocess.run([args.cli, '--cluster-config', site.config, '--catalog-dir', site.root,
                            'backup', 'export', '--file', package], env=site.env, check=True)
            subprocess.run([args.cli, 'backup', 'verify', '--file', package], check=True)
            state = json.loads(package.read_text())['state']['local_tables']['tables']['accounts/managed']
            assert state['archived_generation'] == local(site, 'managed')['generation']
            self.report('soak_backup_verified', generation=state['archived_generation'], rows_sha256=digest(self.rows))

            if args.previous:
                with Site(args, root / 'rollback-site', self.dsn) as recovery:
                    recovery.cli = args.previous
                    recovery.config.write_text(recovery.config.read_text().replace('distributed_partitions: 2', 'distributed_partitions: 0'))
                    shutil.copytree(saved / '.graphlake-local', recovery.root / '.graphlake-local')
                    shutil.copy2(saved / 'accounts.yaml', recovery.root / 'accounts.yaml')
                    recovery.start('restored-legacy')
                    recovery.login()
                    active = ok(recovery.call(MODEL))['active_revision']
                    if not active:
                        active = publish(recovery, (saved / 'accounts.yaml').read_text())
                    assert local(recovery, 'accounts')['history'][0]['status'] == 'succeeded'
                    assert recovery.query(active) == baseline
                    self.report('rollback_after_activation_from_legacy_checkpoint', rows_sha256=digest(baseline),
                                newer_rows_sha256=digest(self.rows))

    def monitor(self, site, token, *, expect_csr_missing):
        config = self.root / 'monitor.json'
        config.write_text(json.dumps({
            'ingress': {'https': site.base},
            'service_account': {'username': 'svc/monitor', 'token_env': 'GRAPHLAKE_MONITOR_TOKEN'},
            'canaries': {'accounts': {'language': 'sql', 'query': GRAPH_QUERY, 'expected_rows': self.rows}},
            'freshness': {'accounts': {'max_reconciliation_age_seconds': 3600, 'require_current_csr': True}},
        }))
        monitor = Path(__file__).resolve().parents[1] / 'deploy/graphlake-monitor'
        sample = subprocess.check_output([monitor, '--config', config, '--once'],
            env={**os.environ, 'GRAPHLAKE_MONITOR_TOKEN': token}, text=True)
        assert 'graphlake_monitor_model_discovery_up 1' in sample, sample
        assert 'graphlake_monitor_model_check_failed' not in sample, sample
        assert ('reason="csr_missing"' in sample) == expect_csr_missing, sample
        with urlopen(site.base + '/metrics', timeout=10) as response:
            metrics = response.read().decode()
        assert 'graphlake_managed_gc_failures_total ' in metrics
        assert 'graphlake_managed_gc_deleted_objects_total ' in metrics
        self.report('monitor_csr_missing' if expect_csr_missing else 'monitor_csr_recovered')

    def faults(self, site, revision, temp, offline):
        def failed_run():
            before = local(site, 'managed')['generation']
            task = load(site, 'managed', wait=False)
            def done():
                state = local(site, 'managed')
                last = state['history'][0]
                if last['id'] == task['id'] and last['status'] != 'running':
                    assert last['status'] == 'failed' and state['generation'] == before, state
                    return last
                return None
            return until(done, seconds=40)

        postgres(self.dsn, '-c', f'UPDATE {self.relation} SET version=3 WHERE id=1')
        offline.set()
        try:
            network = failed_run()
        finally:
            offline.clear()
        self.assert_rows(site, revision)
        with (temp / 'occupy').open('wb') as full:
            free = os.statvfs(temp)
            os.posix_fallocate(full.fileno(), 0, free.f_bavail * free.f_frsize)
            try:
                os.posix_fallocate(full.fileno(), free.f_bavail * free.f_frsize, free.f_frsize)
            except OSError as error:
                assert error.errno == ENOSPC
            else:
                raise AssertionError('temporary volume did not report ENOSPC')
            disk = failed_run()
        (temp / 'occupy').unlink()
        self.assert_rows(site, revision)
        retry = load(site, 'managed')
        self.rows[0][2] = '3'
        self.report('network_and_enospc_retried', network_run=network['id'], disk_run=disk['id'],
                    disk_error=disk['error'], generation=retry['committed_generation'],
                    rows_sha256=self.assert_rows(site, revision))


    def soak(self, site, running, revision, table_id):
        args = self.args
        cluster = site.config.read_text().splitlines()[0].split(': ', 1)[1]
        head_key = '/graphlake/v1/' + cluster + '/local-table/state/' + table_id
        s3_prefix = site.config.read_text().splitlines()[2].split(': ', 1)[1].split('/', 3)[3]
        versions = [digest(self.rows)]
        committed = 0
        samples = []
        stop = threading.Event()

        def concurrent_reads():
            while not stop.wait(.75):
                start = time.monotonic()
                floor = committed
                rows = site.query(revision)
                assert digest(rows) in versions[floor:], rows
                samples.append(time.monotonic() - start)

        def head():
            return value(lookup(args, head_key)[0])['head']

        def manifest(upload):
            key = f'{s3_prefix}/graphlake-v1/{cluster}/local-tables/{table_id}/{upload}/manifest.json'
            return json.loads(subprocess.check_output([*s3_command(args, key), '--fail']))

        def rss():
            return int(re.search(r'^VmRSS:\s+(\d+)', Path(f'/proc/{running.pid}/status').read_text(), re.M)[1])

        rng = random.Random(7)
        start = time.monotonic()
        first_retry = len(site.read_retries)
        generations = set()
        compactions = 0
        parts = len(manifest(head()['upload'])['files'])
        peak_rss_kib = rss()
        with ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(concurrent_reads)
            try:
                for n in range(1, args.rounds + 1):
                    deadline = start + n * args.soak_seconds / args.rounds
                    delay = deadline - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    if reader.done():
                        reader.result()
                    version_ = n + (3 if args.inject_faults else 2)
                    id_ = 1 if n % 4 == 0 else rng.randint(1, len(self.rows))
                    target = (id_ + n) % len(self.rows) + 1
                    self.rows[id_ - 1] = [str(id_), str(target), str(version_)]
                    versions.append(digest(self.rows))
                    postgres(self.dsn, '-c', f'UPDATE {self.relation} SET target={target},version={version_} WHERE id={id_}')
                    lap = time.monotonic()
                    run = load(site, 'managed')
                    committed = n
                    load_s = round(time.monotonic() - lap, 4)
                    commit = head()
                    generation = run['committed_generation']
                    assert generation == commit['generation'] and generation not in generations, run
                    generations.add(generation)
                    next_parts = len(manifest(commit['upload'])['files'])
                    compactions += next_parts < parts
                    parts = next_parts
                    peak_rss_kib = max(peak_rss_kib, rss())
                    self.report('sync_round', round=n, generation=generation, load_s=load_s, files=parts,
                                rows_sha256=self.assert_rows(site, revision), reads=run['source_reads'])
            finally:
                stop.set()
            reader.result()
        assert len(generations) == args.rounds and compactions > 0 and samples
        csr = ok(site.call(MODEL + '/csr', {'model_revision': revision}))
        strict = ok(site.call(MODEL + '/query', {'model_revision': revision, 'request_id': str(uuid4()),
            'freshness': {'require_current_csr': True},
            'csr': {'generation': csr['generation'], 'operation': {'kind': 'wcc', 'max_iterations': 30}}}))
        assert len(strict['rows']) == len(self.rows), strict
        samples.sort()
        retries = site.read_retries[first_retry:]
        self.report('soak_passed', rounds=args.rounds, seconds=round(time.monotonic() - start, 3),
                    compactions=compactions, overlapping_query_count=len(samples), csr_rows=len(strict['rows']),
                    overlapping_query_p95_ms=round(1000 * samples[int(.95 * (len(samples) - 1))], 2),
                    leadership_uncertain_responses=sum(count for count, _ in retries),
                    slowest_recovered_read_s=round(max((seconds for _, seconds in retries), default=0), 3),
                    peak_coordinator_rss_kib=peak_rss_kib, rows_sha256=self.assert_rows(site, revision))


def version(cli):
    return subprocess.check_output([cli, '--version'], text=True).strip()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cli', type=lambda path: Path(path).resolve())
    parser.add_argument('--previous', type=lambda path: Path(path).resolve())
    parser.add_argument('--work-dir', required=True, type=Path)
    parser.add_argument('--soak-seconds', type=int, default=900)
    parser.add_argument('--rounds', type=int, default=100)
    parser.add_argument('--rows', type=int, default=48)
    parser.add_argument('--inject-faults', action='store_true', help='exercise S3 disconnect and a dedicated full tmpfs (requires sudo)')
    parser.add_argument('--etcd', default=os.environ.get('GRAPHLAKE_TEST_ETCD'))
    parser.add_argument('--bucket', default=os.environ.get('GRAPHLAKE_TEST_BUCKET'))
    args = parser.parse_args()
    if not args.etcd or not args.bucket or not 2 <= args.rows <= 50 or args.rounds < 1 or args.soak_seconds < 0:
        parser.error('fixture etcd and bucket, 2–50 rows, positive rounds and nonnegative duration are required')
    args.work_dir.mkdir(parents=True)
    Exercise(args, args.work_dir).run()
