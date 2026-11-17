"""Run the same result-checked Linux workload with one, two and three GraphLake workers."""

import argparse
import concurrent.futures
from contextlib import ExitStack, contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import pty
import select
import signal
import socket
import sqlite3
import statistics
import subprocess
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

COOKIES = {}


def call(base, path, payload=None, token=None):
    request = Request(base + path, data=json.dumps(payload).encode() if payload is not None else None,
                      headers={'Origin': base, 'Content-Type': 'application/json',
                               **({'Authorization': f'Bearer {token}'} if token else {}),
                               **({'Cookie': COOKIES[base]} if base in COOKIES else {})})
    try:
        with urlopen(request, timeout=70) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        with error:
            return error.code, json.load(error)


def ok(response):
    status, result = response
    assert 200 <= status < 300, (status, result)
    return result


def init_credentials(admin, root, password):
    pid, terminal = pty.fork()
    if pid == 0:
        os.execv(admin, [admin, '--catalog-dir', root, 'auth', 'init'])
    received = b''
    try:
        for prompt in (b'Administrator password: ', b'Confirm password: '):
            while prompt not in received:
                if not select.select([terminal], [], [], 5)[0]:
                    raise TimeoutError('administrator credential prompt')
                received += os.read(terminal, 4096)
            os.write(terminal, password.encode() + b'\n')
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
    except BaseException:
        try:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        except (ProcessLookupError, ChildProcessError):
            pass
        raise
    finally:
        os.close(terminal)


def login(base, password):
    request = Request(base + '/api/v1/auth/login', data=json.dumps({'username': 'admin', 'password': password}).encode(),
                      headers={'Origin': base, 'Content-Type': 'application/json'})
    with urlopen(request, timeout=10) as response:
        COOKIES[base] = response.headers['Set-Cookie'].split(';', 1)[0]


def allocate_ports(count):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for item in sockets:
            item.bind(('127.0.0.1', 0))
        return [item.getsockname()[1] for item in sockets]


@contextmanager
def process(args, log, prefix, cpus=None):
    cmd = [*(['taskset', '-c', ','.join(map(str, cpus))] if cpus else []), *map(str, args)]
    with log.open('a+') as output:
        with subprocess.Popen(cmd, stdout=output, stderr=subprocess.STDOUT) as proc:
            try:
                for _ in range(200):
                    output.seek(0)
                    addresses = [line.strip().split()[-1] for line in output if line.startswith(prefix)]
                    if proc.poll() is not None:
                        raise RuntimeError(log.read_text())
                    if addresses:
                        yield proc, addresses[-1]
                        break
                    time.sleep(.05)
                else:
                    raise TimeoutError(log.read_text())
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()


def fixture(root, cli, count):
    with sqlite3.connect(root / 'fixture.db') as db:
        db.execute('CREATE TABLE people(id INTEGER PRIMARY KEY, segment INTEGER NOT NULL) STRICT')
        db.execute('CREATE TABLE events(id INTEGER PRIMARY KEY, owner INTEGER NOT NULL, weight INTEGER NOT NULL, payload TEXT NOT NULL) STRICT')
        db.executemany('INSERT INTO people VALUES(?,?)', ((i, i % 16) for i in range(count)))
        db.executemany('INSERT INTO events VALUES(?,?,?,?)',
                       ((i, i % max(1, count // 10) if i % 3 == 0 else i % count, i % 200, f'{i:08x}' * 12)
                        for i in range(2 * count)))
        db.execute('CREATE TABLE ring(id INTEGER PRIMARY KEY) STRICT')
        db.execute('CREATE TABLE links(id INTEGER PRIMARY KEY, src INTEGER NOT NULL, dst INTEGER NOT NULL) STRICT')
        db.executemany('INSERT INTO ring VALUES(?)', ((i,) for i in range(128)))
        db.executemany('INSERT INTO links VALUES(?,?,?)', ((i, i, i // 64 * 64 + (i + 1) % 64) for i in range(128)))
    export = root / 'export.yaml'
    export.write_text(f'''graph: fixture
sources: {{db: {{type: sqlite, path: '{root / 'fixture.db'}'}}}}
export_locations: {{out: '{root}'}}
nodes: {{Event: {{source: db.events, key: [id], properties: [owner, weight, payload]}}}}
''')
    paths = []
    for shard in range(4):
        file = f'events-{shard}.parquet'
        paths.append(str(root / file))
        start, end = shard * count // 2, (shard + 1) * count // 2
        subprocess.run([cli, '--catalog', export, 'query', '--language', 'sql',
                        f'''EXPORT TO 'out:{file}' SELECT * FROM "Event" WHERE id >= {start} AND id < {end}'''],
                       check=True, stdout=subprocess.DEVNULL)
    return {
        'payments': f'''graph: scaling
sources:
  db: {{type: sqlite, path: '{root / 'fixture.db'}'}}
  lake: {{type: parquet, tables: {{events: {json.dumps(paths)}}}}}
nodes:
  Person: {{source: db.people, key: [id], properties: [segment]}}
  Event: {{source: lake.events, key: [id], properties: [owner, weight, payload]}}
''',
        'topology': f'''graph: topology
sources: {{db: {{type: sqlite, path: '{root / 'fixture.db'}'}}}}
nodes: {{Vertex: {{source: db.ring, key: [id]}}}}
edges:
  Link: {{source: db.links, key: [id], from: {{node: Vertex, columns: [src]}}, to: {{node: Vertex, columns: [dst]}}}}
''',
    }


def resources(processes):
    ticks, rss = 0, 0
    for proc in processes:
        stat = (Path('/proc') / str(proc.pid) / 'stat').read_text().rsplit(')', 1)[1].split()
        ticks += int(stat[11]) + int(stat[12])
        status = (Path('/proc') / str(proc.pid) / 'status').read_text().splitlines()
        rss += int(next(line.split()[1] for line in status if line.startswith('VmHWM:')))
    return ticks / os.sysconf('SC_CLK_TCK'), rss


def sample(base, model, revision, statement, processes):
    before, _ = resources(processes)
    started = time.monotonic()
    page = ok(call(base, f'/api/v1/models/{model}/query',
                   {'request_id': str(uuid4()), 'model_revision': revision, **statement}))
    elapsed = (time.monotonic() - started) * 1000
    after, high_water = resources(processes)
    assert not page.get('truncated'), page
    return page['rows'], {'ms': round(elapsed, 2), 'cpu_s': round(after - before, 3),
                          'rss_high_water_kib_sum': high_water, 'dispatch': page.get('distributed') or page.get('csr_stats')}


def run(root, admin, model_text, etcd, count, total, repeats, expected, failure, min_rows):
    root.mkdir()
    password = uuid4().hex
    init_credentials(admin, root, password)
    for model, text in model_text.items():
        (root / f'{model}.yaml').write_text(text)
    assets = root / 'assets'
    assets.mkdir()
    (assets / 'index.html').write_text('<title>Worker benchmark</title>')
    store = root / 'store'
    store.mkdir()
    ports = allocate_ports(count)
    token = uuid4().hex
    os.environ['GRAPHLAKE_SCALING_TOKEN'] = token
    config = root / 'cluster.yaml'
    config.write_text(f'''name: s5-{uuid4().hex}
metadata: {etcd}
storage: '{store}'
token_env: GRAPHLAKE_SCALING_TOKEN
workers: {json.dumps([f'http://127.0.0.1:{port}' for port in ports])}
distributed_partitions: 3
memory_mib: {384 // count if total else 256}
distributed_min_rows: {min_rows}
''')
    cli = [admin, '--cluster-config', config]
    cpus = sorted(os.sched_getaffinity(0))[:6]
    width = 6 // count if total else 2
    worker_logs = [root / f'worker-{i}.log' for i in range(count)]
    coordinator = [*cli, '--catalog-dir', root, '--assets-dir', assets, '--port', 0, '--query-slots', 4]

    def start_worker(stack, i, suffix=''):
        return stack.enter_context(process([*cli, '--port', ports[i], '--query-slots', 2, 'worker', '--source-dir', root],
                                           worker_logs[i].with_name(f'worker-{i}{suffix}.log'), 'GraphLake worker ',
                                           cpus[i * width:(i + 1) * width]))

    with ExitStack() as stack:
        workers = [start_worker(stack, i) for i in range(count)]
        coord, base = stack.enter_context(process(coordinator, root / 'coordinator.log', 'GraphLake admin '))
        login(base, password)
        revisions = {model: ok(call(base, f'/api/v1/models/{model}'))['active_revision'] for model in model_text}
        head = ok(call(base, '/api/v1/models/topology/csr', {'model_revision': revisions['topology']}))
        sql = {'language': 'sql', 'query': '''SELECT p.segment, count(*) AS n, sum(e.weight) AS w
            FROM "Person" p JOIN "Event" e ON e.owner = p.id WHERE e.weight < {limit}
            GROUP BY p.segment ORDER BY p.segment'''}
        workloads = [
            ('selective_join', 'payments', {**sql, 'query': sql['query'].format(limit=2)}),
            ('wide_join', 'payments', {**sql, 'query': sql['query'].format(limit=200).replace('sum(e.weight)', 'sum(length(e.payload))')}),
            ('aggregate', 'payments', {'language': 'sql', 'query': 'SELECT count(*) AS n, sum(weight) AS w FROM "Event"'}),
            ('pagerank', 'topology', {'csr': {'generation': head['generation'], 'operation': {
                'kind': 'pagerank', 'damping': .85, 'tolerance': 1e-9, 'max_iterations': 3}}}),
            ('wcc', 'topology', {'csr': {'generation': head['generation'], 'operation': {'kind': 'wcc', 'max_iterations': 128}}}),
        ]
        procs = [coord, *(w[0] for w in workers)]
        for name, model, statement in workloads:
            samples = []
            for i in range(repeats + 1):
                rows, entry = sample(base, model, revisions[model], statement, procs)
                if i == 0 and name in ('pagerank', 'wcc'):
                    values = {int(json.loads(vertex)['key'][0]): value for vertex, value in rows}
                    assert len(rows) == 128 and set(values) == set(range(128)), name
                    if name == 'pagerank':
                        assert all(math.isclose(float(v), 1 / 128) for v in values.values())
                    else:
                        # Each 64-vertex ring has exactly one representative inside the ring.
                        for first in (0, 64):
                            representatives = {int(json.loads(values[k])['key'][0]) for k in range(first, first + 64)}
                            assert len(representatives) == 1 and first <= representatives.pop() < first + 64
                assert rows == expected.setdefault(name, rows), (count, name)
                if i:
                    samples.append(entry)
            ms = sorted(item['ms'] for item in samples)
            digest = hashlib.sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()
            print(json.dumps({'workers': count, 'fixed_total': total, 'min_rows': min_rows, 'workload': name,
                              'p50_ms': statistics.median(ms), 'p95_ms': ms[math.ceil(.95 * len(ms)) - 1],
                              'result_rows': len(rows), 'result_sha256': digest, 'samples': samples}), flush=True)
        name, model, statement = workloads[1]
        begin = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(sample, base, model, revisions[model], statement, procs) for _ in range(2)]
            for future in futures:
                assert future.result()[0] == expected[name]
        print(json.dumps({'workers': count, 'fixed_total': total, 'min_rows': min_rows, 'concurrent_queries': 2,
                          'throughput_queries_per_s': round(2 / (time.monotonic() - begin), 3)}), flush=True)
        if not failure:
            return
        identities = {ok(call(addr, '/internal/v1/identity', token=token)): i for i, (_, addr) in enumerate(workers)}
        request_id = str(uuid4())
        attempt_started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(call, base, f'/api/v1/models/{model}/query',
                                 {**statement, 'request_id': request_id, 'model_revision': revisions[model]})
            while not future.done() and time.monotonic() - attempt_started < 10:
                status, body = call(base, '/api/v1/queries/' + request_id)
                if status == 202 and body.get('execution_worker') in identities:
                    killed = identities[body['execution_worker']]
                    workers[killed][0].kill()
                    workers[killed][0].wait()
                    break
                time.sleep(.01)
            else:
                raise AssertionError('query finished before a Worker could be interrupted')
            status, body = future.result()
        assert status == 200 and body['rows'] == expected[name] or body.get('code') == 'worker_lost', (status, body)
        attempt_ms = round((time.monotonic() - attempt_started) * 1000)
        workers[killed] = start_worker(stack, killed, '-restart')
        recovery = str(uuid4())
        recovered = ok(call(base, f'/api/v1/models/{model}/query',
                            {**statement, 'request_id': recovery, 'model_revision': revisions[model]}))
        assert recovered['rows'] == expected[name]
        coord.kill()
        coord.wait()
        _, resumed = stack.enter_context(process(coordinator, root / 'restart.log', 'GraphLake admin '))
        login(resumed, password)
        assert ok(call(resumed, '/api/v1/queries/' + recovery))['rows'] == expected[name]
        print(json.dumps({'workers': count, 'worker_killed_during_query': True, 'attempt_ms': attempt_ms,
                          'worker_loss_outcome': body.get('code', 'correct_result'), 'worker_restart': 'matched',
                          'coordinator_restart': 'matched'}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cli', type=lambda path: Path(path).resolve())
    parser.add_argument('--etcd', default='http://127.0.0.1:22379')
    parser.add_argument('--rows', type=int, default=50000)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--workers', nargs='+', type=int, choices=(1, 2, 3), default=[1, 2, 3])
    parser.add_argument('--skip-faults', action='store_true')
    parser.add_argument('--min-rows', type=int, default=0)
    args = parser.parse_args()
    assert len(os.sched_getaffinity(0)) >= 6 and args.rows > 0 and args.repeats > 0
    with tempfile.TemporaryDirectory() as work:
        root = Path(work)
        models = fixture(root, args.cli, args.rows)
        expected = {}
        for fixed_total in (False, True):
            for workers in args.workers:
                run(root / f'run-{fixed_total}-{workers}', args.cli.with_name('graphlake-admin'), models, args.etcd,
                    workers, fixed_total, args.repeats, expected, not fixed_total and not args.skip_faults, args.min_rows)
