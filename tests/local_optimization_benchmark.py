"""Compare first visible query output and local-copy I/O between two Linux builds."""

import argparse
import hashlib
import json
import os
import sqlite3
import statistics
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.request import Request, urlopen


def catalog(root, table, source='db: {type: sqlite, path: source.db}', keys='id'):
    path = root / 'payments.yaml'
    properties = 'tenant, name, version' if keys == 'id' else 'name, version'
    path.write_text(f'''
graph: benchmark
sources:
  {source}
nodes:
  Account: {{source: db.{table}, key: [{keys}], properties: [{properties}]}}
''')
    return path


def initialize(root, rows, narrow=False):
    db = sqlite3.connect(root / 'source.db')
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('''CREATE TABLE accounts (
          tenant INTEGER NOT NULL, id TEXT NOT NULL, name TEXT NOT NULL, version INTEGER NOT NULL,
          PRIMARY KEY(tenant,id)) STRICT''')
    payload = "printf('%08x',i)" if narrow else "printf('%016x-%064x-%064x',i,i*373555,i*7919)"
    db.execute(f'''WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
        INSERT INTO accounts
        SELECT i%128, printf('00000000-0000-4000-8000-%012x',i),
               {payload}, i FROM n''', (rows,))
    db.commit()
    return db


def query(binary, path, sentence, diagnostics=False):
    cmd = [binary, '--catalog', path, '--batch-size', '256']
    if diagnostics:
        cmd.append('--diagnostics')
    cmd += ['query', '--language', 'sql', sentence]
    start = time.perf_counter()
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
        first = proc.stdout.readline()
        first_ms = (time.perf_counter() - start) * 1000
        output = first + proc.stdout.read()
        stderr = proc.stderr.read().decode()
        if proc.wait() != 0:
            raise RuntimeError(stderr)
    metrics = json.loads(stderr.splitlines()[-1])['metrics'] if diagnostics else {}
    values = output.splitlines()
    return values, first_ms, metrics


def ordered_digest(binary, path, sentence, timeout, memory_mib):
    cmd = [binary, '--catalog', path, '--batch-size', '256', '--timeout-ms', str(timeout * 1000),
           '--memory-mib', str(memory_mib), '--spill-mib', '8192', 'query', '--language', 'sql', sentence]
    rows = 0
    digest = hashlib.sha256()
    with subprocess.Popen(cmd, stdout=subprocess.PIPE) as proc:
        while chunk := proc.stdout.read(1024 * 1024):
            rows += chunk.count(b'\n')
            digest.update(chunk)
        if proc.wait() != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
    return rows, digest.hexdigest()


def stream(args):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        if args.catalog is None:
            db = initialize(root, args.rows)
            db.close()
            path = catalog(root, 'accounts', keys='tenant,id')
        else:
            path = args.catalog
        cases = [
            ('full', 'SELECT id, name FROM "Account"'),
            ('early', 'SELECT id, name FROM "Account" WHERE length(name) > 0 LIMIT 1'),
        ]
        for case, sql in cases:
            samples = [[], []]
            reference = None
            for i in range(args.runs + 1):
                for side in ((0, 1) if i % 2 == 0 else (1, 0)):
                    rows, first, metrics = query(args.binaries[side], path, sql, diagnostics=True)
                    if case == 'full':
                        digest = hashlib.sha256(b'\n'.join(sorted(rows))).hexdigest()
                        expected = (len(rows), digest)
                        reference = reference or expected
                        assert reference == expected
                    else:
                        assert len(rows) == 1 and json.loads(rows[0])['name']
                    if i:
                        samples[side].append({
                            'first_ms': round(first, 3), 'elapsed_ms': metrics['elapsed_ms'],
                            'reads': metrics['reads'],
                        })
            print(json.dumps({'case': case, 'samples': samples,
                              'p50_first_ms': [statistics.median(s['first_ms'] for s in group) for group in samples]}), flush=True)


@contextmanager
def server(binary, root, env=None):
    assets = root / 'assets'
    assets.mkdir()
    (assets / 'index.html').write_text('<title>Benchmark</title>')
    with subprocess.Popen([binary, '--catalog-dir', root, '--assets-dir', assets, '--port', '0', '--insecure-dev'],
                          stderr=subprocess.PIPE, text=True, env=env) as proc:
        try:
            url = next(line.split()[-1] for line in proc.stderr if line.startswith('GraphLake admin http'))
            yield proc, url
        finally:
            proc.kill()
            proc.wait()


def request(url, path, body=None):
    req = Request(url + '/api/v1/' + path, data=None if body is None else json.dumps(body).encode(),
                  headers={'Origin': url, 'Content-Type': 'application/json'})
    with urlopen(req, timeout=30) as response:
        return json.load(response)


def refresh(url, mode, timeout=120):
    path = 'models/payments/local-tables'
    run = request(url, path + '/accounts/load', {'mode': mode})
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        current = request(url, path)[0]['history'][0]
        assert current['id'] == run['id']
        if current['status'] != 'running':
            assert current['status'] == 'succeeded', current
            return current
        time.sleep(.004)
    raise TimeoutError(mode)


def io(pid):
    fields = dict(line.split(': ') for line in Path(f'/proc/{pid}/io').read_text().splitlines())
    return {key: int(fields[key]) for key in ('rchar', 'wchar', 'read_bytes', 'write_bytes')}


def sync(args):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = initialize(root, args.rows, args.narrow)
        if args.indexed_watermark:
            db.execute('CREATE INDEX accounts_version ON accounts(version)')
        keys = ','.join(args.keys)
        folders = [root / 'baseline', root / 'candidate']
        for folder in folders:
            folder.mkdir()
            catalog(folder, 'accounts', source=f"db: {{type: sqlite, path: '{root / 'source.db'}'}}", keys=keys)
        admins = [binary.with_name('graphlake-admin') for binary in args.binaries]
        env = [dict(os.environ, TMPDIR=str(folder)) for folder in folders]
        with server(admins[0], folders[0], env[0]) as base, server(admins[1], folders[1], env[1]) as next_:
            copies = []
            full_ms = []
            for proc, url in (base, next_):
                model = request(url, 'models/payments')
                copies.append(Path(request(url, 'models/payments/local-tables', {
                    'model_revision': model['revision'], 'name': 'accounts', 'source_table': 'db.accounts',
                    'columns': ['tenant', 'id', 'name', 'version'], 'key_columns': args.keys,
                    'watermark_column': 'version', 'mode': 'incremental',
                    'interval_seconds': None, 'timeout_seconds': args.sync_timeout,
                })['path']))
                initial = refresh(url, 'full', args.sync_timeout)
                assert initial['total_rows'] == args.rows
                full_ms.append(initial['finished_at_ms'] - initial['started_at_ms'])
            samples = [[], []]
            compactions = []
            previous_parts = 1
            for step in range(args.rounds):
                version = args.rows + step + 1
                ids = [1 + (step * 1307 + offset * 8191) % args.rows for offset in range(100)]
                with db:
                    db.executemany('UPDATE accounts SET version=?, name=? WHERE tenant=? AND id=?',
                                   [(version, f'updated-{version}-{i}', i % 128,
                                     f'00000000-0000-4000-8000-{i:012x}') for i in ids])
                order = (0, 1) if step % 2 == 0 else (1, 0)
                for side in order:
                    proc, url = (base, next_)[side]
                    before = io(proc.pid)
                    run = refresh(url, 'incremental', args.sync_timeout)
                    after = io(proc.pid)
                    assert run['total_rows'] == args.rows
                    samples[side].append({
                        'ms': run['finished_at_ms'] - run['started_at_ms'],
                        **{key: after[key] - before[key] for key in before},
                        'source_reads': run.get('source_reads'),
                    })
                head = copies[1].read_bytes()
                assert head.startswith(b'GLPQ2\n')
                count = len(json.loads(head[6:])['parts'])
                if count < previous_parts:
                    compactions.append(step + 1)
                previous_parts = count

            sql = 'SELECT tenant,id,name,version FROM "Account" ORDER BY id,tenant'
            verify = (args.sync_timeout, args.verify_memory_mib)
            expected = ordered_digest(args.binaries[1], folders[1] / 'payments.yaml', sql, *verify)
            assert expected[0] == args.rows
            for folder, copy, binary in zip(folders, copies, args.binaries):
                side = folder / 'check'
                side.mkdir()
                mapping = catalog(side, 'copy', source=f"db: {{type: parquet, tables: {{copy: ['{copy}']}}}}", keys=keys)
                actual = ordered_digest(binary, mapping, sql, *verify)
                assert actual == expected
            print(json.dumps({
                'rows': args.rows, 'rounds': args.rounds, 'keys': args.keys, 'verified_rows': expected[0],
                'full_ms': full_ms,
                'compacted_rounds': compactions,
                'p50_ms': [statistics.median(x['ms'] for x in side) for side in samples],
                'max_ms': [max(x['ms'] for x in side) for side in samples],
                'index_bytes': sum(f.stat().st_size for f in folders[1].glob('graphlake-local-keys-*/*') if f.is_file()),
                'total_io': [{key: sum(x[key] for x in side) for key in ('rchar','wchar','read_bytes','write_bytes')}
                             for side in samples],
                'samples': samples,
            }), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('stream', 'sync'))
    parser.add_argument('binaries', nargs=2, type=lambda p: Path(p).resolve())
    parser.add_argument('--rows', type=int, default=200000)
    parser.add_argument('--catalog', type=Path)
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--rounds', type=int, default=100)
    parser.add_argument('--key', choices=('uuid', 'compound'), default='compound')
    parser.add_argument('--indexed-watermark', action='store_true')
    parser.add_argument('--sync-timeout', type=int, default=120)
    parser.add_argument('--narrow', action='store_true', help='use an eight-character initial payload')
    parser.add_argument('--verify-memory-mib', type=int, default=256, help='memory limit for the result digest queries')
    args = parser.parse_args()
    args.keys = ['id'] if args.key == 'uuid' else ['tenant', 'id']
    (stream if args.mode == 'stream' else sync)(args)


if __name__ == '__main__':
    main()
