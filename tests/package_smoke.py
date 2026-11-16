"""Start the extracted release and verify its static assets, local sync and crash recovery."""
import argparse
from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
from urllib.request import urlopen

from cluster_scaling_benchmark import ok
from local_backup_recovery import process_with_env
from release_lifecycle import ReleaseSite, activate, local, load


def verify(root):
    lines = (root / 'BUILD-INFO').read_text().splitlines()
    properties = dict(line.split('=', 1) for line in lines if '=' in line and '  ' not in line)
    files = {}
    for line in lines:
        if match := re.fullmatch(r'([0-9a-f]{64})  \./(.+)', line):
            files[match[2]] = match[1]
    actual = {path.relative_to(root).as_posix() for path in root.rglob('*') if path.is_file()}
    assert actual == files.keys() | {'BUILD-INFO'}, actual ^ (files.keys() | {'BUILD-INFO'})
    for path, expected in files.items():
        with (root / path).open('rb') as handle:
            assert file_digest(handle, 'sha256').hexdigest() == expected, path
    return properties


def smoke(args, root):
    properties = verify(args.archive_dir)
    if args.git_sha:
        assert properties['git_base'] == args.git_sha, properties
        assert properties['binary_origin'] == 'packaging_build'
    build = properties['build']
    cli = args.archive_dir / 'bin/graphlake'
    admin = args.archive_dir / 'bin/graphlake-admin'
    for binary in [cli, admin]:
        assert subprocess.check_output([binary, '--version'], text=True).strip().endswith(build)
    args.cli = admin
    with ReleaseSite(args, root / 'catalog', '') as site:
        site.public = site.base
        db = sqlite3.connect(site.root / 'accounts.db', isolation_level=None)
        try:
            db.executescript('CREATE TABLE accounts(id INTEGER PRIMARY KEY, target INTEGER, version INTEGER) STRICT;'
                             'INSERT INTO accounts VALUES (1, 2, 1), (2, 1, 1);')
            source = 'db.accounts'
            yaml = '''graph: Package smoke
sources:
  db: {type: sqlite, path: accounts.db}
nodes:
  Account: {source: db.accounts, key: [id], properties: [target, version]}
'''
            (site.root / 'accounts.yaml').write_text(yaml)
            cli_output = subprocess.check_output([cli, '--catalog', site.root / 'accounts.yaml',
                'query', '--language', 'sql', 'SELECT count(*) AS n FROM "Account"'], text=True)
            assert json.loads(cli_output) == {'n': 2}, cli_output
            command = [admin, '--catalog-dir', site.root, '--assets-dir',
                       args.archive_dir / 'web', '--port', site.port]
            env = dict(os.environ)
            with process_with_env(command, root / 'before.log', env, 'GraphLake admin ') as running:
                site.login()
                assert ok(site.call('/healthz'))['build'] == build
                assert ok(site.call('/readyz'))['status'] == 'ready'
                with urlopen(site.base + '/') as response:
                    html = response.read().decode()
                js = re.search(r'src="(/assets/[^"]+\.js)"', html)[1]
                with urlopen(site.base + js) as response:
                    assert response.read() == (args.archive_dir / 'web' / js.lstrip('/')).read_bytes()
                revision, _, first = activate(site, 'accounts', source, yaml)
                before = [['1', '2', '1'], ['2', '1', '1']]
                assert site.query(revision) == before
                db.execute('UPDATE accounts SET version=2,target=1 WHERE id=1')
                db.execute('BEGIN EXCLUSIVE')
                pending = load(site, 'accounts', wait=False)
                assert local(site, 'accounts')['history'][0]['id'] == pending['id']
                running.kill()
                assert running.wait(timeout=15) == -9
            db.execute('ROLLBACK')
            with process_with_env(command, root / 'after.log', env, 'GraphLake admin '):
                site.login()
                row = local(site, 'accounts')
                assert row['generation'] == first['committed_generation'] and row['history'][0]['status'] == 'interrupted'
                assert site.query(revision) == before
                retry = load(site, 'accounts')
                assert retry['committed_generation'] != first['committed_generation']
                assert site.query(revision) == [['1', '1', '2'], ['2', '1', '1']]
            return {'build': build, 'target': properties['target'], 'glibc': properties['glibc_required'],
                    'package_cli': True, 'shipped_ui': True, 'local_sync_restart_and_retry': True}
        finally:
            db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive_dir', type=lambda path: Path(path).resolve())
    parser.add_argument('--git-sha')
    args = parser.parse_args()
    args.etcd = 'http://127.0.0.1:1'
    args.bucket = 'unused'
    with tempfile.TemporaryDirectory(prefix='graphlake-package-') as root:
        print(json.dumps(smoke(args, Path(root)), sort_keys=True))
