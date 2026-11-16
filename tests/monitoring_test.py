"""Regression tests for published data and capacity alarms, including recovery."""
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

file = Path(__file__).resolve().parents[1] / 'deploy/graphlake-monitor'
spec = importlib.util.spec_from_loader('graphlake_monitor', importlib.machinery.SourceFileLoader('graphlake_monitor', str(file)))
monitoring = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitoring)


class MonitorTest(unittest.TestCase):
    def test_published_csr_freshness_discovery_loss_and_recovery(self):
        revision = 'r1'
        now = time.time_ns() // 1_000_000
        inventory = [{'id': 'accounts', 'active_revision': revision}]
        freshness = {
            'model_revision': revision, 'local_tables': [
                {'source': 'replica.accounts', 'generation': 't1', 'observed_at_ms': now - 20_000,
                 'reconciled_at_ms': now - 100_000, 'last_status': 'failed'},
            ], 'csr': {'generation': 'g1', 'current': False, 'built_at_ms': now - 30_000},
            'refresh': {'status': 'failed'},
        }
        with patch.dict(os.environ, {'TEST_MONITOR_TOKEN': 'fixture-token'}):
            monitor = monitoring.Monitor({
                'ingress': {'https': 'http://127.0.0.1:7337'},
                'service_account': {'username': 'svc/monitor', 'token_env': 'TEST_MONITOR_TOKEN'},
                'canaries': {'accounts': {'csr': {'operation': {'kind': 'wcc'}}, 'expected_rows': [['1', '1']]}},
                'freshness': {'accounts': {'max_observation_age_seconds': 5, 'max_reconciliation_age_seconds': 90,
                                           'max_csr_age_seconds': 5, 'require_current_csr': True}},
            })

        def request(url, headers=None, body=None):
            route = url.rsplit('/', 1)[-1]
            if route == 'healthz':
                return 200, {'status': 'alive'}
            self.assertIn('Authorization', headers)
            if route == 'models':
                return (200, inventory) if inventory else (503, None)
            if route == 'readiness':
                return 200, {'status': 'ready', 'model_revision': revision}
            if route == 'freshness':
                return 200, freshness
            self.assertEqual(body['csr']['generation'], freshness['csr']['generation'])
            return 200, {'model_revision': revision, 'rows': [['1', '1']]}

        monitor.json = request
        monitor.cycle()
        broken = monitor.metrics().decode()
        for reason in ['sync_failed', 'observation_stale', 'reconciliation_stale', 'csr_stale', 'csr_behind', 'csr_refresh_failed']:
            self.assertIn(f'reason="{reason}"}} 1', broken)
        self.assertNotIn('model_check_failed', broken)
        inventory.clear()
        monitor.cycle()
        self.assertIn('model_discovery_up 0', monitor.metrics().decode())
        self.assertIn('reason="csr_behind"', monitor.metrics().decode())

        inventory.append({'id': 'accounts', 'active_revision': revision})
        freshness['local_tables'][0].update(observed_at_ms=now, reconciled_at_ms=now, last_status='succeeded')
        freshness['csr'].update(current=True, built_at_ms=now, generation='g2')
        freshness['refresh']['status'] = 'succeeded'
        monitor.cycle()
        healthy = monitor.metrics().decode()
        self.assertIn('model_discovery_up 1', healthy)
        self.assertNotIn('freshness_check_failed', healthy)
        self.assertNotIn('model_check_failed', healthy)

    def test_missing_low_and_recovered_volume(self):
        with tempfile.TemporaryDirectory() as temp:
            location = Path(temp) / 'catalog'
            monitor = monitoring.Monitor({'volumes': {'catalog': {'path': str(location), 'min_free_bytes': 1}}})
            monitor.cycle()
            self.assertIn('volume_up{volume="catalog"} 0', monitor.metrics().decode())
            location.mkdir()
            monitor.volumes['catalog']['min_free_bytes'] = 1 << 63
            monitor.cycle()
            self.assertIn('volume_low{volume="catalog"} 1', monitor.metrics().decode())
            monitor.volumes['catalog']['min_free_bytes'] = 1
            monitor.cycle()
            self.assertIn('volume_low{volume="catalog"} 0', monitor.metrics().decode())


if __name__ == '__main__':
    unittest.main()
