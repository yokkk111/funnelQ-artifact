import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(os.name == 'posix', 'Linux process-group enforcement')
class WatchdogTests(unittest.TestCase):
    def test_memory_ceiling_is_enforced_outside_worker(self):
        root = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as td:
            out, logs = Path(td)/'session', Path(td)/'monitor'
            p = subprocess.run([sys.executable,str(root/'resident_watchdog.py'),
                '--out',str(logs),'--memory-mib','1','--session-seconds','15','--',
                sys.executable,str(root/'watchdog_test_child.py'),'--out',str(out)],
                capture_output=True,text=True,timeout=20)
            self.assertNotEqual(p.returncode,0)
            report = json.loads((logs/'monitor.json').read_text())
            self.assertEqual(report['resource_failure'],'memory_limit')
            self.assertGreater(report['peak_process_tree_rss_bytes'],2**20)

    def test_native_hang_kills_only_owned_process_group(self):
        import psutil
        root = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)/'session'
            logs = Path(td)/'monitor'
            p = subprocess.run([sys.executable,str(root/'resident_watchdog.py'),
                '--out',str(logs),'--session-seconds','15','--',sys.executable,
                str(root/'watchdog_test_child.py'),'--out',str(out)],capture_output=True,text=True,timeout=20)
            self.assertNotEqual(p.returncode,0)
            report = json.loads((logs/'monitor.json').read_text())
            self.assertEqual(report['resource_failure'],'phase_timeout')
            child = json.loads((out/'child.json').read_text())['pid']
            try:
                self.assertEqual(psutil.Process(child).status(),psutil.STATUS_ZOMBIE)
            except psutil.NoSuchProcess:
                pass
            self.assertFalse(report['external_database_processes_killed'])


if __name__ == '__main__':unittest.main()
