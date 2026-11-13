import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SUITES = [
    ('tests', 'test_selected5_query_contract_and_event_ids.py'),
    ('experiments/nft_query', 'tests.py'),
    ('experiments/defi_query', 'test_*.py'),
    ('experiments/defi_native_verification/revision_v4', 'test_*.py'),
    ('experiments/defi_verification', 'test_*.py'),
    ('experiments/verification_codec', 'test_*.py'),
    ('experiments/chain_verification', 'test_*.py'),
    ('experiments/yo_verification', 'test_*.py'),
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rust', action='store_true')
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    env = dict(os.environ)
    env['PYTHONPATH'] = str(ROOT) + os.pathsep + env.get('PYTHONPATH', '')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    for directory, pattern in SUITES:
        print(directory, flush=True)
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s',
                        str(ROOT / directory), '-p', pattern], cwd=ROOT, env=env, check=True)
    if args.rust:
        command = ['cargo', 'test', '--locked', '--manifest-path',
                   str(ROOT / 'FunnelQ_Columnar_Count_Experiment/Cargo.toml')]
        if args.offline:
            command.append('--offline')
        subprocess.run(command, cwd=ROOT, check=True)

if __name__ == '__main__':
    main()
