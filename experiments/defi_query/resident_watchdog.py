import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path
import psutil


def group_rss(pid, external=()):
    found = {}
    for root in [pid, *external]:
        try:
            p = psutil.Process(root)
            for x in [p, *p.children(recursive=True)]:
                found[x.pid] = x
        except psutil.NoSuchProcess:
            pass
    total = 0
    for p in found.values():
        try:
            total += p.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total


def stop_group(p):

    try:
        os.killpg(p.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(p.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    p.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--memory-mib', type=int, default=65536)
    ap.add_argument('--session-seconds', type=float, default=7200)
    ap.add_argument('--server-pid', action='append', type=int, default=[])
    ap.add_argument('--external-memory-file', type=Path)
    ap.add_argument('command', nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.command[1:] if args.command[:1] == ['--'] else args.command
    if os.name != 'posix' or not cmd:
        ap.error('Linux and a command are required')
    dest = Path(cmd[cmd.index('--out') + 1])
    if dest.exists():
        ap.error('refusing an existing session output path')
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / 'command.json').write_text(json.dumps(cmd), encoding='utf-8')
    peak, samples, reason, stage = 0, 0, None, None
    started = time.monotonic()
    with (args.out / 'stdout.log').open('x') as stdout, (args.out / 'stderr.log').open('x') as stderr:
        p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            while p.poll() is None:
                from external_memory import read_external_memory
                try:
                    rss = group_rss(p.pid, args.server_pid) + read_external_memory(args.external_memory_file)
                except Exception:
                    reason = 'external_database_monitor_lost'
                    stop_group(p)
                    break
                peak = max(peak, rss)
                samples += 1
                try:
                    stage = json.loads((dest / 'phase.json').read_text())
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                now = time.monotonic()
                if peak > args.memory_mib * 2**20:
                    reason = 'memory_limit'
                elif stage and now - stage['started_monotonic'] > stage['limit_seconds']:
                    reason = 'phase_timeout'
                elif now - started > args.session_seconds:
                    reason = 'session_timeout'
                if reason:
                    stop_group(p)
                    break
                time.sleep(.05)
        finally:

            stop_group(p)
    report = {'returncode': p.returncode, 'resource_failure': reason, 'last_phase': stage,
              'process_wall_seconds': time.monotonic()-started, 'peak_process_tree_rss_bytes': peak,
              'samples': samples, 'native_children_included': True,
              'external_database_processes_killed': False}
    (args.out / 'monitor.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report), flush=True)
    if reason or p.returncode:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
