import argparse
import csv
import ctypes
import json
import os
import subprocess
import sys
import time
import uuid
from ctypes import wintypes
from pathlib import Path

import psutil


IS_WINDOWS = os.name == "nt"
MB = 1024 * 1024


if IS_WINDOWS:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION_CLASS = 1
    JOB_OBJECT_MEMORY_USAGE_INFORMATION_CLASS = 28

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    class JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    class JOBOBJECT_MEMORY_USAGE_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("JobMemory", ctypes.c_ulonglong),
            ("PeakJobMemoryUsed", ctypes.c_ulonglong),
        ]

    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.OpenJobObjectW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
    kernel32.OpenJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.QueryInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p
    ]
    kernel32.QueryInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE


def win_error(label):
    raise ctypes.WinError(ctypes.get_last_error(), label)


def create_job(name, memory_limit_bytes):
    handle = kernel32.CreateJobObjectW(None, name)
    if not handle:
        win_error("CreateJobObjectW")
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if memory_limit_bytes is not None:
        info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY
        info.JobMemoryLimit = int(memory_limit_bytes)
    ok = kernel32.SetInformationJobObject(
        handle,
        JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if not ok:
        kernel32.CloseHandle(handle)
        win_error("SetInformationJobObject")
    return handle


def open_and_join_job(name):
    # JOB_OBJECT_ASSIGN_PROCESS | JOB_OBJECT_QUERY
    handle = kernel32.OpenJobObjectW(0x0001 | 0x0004, False, name)
    if not handle:
        win_error("OpenJobObjectW")
    if not kernel32.AssignProcessToJobObject(handle, kernel32.GetCurrentProcess()):
        kernel32.CloseHandle(handle)
        win_error("AssignProcessToJobObject")
    return handle


def query_job(handle):
    ext = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    if not kernel32.QueryInformationJobObject(
        handle,
        JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
        ctypes.byref(ext),
        ctypes.sizeof(ext),
        None,
    ):
        win_error("QueryInformationJobObject(extended)")
    accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION()
    if not kernel32.QueryInformationJobObject(
        handle,
        JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION_CLASS,
        ctypes.byref(accounting),
        ctypes.sizeof(accounting),
        None,
    ):
        win_error("QueryInformationJobObject(accounting)")
    return {
        "job_memory_limit_bytes": int(ext.JobMemoryLimit),
        "peak_job_memory_used_bytes": int(ext.PeakJobMemoryUsed),
        "active_processes": int(accounting.ActiveProcesses),
        "total_processes": int(accounting.TotalProcesses),
        "terminated_processes": int(accounting.TotalTerminatedProcesses),
    }


def query_job_current_memory(handle):
    info = JOBOBJECT_MEMORY_USAGE_INFORMATION()
    if not kernel32.QueryInformationJobObject(
        handle,
        JOB_OBJECT_MEMORY_USAGE_INFORMATION_CLASS,
        ctypes.byref(info),
        ctypes.sizeof(info),
        None,
    ):
        win_error("QueryInformationJobObject(memory usage)")
    return {
        "job_memory_bytes": int(info.JobMemory),
        "peak_job_memory_used_bytes": int(info.PeakJobMemoryUsed),
    }


def job_child(job_name, cmd):
    handle = open_and_join_job(job_name)
    try:
        child = subprocess.Popen(cmd)
        return child.wait()
    finally:
        kernel32.CloseHandle(handle)


def process_tree_snapshot(root):
    processes = []
    try:
        processes = [root, *root.children(recursive=True)]
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    seen = set()
    totals = {
        "process_count": 0,
        "working_set_bytes": 0,
        "private_bytes": 0,
        "read_bytes": 0,
        "write_bytes": 0,
        "cpu_user_s": 0.0,
        "cpu_system_s": 0.0,
    }
    for proc in processes:
        if proc.pid in seen:
            continue
        seen.add(proc.pid)
        try:
            with proc.oneshot():
                mem = proc.memory_info()
                io = proc.io_counters()
                cpu = proc.cpu_times()
            totals["process_count"] += 1
            totals["working_set_bytes"] += int(getattr(mem, "wset", getattr(mem, "rss", 0)))
            totals["private_bytes"] += int(getattr(mem, "private", 0))
            totals["read_bytes"] += int(getattr(io, "read_bytes", 0))
            totals["write_bytes"] += int(getattr(io, "write_bytes", 0))
            totals["cpu_user_s"] += float(getattr(cpu, "user", 0.0))
            totals["cpu_system_s"] += float(getattr(cpu, "system", 0.0))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return totals


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir")
    ap.add_argument("--sample-interval-ms", type=int, default=100)
    ap.add_argument("--job-memory-limit-mb", type=float)
    ap.add_argument("--min-system-available-mb", type=float)
    ap.add_argument("--timeout-sec", type=float)
    ap.add_argument(
        "--phase-marker",
        help="If set, report separate memory peaks only while this marker file exists.",
    )
    ap.add_argument("--max-rss-mb", type=float, help="Deprecated compatibility alias for --job-memory-limit-mb")
    ap.add_argument("--job-child", help=argparse.SUPPRESS)
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    return ap.parse_args()


def main():
    args = parse_args()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if args.job_child:
        if not IS_WINDOWS:
            raise SystemExit("--job-child is Windows-only")
        if not cmd:
            raise SystemExit("Missing child command")
        raise SystemExit(job_child(args.job_child, cmd))

    if not args.outdir or not cmd:
        raise SystemExit("--outdir and a command after '--' are required")
    if not IS_WINDOWS:
        raise SystemExit("This monitor requires Windows Job Objects")

    limit_mb = args.job_memory_limit_mb
    if limit_mb is None:
        limit_mb = args.max_rss_mb
    if limit_mb is None:
        raise SystemExit("--job-memory-limit-mb is required")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    job_name = f"HVCEQ_{os.getpid()}_{uuid.uuid4().hex}"
    job_handle = create_job(job_name, int(limit_mb * MB))
    wrapper_cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--job-child",
        job_name,
        "--",
        *cmd,
    ]

    samples = []
    stop_reason = ""
    peak_tree_working_set = 0
    peak_tree_private = 0
    min_system_available = None
    phase_marker = Path(args.phase_marker) if args.phase_marker else None
    phase_sample_count = 0
    phase_peak_job_commit = 0
    phase_peak_tree_working_set = 0
    phase_peak_tree_private = 0
    phase_min_system_available = None
    phase_first_t = None
    phase_last_t = None
    t0 = time.perf_counter()
    wrapper = subprocess.Popen(wrapper_cmd)
    root = psutil.Process(wrapper.pid)

    try:
        while wrapper.poll() is None:
            now = time.perf_counter() - t0
            tree = process_tree_snapshot(root)
            job = query_job(job_handle)
            current_job_memory = query_job_current_memory(job_handle)
            available = int(psutil.virtual_memory().available)
            peak_tree_working_set = max(peak_tree_working_set, tree["working_set_bytes"])
            peak_tree_private = max(peak_tree_private, tree["private_bytes"])
            min_system_available = available if min_system_available is None else min(min_system_available, available)
            samples.append({
                "t_s": round(now, 6),
                "job_peak_commit_mb": job["peak_job_memory_used_bytes"] / MB,
                "job_current_commit_mb": current_job_memory["job_memory_bytes"] / MB,
                "tree_working_set_mb": tree["working_set_bytes"] / MB,
                "tree_private_bytes_mb": tree["private_bytes"] / MB,
                "system_available_mb": available / MB,
                "tree_process_count": tree["process_count"],
                "job_active_processes": job["active_processes"],
                "read_bytes": tree["read_bytes"],
                "write_bytes": tree["write_bytes"],
                "query_phase_active": bool(phase_marker and phase_marker.exists()),
            })
            if phase_marker is not None and phase_marker.exists():
                phase_sample_count += 1
                phase_peak_job_commit = max(
                    phase_peak_job_commit, current_job_memory["job_memory_bytes"]
                )
                phase_peak_tree_working_set = max(
                    phase_peak_tree_working_set, tree["working_set_bytes"]
                )
                phase_peak_tree_private = max(
                    phase_peak_tree_private, tree["private_bytes"]
                )
                phase_min_system_available = (
                    available
                    if phase_min_system_available is None
                    else min(phase_min_system_available, available)
                )
                phase_first_t = now if phase_first_t is None else phase_first_t
                phase_last_t = now
            if (
                args.min_system_available_mb is not None
                and available / MB < args.min_system_available_mb
            ):
                stop_reason = "system_available_below_floor"
                kernel32.TerminateJobObject(job_handle, 137)
                break
            if args.timeout_sec is not None and now >= args.timeout_sec:
                stop_reason = "timeout"
                kernel32.TerminateJobObject(job_handle, 124)
                break
            time.sleep(max(1, args.sample_interval_ms) / 1000.0)

        rc = wrapper.wait()
        # The wrapper waits for the target. Give any unexpected descendants a
        # brief chance to exit, then terminate the whole Job if they remain.
        deadline = time.monotonic() + 2.0
        final_job = query_job(job_handle)
        while final_job["active_processes"] and time.monotonic() < deadline:
            time.sleep(0.05)
            final_job = query_job(job_handle)
        if final_job["active_processes"]:
            if not stop_reason:
                stop_reason = "lingering_job_processes"
            kernel32.TerminateJobObject(job_handle, 137)
            deadline = time.monotonic() + 2.0
            while final_job["active_processes"] and time.monotonic() < deadline:
                time.sleep(0.05)
                final_job = query_job(job_handle)
        if rc != 0 and not stop_reason:
            # Windows returns an implementation-dependent failure code when the
            # Job commit hard limit prevents an allocation.
            stop_reason = "target_failed_or_job_memory_limit"
        t1 = time.perf_counter()
        report = {
            "return_code": rc,
            "stop_reason": stop_reason,
            "wall_s_external_monitor": t1 - t0,
            "sample_count": len(samples),
            "limits": {
                "job_commit_hard_limit_mb": limit_mb,
                "system_available_floor_mb": args.min_system_available_mb,
                "timeout_sec": args.timeout_sec,
                "kill_on_job_close": True,
                "breakaway_allowed": False,
            },
            "job_commit": {
                "peak_job_commit_mb": final_job["peak_job_memory_used_bytes"] / MB,
            },
            "process_tree_working_set": {
                "peak_tree_working_set_mb": peak_tree_working_set / MB,
                "note": "Summed working sets may double-count shared physical pages.",
            },
            "process_tree_private_bytes": {
                "peak_tree_private_bytes_mb": peak_tree_private / MB,
            },
            "system_memory": {
                "minimum_available_mb": (min_system_available or 0) / MB,
            },
            "query_phase": {
                "marker_path": str(phase_marker) if phase_marker else "",
                "sample_count": phase_sample_count,
                "first_sample_t_s": phase_first_t,
                "last_sample_t_s": phase_last_t,
                "peak_job_commit_mb": phase_peak_job_commit / MB,
                "peak_tree_working_set_mb": phase_peak_tree_working_set / MB,
                "peak_tree_private_bytes_mb": phase_peak_tree_private / MB,
                "minimum_system_available_mb": (
                    (phase_min_system_available or 0) / MB
                ),
                "job_commit_note": (
                    "Maximum current JobMemory sampled during the marked query phase; "
                    "the Job hard limit remains active for the full process lifetime."
                ),
            },
            "job_accounting_after_root_exit": {
                "active_processes": final_job["active_processes"],
                "total_processes": final_job["total_processes"],
                "terminated_processes": final_job["terminated_processes"],
            },
            "timing_note": "External monitor wall time is not query latency; use target-reported internal timing.",
        }
        with open(outdir / "monitor_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        with open(outdir / "monitor_samples.csv", "w", newline="", encoding="utf-8") as f:
            fields = list(samples[0].keys()) if samples else ["t_s"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(samples)
        return rc
    finally:
        kernel32.CloseHandle(job_handle)


if __name__ == "__main__":
    raise SystemExit(main())
