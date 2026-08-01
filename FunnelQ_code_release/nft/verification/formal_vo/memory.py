from __future__ import annotations

import os


def peak_rss_mb() -> float:
    """Return process peak resident memory, not the current working set.

    On Unix, ``resource.ru_maxrss`` is the kernel-maintained peak.  On
    Windows, ``PeakWorkingSetSize`` is used.  The previous implementation
    preferred psutil's current RSS on Linux, which under-reported verifier
    peaks after large temporary structures had been released.
    """
    if os.name == "nt":
        try:
            import psutil

            info = psutil.Process(os.getpid()).memory_info()
            peak = getattr(info, "peak_wset", None)
            if peak is not None:
                return float(peak) / (1024.0 * 1024.0)
        except ImportError:
            pass

        import ctypes
        from ctypes import wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(counters)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        if not ctypes.windll.psapi.GetProcessMemoryInfo(
            handle, ctypes.byref(counters), counters.cb
        ):
            raise OSError("GetProcessMemoryInfo failed")
        return float(counters.PeakWorkingSetSize) / (1024.0 * 1024.0)

    import resource

    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if hasattr(os, "uname") and os.uname().sysname == "Darwin":
        # macOS reports bytes; Linux and other common Unix platforms report KiB.
        return value / (1024.0 * 1024.0)
    return value / 1024.0
