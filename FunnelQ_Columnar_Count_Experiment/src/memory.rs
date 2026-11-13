                                                                             
#[cfg(windows)]
pub fn peak_bytes() -> Option<u64> {
    #[repr(C)]
    struct Counters {
        cb: u32,
        faults: u32,
        peak: usize,
        current: usize,
        pp: usize,
        p: usize,
        pnp: usize,
        np: usize,
        page: usize,
        peakpage: usize,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(p: *mut core::ffi::c_void, c: *mut Counters, size: u32) -> i32;
    }
    let mut c = Counters {
        cb: std::mem::size_of::<Counters>() as u32,
        faults: 0,
        peak: 0,
        current: 0,
        pp: 0,
        p: 0,
        pnp: 0,
        np: 0,
        page: 0,
        peakpage: 0,
    };
    let ok = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut c, c.cb) };
    if ok != 0 {
        Some(c.peak as u64)
    } else {
        None
    }
}
#[cfg(not(windows))]
pub fn peak_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|l| l.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()
        .map(|n| n * 1024)
}
