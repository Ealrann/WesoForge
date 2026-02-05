//! Minimal thread-affinity helpers.

use std::io;

/// Set the current thread's CPU affinity to the provided CPU list.
///
/// On non-Linux platforms this is currently a no-op.
pub fn set_current_thread_affinity(cpus: &[usize]) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        set_current_thread_affinity_linux(cpus)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cpus;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn set_current_thread_affinity_linux(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }

    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };

    let word_bits = std::mem::size_of::<libc::c_ulong>() * 8;
    let words = std::mem::size_of::<libc::cpu_set_t>() / std::mem::size_of::<libc::c_ulong>();
    let bits: *mut libc::c_ulong = (&mut set as *mut libc::cpu_set_t).cast::<libc::c_ulong>();

    for &cpu in cpus {
        let idx = cpu / word_bits;
        if idx >= words {
            continue;
        }
        let bit = cpu % word_bits;
        unsafe {
            *bits.add(idx) |= (1 as libc::c_ulong) << bit;
        }
    }

    let res = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if res != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
