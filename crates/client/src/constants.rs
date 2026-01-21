use std::time::Duration;

pub const PROGRESS_BAR_STEPS: u64 = 20;
pub const IDLE_SLEEP: Duration = Duration::from_secs(10);
pub const DISCRIMINANT_BITS: usize = 1024;

pub fn default_classgroup_element() -> [u8; 100] {
    let mut el = [0u8; 100];
    el[0] = 0x08;
    el
}
