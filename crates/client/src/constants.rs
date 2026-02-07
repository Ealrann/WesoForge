pub const PROGRESS_BAR_STEPS: u64 = 1_000;  // Report every 0.1%

pub fn default_classgroup_element() -> [u8; 100] {
    let mut el = [0u8; 100];
    el[0] = 0x08;
    el
}
