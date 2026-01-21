use std::borrow::Cow;
use std::time::Duration;

pub fn field_vdf_label(field_vdf: i32) -> Cow<'static, str> {
    match field_vdf {
        1 => Cow::Borrowed("CC_EOS_VDF"),
        2 => Cow::Borrowed("ICC_EOS_VDF"),
        3 => Cow::Borrowed("CC_SP_VDF"),
        4 => Cow::Borrowed("CC_IP_VDF"),
        other => Cow::Owned(format!("UNKNOWN_VDF({other})")),
    }
}

pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let mut count = 0;
    for ch in s.chars().rev() {
        if count != 0 && count % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
        count += 1;
    }
    out.chars().rev().collect()
}

pub fn format_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms < 1000 {
        return format!("{ms}ms");
    }

    if ms < 60_000 {
        let seconds = ms / 1000;
        let millis = ms % 1000;
        return format!("{seconds}.{millis:03}s");
    }

    if ms < 3_600_000 {
        let minutes = ms / 60_000;
        let seconds = (ms % 60_000) / 1000;
        return format!("{minutes}m{seconds:02}s");
    }

    if ms < 86_400_000 {
        let hours = ms / 3_600_000;
        let minutes = (ms % 3_600_000) / 60_000;
        let seconds = (ms % 60_000) / 1000;
        return format!("{hours}h{minutes:02}m{seconds:02}s");
    }

    let days = ms / 86_400_000;
    let hours = (ms % 86_400_000) / 3_600_000;
    let minutes = (ms % 3_600_000) / 60_000;
    let seconds = (ms % 60_000) / 1000;
    format!("{days}d{hours:02}h{minutes:02}m{seconds:02}s")
}

pub fn format_job_done_line(
    height: u32,
    field_vdf: i32,
    status: &str,
    number_of_iterations: u64,
    duration: Duration,
) -> String {
    let field = field_vdf_label(field_vdf);
    format!(
        "Block: {height} ({field}), Status: {status}, Iterations: {}, Duration: {}",
        format_number(number_of_iterations),
        format_duration(duration)
    )
}

pub fn humanize_submit_reason(reason: &str) -> String {
    let s = reason.trim();
    if s.is_empty() {
        return "Unknown".to_string();
    }

    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "accepted" => return "Accepted".to_string(),
        "already_compact" => return "Already compact".to_string(),
        _ => {}
    }

    let mut out = String::with_capacity(lower.len());
    let mut capitalize_next = true;
    for ch in lower.chars() {
        if ch == '_' || ch == '-' {
            out.push(' ');
            capitalize_next = true;
            continue;
        }
        if capitalize_next {
            out.extend(ch.to_uppercase());
            capitalize_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}
