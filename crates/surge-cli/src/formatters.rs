use std::time::Duration;

pub(crate) fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if duration.as_millis() < 1000 {
        format!("{}ms", duration.as_millis())
    } else if secs < 60 {
        format!("{:.1}s", duration.as_secs_f64())
    } else {
        let mins = secs / 60;
        let rem = secs % 60;
        format!("{mins}m{rem:02}s")
    }
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.1} GB", value / GIB)
    } else if value >= MIB {
        format!("{:.1} MB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) fn format_signed_bytes(bytes: i64) -> String {
    if bytes < 0 {
        "-".to_string()
    } else {
        format_bytes(bytes as u64)
    }
}

pub(crate) fn format_byte_progress(done: u64, total: u64, verb: &str) -> String {
    if total == 0 {
        return format!("{verb} {}", format_bytes(done));
    }

    let done = done.min(total);
    let pct = (done as f64 / total as f64) * 100.0;
    format!("{verb} {} / {} ({pct:.0}%)", format_bytes(done), format_bytes(total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration_prefers_ms_for_subsecond_values() {
        assert_eq!(format_duration(Duration::from_millis(995)), "995ms");
    }

    #[test]
    fn format_duration_uses_seconds_for_under_a_minute() {
        assert_eq!(format_duration(Duration::from_secs_f64(12.3)), "12.3s");
    }

    #[test]
    fn format_duration_uses_minutes_for_60s_and_above() {
        assert_eq!(format_duration(Duration::from_secs(60)), "1m00s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_duration(Duration::from_secs(725)), "12m05s");
    }

    #[test]
    fn format_byte_progress_includes_percent() {
        assert_eq!(
            format_byte_progress(23 * 1024 * 1024, 102 * 1024 * 1024, "uploaded"),
            "uploaded 23.0 MB / 102.0 MB (23%)"
        );
    }
}
