use anyhow::{Result, bail};

/// Parse a time argument: either ISO 8601 or relative duration (5m, 1h, 2d).
/// Relative durations are interpreted as "ago" (subtracted from now).
/// Returns microseconds since epoch.
pub fn parse_time(s: &str) -> Result<u64> {
    let try_relative = |input: &str| -> Option<(u64, char)> {
        let last = input.chars().last()?;
        if !matches!(last, 'd' | 'h' | 'm' | 's') {
            return None;
        }
        let num_str = &input[..input.len() - 1];
        let num: u64 = num_str.parse().ok()?;
        Some((num, last))
    };

    if let Some((num, unit)) = try_relative(s) {
        let secs = match unit {
            'd' => num * 86400,
            'h' => num * 3600,
            'm' => num * 60,
            's' => num,
            _ => unreachable!(),
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        Ok(now - secs * 1_000_000)
    } else {
        use chrono::{DateTime, NaiveDateTime};
        if let Ok(dt) = s.parse::<DateTime<chrono::FixedOffset>>() {
            return Ok(dt.timestamp_micros() as u64);
        }
        if let Ok(dt) = s.parse::<DateTime<chrono::Utc>>() {
            return Ok(dt.timestamp_micros() as u64);
        }
        if let Ok(dt) = s.parse::<NaiveDateTime>() {
            return Ok(dt.and_utc().timestamp_micros() as u64);
        }
        bail!(
            "cannot parse time {s:?}, expected ISO 8601 (e.g. 2026-03-13T17:00:00Z) or relative (5m, 1h)"
        )
    }
}

pub fn format_epoch_secs(secs: u64) -> String {
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
    let (year, month, day) = days_to_date(days);
    format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02} UTC")
}

fn days_to_date(days_since_epoch: u64) -> (u64, u64, u64) {
    let z = days_since_epoch + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
