//! Hand-rolled UTC RFC3339 formatting from `SystemTime` — avoids pulling in
//! `chrono`/`time` just to stamp receipt times on a device with ~22MB of
//! free RAM. `civil_from_days` is Howard Hinnant's well-known algorithm
//! (http://howardhinnant.github.io/date_algorithms.html), proleptic
//! Gregorian, no leap-second handling (not needed for telemetry stamps).

pub fn now_rfc3339() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format_rfc3339(now.as_secs() as i64)
}

fn format_rfc3339(unix_secs: i64) -> String {
    let days = unix_secs.div_euclid(86400);
    let secs_of_day = unix_secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let h = secs_of_day / 3600;
    let mi = (secs_of_day % 3600) / 60;
    let s = secs_of_day % 60;
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_1970_01_01() {
        assert_eq!(format_rfc3379_helper(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn day_31_is_february_1st() {
        assert_eq!(format_rfc3379_helper(31 * 86400), "1970-02-01T00:00:00Z");
    }

    #[test]
    fn mid_day_time_components_are_correct() {
        // 1970-01-01T13:45:30Z
        let secs = 13 * 3600 + 45 * 60 + 30;
        assert_eq!(format_rfc3379_helper(secs), "1970-01-01T13:45:30Z");
    }

    #[test]
    fn known_recent_date_2026_07_01() {
        // 2026-07-01T00:00:00Z, cross-checked against an independent epoch
        // converter: 20635 days since 1970-01-01.
        assert_eq!(format_rfc3379_helper(20635 * 86400), "2026-07-01T00:00:00Z");
    }

    fn format_rfc3379_helper(secs: i64) -> String {
        format_rfc3339(secs)
    }
}
