//! A wall-clock x-axis for the equity chart.
//!
//! The chart used to plot "seconds since the first point", which is hard to
//! read (what time was 2 000 s?) and cannot be compared with a log line or an
//! exchange statement. Here x is the Unix time in seconds and the axis shows
//! the operator's **local** time. Ticks fall on round local times (whole
//! minutes, hours, local midnights), whatever the span, and the axis label
//! names the zone so a screenshot is unambiguous.
//!
//! Everything is a pure function of numbers and a time zone, so it is tested
//! without a window and across daylight-saving changes.

use chrono::{DateTime, Duration, Local, NaiveDateTime, TimeZone, Timelike, Utc};
use egui_plot::GridMark;

/// Tick spacings, in seconds, from fine to coarse. Each is a round number of
/// minutes, hours or days, so ticks read as 14:00, 14:15, 14:30.
const STEPS: [i64; 17] = [
    1,
    5,
    15,
    30,
    60,
    5 * 60,
    15 * 60,
    30 * 60,
    3600,
    3 * 3600,
    6 * 3600,
    12 * 3600,
    86_400,
    2 * 86_400,
    7 * 86_400,
    14 * 86_400,
    30 * 86_400,
];

/// Unix seconds for a timestamp, with sub-second precision.
pub fn to_x(at: DateTime<Utc>) -> f64 {
    at.timestamp_millis() as f64 / 1000.0
}

/// The timestamp for a plot x (Unix seconds). `None` for a value no date can
/// represent, which a plot can ask about while the user drags it far away.
pub fn from_x(x: f64) -> Option<DateTime<Utc>> {
    if !x.is_finite() {
        return None;
    }
    DateTime::from_timestamp_millis((x * 1000.0).round() as i64)
}

/// The smallest step that keeps labels at least `min_spacing_secs` apart.
pub fn pick_step(min_spacing_secs: f64) -> i64 {
    STEPS
        .iter()
        .copied()
        .find(|step| *step as f64 >= min_spacing_secs)
        .unwrap_or(STEPS[STEPS.len() - 1])
}

/// How a tick is written, which depends on the step between ticks and on the
/// span on screen: seconds only when ticks are seconds apart, the date only
/// when the view crosses a local midnight (or ticks are days apart).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickStyle {
    /// `14:05:30`
    WithSeconds,
    /// `14:05`
    Minutes,
    /// `14:05` on ticks, `11-16` at a local midnight.
    MinutesWithDay,
    /// `11-16`
    DayOnly,
}

pub fn tick_style(step_secs: i64, spans_midnight: bool) -> TickStyle {
    if step_secs >= 86_400 {
        TickStyle::DayOnly
    } else if step_secs < 60 {
        TickStyle::WithSeconds
    } else if spans_midnight {
        TickStyle::MinutesWithDay
    } else {
        TickStyle::Minutes
    }
}

/// Grid marks on round **local** times between `bounds` (Unix seconds).
///
/// Round local times are not round UTC times (a +05:30 zone has its hours at
/// :30 UTC), so each candidate is found by rounding the *local* clock value
/// and converting back. `zone` is any `TimeZone`, which is how tests pin a
/// zone without touching the machine's.
pub fn grid_marks<Tz: TimeZone>(
    zone: &Tz,
    bounds: (f64, f64),
    base_step_secs: f64,
) -> Vec<GridMark> {
    let (lo, hi) = bounds;
    if !lo.is_finite() || !hi.is_finite() || hi <= lo {
        return Vec::new();
    }
    let step = pick_step(base_step_secs);
    // Cap the work: an absurd zoom-out must not generate a million marks.
    if (hi - lo) / step as f64 > 400.0 {
        return Vec::new();
    }
    let mut marks = Vec::new();
    let Some(start) = from_x(lo) else {
        return marks;
    };
    let mut tick = first_tick_at_or_after(zone, start, step);
    while let Some(at) = tick {
        let x = to_x(at);
        if x > hi {
            break;
        }
        marks.push(GridMark {
            value: x,
            step_size: significance(zone, at, step) as f64,
        });
        tick = next_tick(zone, at, step);
    }
    marks
}

/// Bigger for ticks that fall on bigger round times (an hour among minutes, a
/// midnight among hours), so the plot draws them stronger.
fn significance<Tz: TimeZone>(zone: &Tz, at: DateTime<Utc>, step: i64) -> i64 {
    let local = at.with_timezone(zone).naive_local();
    let seconds_into_day = i64::from(local.num_seconds_from_midnight());
    for bigger in [86_400, 6 * 3600, 3600, 15 * 60, 60] {
        if bigger > step && seconds_into_day % bigger == 0 {
            return bigger;
        }
    }
    step
}

/// The first round local time at or after `from`.
fn first_tick_at_or_after<Tz: TimeZone>(
    zone: &Tz,
    from: DateTime<Utc>,
    step: i64,
) -> Option<DateTime<Utc>> {
    if step >= 86_400 {
        return first_day_tick(zone, from, step / 86_400);
    }
    let local = from.with_timezone(zone).naive_local();
    let into_day = i64::from(local.num_seconds_from_midnight());
    let rounded_up = (into_day + step - 1) / step * step;
    let midnight = local.date().and_hms_opt(0, 0, 0)?;
    local_to_utc(zone, midnight + Duration::seconds(rounded_up))
        .filter(|at| *at >= from)
        .or_else(|| next_tick(zone, from, step))
}

fn first_day_tick<Tz: TimeZone>(
    zone: &Tz,
    from: DateTime<Utc>,
    every_days: i64,
) -> Option<DateTime<Utc>> {
    let local = from.with_timezone(zone).naive_local();
    let mut day = local.date();
    let midnight = |d: chrono::NaiveDate| d.and_hms_opt(0, 0, 0);
    if midnight(day)? < local {
        day += Duration::days(1);
    }
    // Align to a fixed epoch day so ticks do not shift as the view pans.
    let days = day.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?);
    let aligned = days.num_days().div_euclid(every_days) * every_days;
    let aligned = if aligned < days.num_days() {
        aligned + every_days
    } else {
        aligned
    };
    let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)? + Duration::days(aligned);
    local_to_utc(zone, midnight(date)?)
}

/// The tick after `at`. Steps are walked on the **local clock**, so a tick
/// that falls in a daylight-saving gap or overlap still lands on a real,
/// increasing instant.
fn next_tick<Tz: TimeZone>(zone: &Tz, at: DateTime<Utc>, step: i64) -> Option<DateTime<Utc>> {
    let local = at.with_timezone(zone).naive_local();
    if step >= 86_400 {
        let every = step / 86_400;
        let next_day = local.date() + Duration::days(every);
        return local_to_utc(zone, next_day.and_hms_opt(0, 0, 0)?).filter(|n| *n > at);
    }
    let mut candidate = local + Duration::seconds(step);
    // Skip a wall-clock time that does not exist (the spring-forward gap).
    for _ in 0..4 {
        if let Some(utc) = local_to_utc(zone, candidate)
            && utc > at
        {
            return Some(utc);
        }
        candidate += Duration::seconds(step);
    }
    Some(at + Duration::seconds(step))
}

/// A local wall-clock time as an instant. In the autumn overlap the earlier of
/// the two is used; in the spring gap there is none.
fn local_to_utc<Tz: TimeZone>(zone: &Tz, local: NaiveDateTime) -> Option<DateTime<Utc>> {
    zone.from_local_datetime(&local)
        .earliest()
        .map(|t| t.with_timezone(&Utc))
}

/// Whether a local midnight falls inside `bounds`.
pub fn spans_local_midnight<Tz: TimeZone>(zone: &Tz, bounds: (f64, f64)) -> bool {
    let (Some(lo), Some(hi)) = (from_x(bounds.0), from_x(bounds.1)) else {
        return false;
    };
    lo.with_timezone(zone).date_naive() != hi.with_timezone(zone).date_naive()
}

/// The text for one tick.
pub fn tick_label<Tz: TimeZone>(
    zone: &Tz,
    x: f64,
    style: TickStyle,
    is_midnight_among_times: bool,
) -> String
where
    Tz::Offset: std::fmt::Display,
{
    let Some(at) = from_x(x) else {
        return String::new();
    };
    let local = at.with_timezone(zone);
    match style {
        TickStyle::WithSeconds => local.format("%H:%M:%S").to_string(),
        TickStyle::Minutes => local.format("%H:%M").to_string(),
        TickStyle::MinutesWithDay if is_midnight_among_times => local.format("%m-%d").to_string(),
        TickStyle::MinutesWithDay => local.format("%H:%M").to_string(),
        TickStyle::DayOnly => local.format("%m-%d").to_string(),
    }
}

/// Whether `x` is a local midnight.
pub fn is_local_midnight<Tz: TimeZone>(zone: &Tz, x: f64) -> bool {
    from_x(x).is_some_and(|at| local_seconds_of_day(zone, at) == 0)
}

fn local_seconds_of_day<Tz: TimeZone>(zone: &Tz, at: DateTime<Utc>) -> u32 {
    at.with_timezone(zone).time().num_seconds_from_midnight()
}

/// Below this span the grid's ticks are seconds apart (about ten labels fit,
/// so 6 minutes is 36 s per label), so labels carry seconds. Above it ticks
/// are whole minutes or coarser and seconds would only be noise (`18:26:00`).
const SECONDS_BELOW_SPAN_SECS: f64 = 6.0 * 60.0;

/// The span of the visible range picks how much of a time each label shows:
/// seconds when the view is a few minutes wide, the date only on a local
/// midnight (or when the view is many days wide), minutes otherwise.
pub fn label_for_mark<Tz: TimeZone>(zone: &Tz, x: f64, bounds: (f64, f64)) -> String
where
    Tz::Offset: std::fmt::Display,
{
    let span = (bounds.1 - bounds.0).abs();
    let step_hint = if span < SECONDS_BELOW_SPAN_SECS {
        1 // a few minutes wide: ticks are seconds apart
    } else if span >= 3.0 * 86_400.0 {
        86_400
    } else {
        900
    };
    let style = tick_style(step_hint, spans_local_midnight(zone, bounds));
    tick_label(zone, x, style, is_local_midnight(zone, x))
}

/// The full local timestamp shown when hovering a point.
pub fn hover_label<Tz: TimeZone>(zone: &Tz, x: f64) -> String
where
    Tz::Offset: std::fmt::Display,
{
    match from_x(x) {
        Some(at) => at
            .with_timezone(zone)
            .format("%Y-%m-%d %H:%M:%S %:z")
            .to_string(),
        None => String::new(),
    }
}

/// `Local time (UTC+09:00)` for the axis title: the zone is part of the data.
pub fn axis_title<Tz: TimeZone>(zone: &Tz, at: DateTime<Utc>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    let offset = at.with_timezone(zone).format("%:z").to_string();
    format!("local time (UTC{offset})")
}

/// The machine's local zone.
pub fn local_zone() -> Local {
    Local
}

#[cfg(test)]
fn local_seconds<Tz: TimeZone>(zone: &Tz, at: DateTime<Utc>) -> u32 {
    local_seconds_of_day(zone, at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::FixedOffset;

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, s).unwrap()
    }

    fn kst() -> FixedOffset {
        FixedOffset::east_opt(9 * 3600).unwrap()
    }

    fn ist() -> FixedOffset {
        FixedOffset::east_opt(5 * 3600 + 1800).unwrap()
    }

    fn values(marks: &[GridMark]) -> Vec<f64> {
        marks.iter().map(|m| m.value).collect()
    }

    #[test]
    fn x_is_unix_seconds_and_round_trips() {
        let at = utc(2026, 11, 16, 5, 50, 51);
        assert_eq!(to_x(at), 1_794_808_251.0);
        assert_eq!(from_x(to_x(at)), Some(at));
        assert_eq!(from_x(f64::NAN), None);
        assert_eq!(from_x(f64::INFINITY), None);
        assert_eq!(from_x(1e30), None, "no panic on an absurd drag");
    }

    #[test]
    fn the_step_is_the_smallest_round_one_that_keeps_labels_apart() {
        assert_eq!(pick_step(0.5), 1);
        assert_eq!(pick_step(7.0), 15);
        assert_eq!(pick_step(61.0), 300);
        assert_eq!(pick_step(3601.0), 3 * 3600);
        assert_eq!(pick_step(1e12), 30 * 86_400, "capped, never panics");
    }

    #[test]
    fn ticks_land_on_round_local_times_not_round_utc_times() {
        // 14:00-15:00 KST is 05:00-06:00 UTC; a 15-minute tick must read :00/:15/:30/:45.
        let lo = to_x(utc(2026, 11, 16, 5, 7, 0));
        let hi = to_x(utc(2026, 11, 16, 6, 3, 0));
        let marks = grid_marks(&kst(), (lo, hi), 800.0);
        let labels: Vec<String> = marks
            .iter()
            .map(|m| tick_label(&kst(), m.value, TickStyle::Minutes, false))
            .collect();
        assert_eq!(labels, ["14:15", "14:30", "14:45", "15:00"]);
    }

    #[test]
    fn a_half_hour_offset_zone_still_gets_round_local_ticks() {
        // India is UTC+05:30: its round hours are :30 past in UTC.
        let lo = to_x(utc(2026, 11, 16, 0, 0, 0));
        let hi = to_x(utc(2026, 11, 16, 6, 0, 0));
        for m in grid_marks(&ist(), (lo, hi), 3000.0) {
            let secs = local_seconds(&ist(), from_x(m.value).unwrap());
            assert_eq!(secs % 3600, 0, "{} is not a round local hour", m.value);
        }
        assert!(!grid_marks(&ist(), (lo, hi), 3000.0).is_empty());
    }

    #[test]
    fn marks_are_inside_the_bounds_strictly_increasing_and_bounded_in_number() {
        for (lo, hi, base) in [
            (0.0, 90.0, 5.0),
            (1.7e9, 1.7e9 + 4000.0, 200.0),
            (1.7e9, 1.7e9 + 90_000.0, 3000.0),
            (1.7e9, 1.7e9 + 9_000_000.0, 200_000.0),
        ] {
            let marks = grid_marks(&kst(), (lo, hi), base);
            assert!(marks.len() < 400, "{} marks", marks.len());
            assert!(marks.iter().all(|m| m.value >= lo && m.value <= hi));
            assert!(values(&marks).windows(2).all(|w| w[0] < w[1]));
        }
    }

    #[test]
    fn degenerate_ranges_produce_no_marks_and_no_panic() {
        assert!(grid_marks(&kst(), (10.0, 10.0), 5.0).is_empty());
        assert!(grid_marks(&kst(), (10.0, 5.0), 5.0).is_empty());
        assert!(grid_marks(&kst(), (f64::NAN, 5.0), 5.0).is_empty());
        assert!(grid_marks(&kst(), (0.0, f64::INFINITY), 5.0).is_empty());
        assert!(
            grid_marks(&kst(), (0.0, 1e15), 1.0).is_empty(),
            "an absurd zoom-out must not allocate a million marks"
        );
    }

    #[test]
    fn a_local_midnight_is_the_strongest_tick_among_hours() {
        // 22:00 KST (13:00 UTC) to 03:00 KST next day (18:00 UTC).
        let lo = to_x(utc(2026, 11, 16, 13, 0, 0));
        let hi = to_x(utc(2026, 11, 16, 18, 0, 0));
        let marks = grid_marks(&kst(), (lo, hi), 3000.0);
        let midnight = marks
            .iter()
            .find(|m| local_seconds(&kst(), from_x(m.value).unwrap()) == 0)
            .expect("a local midnight is in range");
        assert!(
            marks
                .iter()
                .filter(|m| m.value != midnight.value)
                .all(|m| m.step_size < midnight.step_size),
            "midnight outranks the other hours"
        );
        assert!(spans_local_midnight(&kst(), (lo, hi)));
        assert!(!spans_local_midnight(&kst(), (lo, lo + 3600.0)));
    }

    #[test]
    fn labels_show_the_date_only_where_the_day_changes() {
        let at_midnight = to_x(utc(2026, 11, 16, 15, 0, 0)); // 00:00 KST on the 17th
        let later = to_x(utc(2026, 11, 16, 18, 0, 0));
        assert_eq!(
            tick_label(&kst(), at_midnight, TickStyle::MinutesWithDay, true),
            "11-17"
        );
        assert_eq!(
            tick_label(&kst(), later, TickStyle::MinutesWithDay, false),
            "03:00"
        );
        assert_eq!(
            tick_label(&kst(), later, TickStyle::WithSeconds, false),
            "03:00:00"
        );
        assert_eq!(
            tick_label(&kst(), later, TickStyle::DayOnly, false),
            "11-17"
        );
        assert_eq!(tick_label(&kst(), f64::NAN, TickStyle::Minutes, false), "");
    }

    #[test]
    fn the_style_follows_the_step_and_the_span() {
        assert_eq!(tick_style(5, false), TickStyle::WithSeconds);
        assert_eq!(tick_style(900, false), TickStyle::Minutes);
        assert_eq!(tick_style(900, true), TickStyle::MinutesWithDay);
        assert_eq!(tick_style(86_400, false), TickStyle::DayOnly);
    }

    #[test]
    fn day_ticks_are_local_midnights_aligned_so_they_do_not_shift_when_panning() {
        let lo = to_x(utc(2026, 11, 1, 0, 0, 0));
        let hi = to_x(utc(2026, 12, 20, 0, 0, 0));
        let a = grid_marks(&kst(), (lo, hi), 86_400.0 * 6.0);
        let b = grid_marks(&kst(), (lo + 86_400.0 * 3.0, hi), 86_400.0 * 6.0);
        for m in &a {
            assert_eq!(local_seconds(&kst(), from_x(m.value).unwrap()), 0);
        }
        let shared: Vec<f64> = values(&b)
            .into_iter()
            .filter(|v| values(&a).contains(v))
            .collect();
        assert_eq!(shared, values(&b), "the same days stay ticked after a pan");
    }

    #[test]
    fn labels_adapt_to_how_wide_the_view_is() {
        let t = |h, m, s| to_x(utc(2026, 11, 16, h, m, s)); // KST = UTC+9
        // A few minutes wide: ticks are seconds apart, so seconds are shown.
        let narrow = (t(5, 0, 0), t(5, 5, 0));
        assert_eq!(label_for_mark(&kst(), t(5, 2, 30), narrow), "14:02:30");
        // Ten minutes wide: ticks are whole minutes, so no `:00` noise.
        let ten = (t(5, 0, 0), t(5, 10, 0));
        assert_eq!(label_for_mark(&kst(), t(5, 4, 0), ten), "14:04");
        // A few hours wide: minutes, no seconds.
        let hours = (t(1, 0, 0), t(9, 0, 0));
        assert_eq!(label_for_mark(&kst(), t(5, 15, 0), hours), "14:15");
        // Crossing local midnight: the midnight tick names the day.
        let across = (t(13, 0, 0), t(18, 0, 0));
        assert_eq!(label_for_mark(&kst(), t(15, 0, 0), across), "11-17");
        assert_eq!(label_for_mark(&kst(), t(16, 0, 0), across), "01:00");
        // Many days wide: just dates.
        let wide = (t(0, 0, 0), t(0, 0, 0) + 5.0 * 86_400.0);
        assert_eq!(label_for_mark(&kst(), t(15, 0, 0), wide), "11-17");
    }

    #[test]
    fn the_axis_title_names_the_zone_and_the_hover_shows_the_full_local_time() {
        let at = utc(2026, 11, 16, 5, 50, 51);
        assert_eq!(axis_title(&kst(), at), "local time (UTC+09:00)");
        assert_eq!(axis_title(&ist(), at), "local time (UTC+05:30)");
        assert_eq!(hover_label(&kst(), to_x(at)), "2026-11-16 14:50:51 +09:00");
        assert_eq!(hover_label(&kst(), f64::NAN), "");
    }

    /// A zone that observes daylight saving, built by hand so the test does
    /// not depend on the machine's tz database: UTC-5, or UTC-4 from the
    /// spring-forward instant.
    #[derive(Clone, Copy, Debug)]
    struct Dst;
    #[derive(Clone, Copy, Debug)]
    struct DstOffset(i32);
    impl std::fmt::Display for DstOffset {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl chrono::Offset for DstOffset {
        fn fix(&self) -> FixedOffset {
            FixedOffset::east_opt(self.0).unwrap()
        }
    }
    fn spring_forward() -> i64 {
        // 2026-03-08 07:00 UTC = 02:00 EST -> 03:00 EDT
        utc(2026, 3, 8, 7, 0, 0).timestamp()
    }
    impl TimeZone for Dst {
        type Offset = DstOffset;
        fn from_offset(_: &DstOffset) -> Self {
            Dst
        }
        fn offset_from_local_date(&self, _: &chrono::NaiveDate) -> chrono::LocalResult<DstOffset> {
            chrono::LocalResult::Single(DstOffset(-5 * 3600))
        }
        fn offset_from_local_datetime(
            &self,
            local: &NaiveDateTime,
        ) -> chrono::LocalResult<DstOffset> {
            let as_est = local.and_utc().timestamp() + 5 * 3600;
            let as_edt = local.and_utc().timestamp() + 4 * 3600;
            let est_valid = as_est < spring_forward();
            let edt_valid = as_edt >= spring_forward();
            match (est_valid, edt_valid) {
                (true, false) => chrono::LocalResult::Single(DstOffset(-5 * 3600)),
                (false, true) => chrono::LocalResult::Single(DstOffset(-4 * 3600)),
                (true, true) => {
                    chrono::LocalResult::Ambiguous(DstOffset(-5 * 3600), DstOffset(-4 * 3600))
                }
                (false, false) => chrono::LocalResult::None,
            }
        }
        fn offset_from_utc_date(&self, _: &chrono::NaiveDate) -> DstOffset {
            DstOffset(-5 * 3600)
        }
        fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> DstOffset {
            if utc.and_utc().timestamp() < spring_forward() {
                DstOffset(-5 * 3600)
            } else {
                DstOffset(-4 * 3600)
            }
        }
    }

    #[test]
    fn ticks_across_the_spring_forward_gap_stay_increasing_and_never_invent_a_time() {
        // 00:00 EST to 06:00 EDT on 2026-03-08: local 02:00-03:00 does not exist.
        let lo = to_x(utc(2026, 3, 8, 5, 0, 0));
        let hi = to_x(utc(2026, 3, 8, 10, 0, 0));
        let marks = grid_marks(&Dst, (lo, hi), 3000.0);
        assert!(marks.len() >= 4, "{marks:?}");
        assert!(
            values(&marks).windows(2).all(|w| w[0] < w[1]),
            "strictly increasing"
        );
        let labels: Vec<String> = marks
            .iter()
            .map(|m| tick_label(&Dst, m.value, TickStyle::Minutes, false))
            .collect();
        assert!(
            !labels.contains(&"02:00".to_string()),
            "02:00 does not exist that night: {labels:?}"
        );
        assert!(labels.contains(&"03:00".to_string()), "{labels:?}");
    }
}
