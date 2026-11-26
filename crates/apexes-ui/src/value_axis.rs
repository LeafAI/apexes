//! The y-axis of the equity chart.
//!
//! `egui_plot`'s default steps by powers of ten, so equity wandering between
//! 73,930 and 74,765 got ONE label (`74000`) and none to read values from.
//! Here the step is chosen from the visible range so the axis always carries
//! about five to ten labelled lines on a round 1-2-5 step, and the labels
//! show only the digits the step needs.

use egui_plot::GridMark;

/// Labels wanted on the axis: enough to read values off, few enough not to
/// crowd a chart a couple of hundred points tall.
const TARGET_LABELS: f64 = 6.0;

/// The 1-2-5 step (1, 2, 5, 10, 20, 50, ...) nearest `raw` on a log scale.
/// Nearest, not next-up: rounding up turned a range wanting 139 per line into
/// 200 and left four lines on a chart that needs six to eight.
pub fn nice_step(raw: f64) -> f64 {
    if !raw.is_finite() || raw <= 0.0 {
        return 1.0;
    }
    let magnitude = 10f64.powf(raw.log10().floor());
    [1.0, 2.0, 5.0, 10.0]
        .iter()
        .map(|nice| nice * magnitude)
        .min_by(|a, b| (raw / a).ln().abs().total_cmp(&(raw / b).ln().abs()))
        .unwrap_or(magnitude)
}

/// Grid marks across `bounds`: strong lines on every fifth step, the step
/// itself in between. `base_step` (the plot's own minimum spacing) is a floor,
/// so lines never crowd below what is legible.
pub fn grid_marks(bounds: (f64, f64), base_step: f64) -> Vec<GridMark> {
    let (lo, hi) = bounds;
    if !lo.is_finite() || !hi.is_finite() || hi <= lo {
        return Vec::new();
    }
    let step = nice_step(((hi - lo) / TARGET_LABELS).max(base_step));
    // An absurd zoom-out must not generate a flood of marks.
    if (hi - lo) / step > 500.0 {
        return Vec::new();
    }
    let strong = step * 5.0;
    let first = (lo / step).ceil() as i64;
    let last = (hi / step).floor() as i64;
    (first..=last)
        .map(|i| {
            let value = i as f64 * step;
            let on_strong = ((value / strong).round() * strong - value).abs() < step * 1e-6;
            GridMark {
                value,
                step_size: if on_strong { strong } else { step },
            }
        })
        .collect()
}

/// A label for `value` with only the places the grid needs: whole numbers
/// when the step is whole (`74,200`), one or two places for a finer step. The
/// range picks the step the same way [`grid_marks`] does.
pub fn label(value: f64, bounds: (f64, f64)) -> String {
    if !value.is_finite() {
        return String::new();
    }
    let step = nice_step(((bounds.1 - bounds.0) / TARGET_LABELS).max(f64::MIN_POSITIVE));
    let places = if step >= 1.0 {
        0
    } else {
        (-step.log10().floor()) as usize
    }
    .min(8);
    let text = format!("{value:.places$}");
    // `-0` reads as an error; it is just zero.
    if text
        .trim_start_matches('-')
        .chars()
        .all(|c| c == '0' || c == '.')
    {
        return format!("{:.places$}", 0.0);
    }
    group_thousands(&text)
}

/// `74200` -> `74,200`, `-1234.5` -> `-1,234.5`.
fn group_thousands(text: &str) -> String {
    let (sign, rest) = match text.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", text),
    };
    let (whole, fraction) = match rest.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (rest, None),
    };
    let mut grouped = String::new();
    for (i, digit) in whole.chars().enumerate() {
        if i > 0 && (whole.len() - i).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    match fraction {
        Some(fraction) => format!("{sign}{grouped}.{fraction}"),
        None => format!("{sign}{grouped}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(marks: &[GridMark]) -> Vec<f64> {
        marks.iter().map(|m| m.value).collect()
    }

    #[test]
    fn steps_are_one_two_five_times_a_power_of_ten() {
        for (raw, want) in [
            (1.0, 1.0),
            (1.3, 1.0),
            (1.5, 2.0),
            (2.0, 2.0),
            (3.0, 2.0),
            (3.5, 5.0),
            (5.0, 5.0),
            (7.0, 5.0),
            (8.0, 10.0),
            (139.0, 100.0),
            (0.013, 0.01),
            (0.0004, 0.0005),
        ] {
            let got = nice_step(raw);
            assert!(
                (got - want).abs() < want * 1e-9,
                "{raw} -> {got}, want {want}"
            );
        }
        assert_eq!(nice_step(0.0), 1.0);
        assert_eq!(nice_step(-3.0), 1.0);
        assert_eq!(nice_step(f64::NAN), 1.0);
    }

    /// The screenshot's range: this used to give a single `74000` line.
    #[test]
    fn the_screenshots_equity_range_gets_a_readable_set_of_lines() {
        let marks = grid_marks((73_930.0, 74_765.0), 1.0);
        let v = values(&marks);
        assert!((5..=10).contains(&v.len()), "{} lines: {v:?}", v.len());
        assert!(v.iter().all(|x| *x >= 73_930.0 && *x <= 74_765.0));
        assert!(v.windows(2).all(|w| w[0] < w[1]));
        let gaps: Vec<f64> = v.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(
            gaps.iter().all(|g| (g - gaps[0]).abs() < 1e-6),
            "even spacing"
        );
        assert_eq!(gaps[0], 100.0, "a round step for a 835-wide range");
    }

    #[test]
    fn a_tiny_range_gets_fine_steps_and_a_huge_one_coarse_steps() {
        let fine = values(&grid_marks((10_000.00, 10_000.30), 0.0));
        assert!(fine.len() >= 4, "{fine:?}");
        let coarse = values(&grid_marks((0.0, 5_000_000.0), 1.0));
        assert!((4..=12).contains(&coarse.len()), "{coarse:?}");
    }

    #[test]
    fn every_fifth_line_is_stronger() {
        // 0..300 -> step 50, strong every 250.
        let marks = grid_marks((0.0, 300.0), 0.0);
        let weak = marks.iter().map(|m| m.step_size).fold(f64::MAX, f64::min);
        let strong: Vec<f64> = marks
            .iter()
            .filter(|m| m.step_size > weak)
            .map(|m| m.value)
            .collect();
        assert_eq!(strong, vec![0.0, 250.0], "{marks:?}");
    }

    #[test]
    fn degenerate_and_absurd_ranges_yield_no_marks_and_no_panic() {
        assert!(grid_marks((5.0, 5.0), 1.0).is_empty());
        assert!(grid_marks((9.0, 1.0), 1.0).is_empty());
        assert!(grid_marks((f64::NAN, 1.0), 1.0).is_empty());
        assert!(grid_marks((0.0, f64::INFINITY), 1.0).is_empty());
        assert!(
            grid_marks((0.0, 1e12), 1e-9).len() < 500,
            "bounded even when the plot's floor is tiny"
        );
    }

    #[test]
    fn labels_show_only_the_places_the_step_needs_with_thousands_grouped() {
        let wide = (73_930.0, 74_765.0); // step 100
        assert_eq!(label(74_200.0, wide), "74,200");
        assert_eq!(label(74_100.0, wide), "74,100");
        let fine = (10_000.00, 10_000.30); // step 0.05
        assert_eq!(label(10_000.15, fine), "10,000.15");
        let mid = (100.0, 160.0); // step 10
        assert_eq!(label(120.0, mid), "120");
        let half = (100.0, 104.0); // step 0.5: one place, not a whole number
        assert_eq!(label(102.0, half), "102.0");
        assert_eq!(label(102.5, half), "102.5");
    }

    #[test]
    fn negative_and_zero_labels_are_clean() {
        assert_eq!(label(-1_234.0, (-2_000.0, 0.0)), "-1,234");
        // Range -2..2 has step 0.5, so zero is "0.0" - but never "-0.0".
        assert_eq!(label(-0.0, (-2.0, 2.0)), "0.0");
        assert_eq!(label(-0.0004, (-2.0, 2.0)), "0.0", "no `-0.0`");
        assert_eq!(label(0.0, (-2000.0, 2000.0)), "0");
        assert_eq!(label(f64::NAN, (0.0, 1.0)), "");
    }

    #[test]
    fn thousands_grouping_handles_every_length() {
        assert_eq!(group_thousands("7"), "7");
        assert_eq!(group_thousands("999"), "999");
        assert_eq!(group_thousands("1000"), "1,000");
        assert_eq!(group_thousands("1234567"), "1,234,567");
        assert_eq!(group_thousands("-1234567.25"), "-1,234,567.25");
    }
}
