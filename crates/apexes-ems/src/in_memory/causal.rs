//! Validation and time scaling for causal simulated EMS scheduling.
use crate::{AlgoOrderRequest, EmsError};
use apexes_storage::reader::VolumeCurve;
use apexes_strategy::{ExecutionAlgoSpec, OrderRouting};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;

fn refuse(reason: &str) -> EmsError {
    EmsError::ParentAdmissionRejected {
        reason: reason.into(),
    }
}

pub(super) fn validate_causal_request(
    request: &AlgoOrderRequest,
    at: DateTime<Utc>,
) -> Result<(), EmsError> {
    if request.quantity <= Decimal::ZERO || request.routing != OrderRouting::Fixed {
        return Err(refuse(
            "causal EMS requires positive quantity and fixed routing",
        ));
    }
    let duration = match request.execution_algo {
        ExecutionAlgoSpec::Twap { duration, slices } => {
            if slices == 0 || slices > i32::MAX as u32 {
                return Err(refuse("invalid TWAP slice count"));
            }
            let d = TimeDelta::from_std(duration).map_err(|_| refuse("invalid TWAP duration"))?;
            if d / slices as i32 <= TimeDelta::zero() {
                return Err(refuse("TWAP slice interval must be positive"));
            }
            Some(d)
        }
        ExecutionAlgoSpec::Vwap {
            duration,
            lookback_days,
        } => {
            if lookback_days == 0 {
                return Err(refuse("VWAP lookback must be positive"));
            }
            Some(TimeDelta::from_std(duration).map_err(|_| refuse("invalid VWAP duration"))?)
        }
        ExecutionAlgoSpec::Pov { participation_rate } => {
            if participation_rate <= Decimal::ZERO || participation_rate > Decimal::ONE {
                return Err(refuse("POV rate must be in (0,1]"));
            }
            None
        }
        ExecutionAlgoSpec::Iceberg { visible_qty } => {
            if visible_qty <= Decimal::ZERO {
                return Err(refuse("visible quantity must be positive"));
            }
            None
        }
    };
    if duration
        .is_some_and(|d| d < TimeDelta::milliseconds(1) || at.checked_add_signed(d).is_none())
    {
        return Err(refuse("invalid causal schedule duration"));
    }
    Ok(())
}

pub(super) fn scale_causal_curve(
    mut curve: VolumeCurve,
    spec: &ExecutionAlgoSpec,
) -> Result<VolumeCurve, EmsError> {
    let ExecutionAlgoSpec::Vwap { duration, .. } = spec else {
        return Err(refuse("not VWAP"));
    };
    let duration = TimeDelta::from_std(*duration)
        .map_err(|_| refuse("invalid VWAP duration"))?
        .num_milliseconds();
    let mut previous = None;
    for &(offset, fraction) in &curve.cumulative_fractions {
        if offset < TimeDelta::zero()
            || fraction < Decimal::ZERO
            || fraction > Decimal::ONE
            || previous.is_some_and(|(t, f)| offset <= t || fraction < f)
        {
            return Err(refuse("invalid VWAP curve"));
        }
        previous = Some((offset, fraction));
    }
    let (last, fraction) = previous.ok_or_else(|| refuse("empty VWAP curve"))?;
    let last = last.num_milliseconds();
    if last <= 0 || fraction != Decimal::ONE {
        return Err(refuse("incomplete VWAP curve"));
    }
    for (offset, _) in &mut curve.cumulative_fractions {
        let scaled =
            i128::from(offset.num_milliseconds()) * i128::from(duration) / i128::from(last);
        *offset = TimeDelta::try_milliseconds(
            i64::try_from(scaled).map_err(|_| refuse("VWAP offset overflow"))?,
        )
        .ok_or_else(|| refuse("VWAP offset overflow"))?;
    }
    Ok(curve)
}
