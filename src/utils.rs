use eyre::Result;
use std::i64;

const SCALE: i64 = 10i64.pow(8);
const OVERFLOW_THRESHOLD: f64 = (i64::MAX as f64) / (SCALE as f64);

pub fn f64_to_i64(value: f64) -> Result<i64> {
    if value > OVERFLOW_THRESHOLD {
        return Err(eyre::eyre!(
            "Value overflow while trying to convert to internal format"
        ));
    }
    Ok((value * SCALE as f64).round() as i64)
}

pub fn i64_to_f64(internal_value: i64) -> f64 {
    (internal_value as f64) / (SCALE as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f64_to_i64() {
        let values = [
            0.12345678,
            0.0,
            1.0,
            0.1,
            0.00000001,
            12345.6789,
            -0.12345678,
            -1.0,
        ];
        let expected = [
            12345678,
            0,
            100_000_000,
            10_000_000,
            1,
            1_234_567_890_000,
            -12345678,
            -100_000_000,
        ];

        let actual: Vec<Result<i64>> = values.iter().map(|x| f64_to_i64(*x)).collect();

        for (act, exp) in actual.into_iter().zip(expected.iter()) {
            assert_eq!(act.unwrap(), *exp);
        }
    }

    #[test]
    fn test_i64_to_f64() {
        let values = [
            12345678,
            0,
            100_000_000,
            10_000_000,
            1,
            1_234_567_890_000,
            -12345678,
            -100_000_000,
        ];
        let expected = [
            0.12345678,
            0.0,
            1.0,
            0.1,
            0.00000001,
            12345.6789,
            -0.12345678,
            -1.0,
        ];
        let actual: Vec<f64> = values.iter().map(|x| i64_to_f64(*x)).collect();
        for (act, exp) in actual.iter().zip(expected.iter()) {
            assert_eq!(act, exp)
        }
    }

    #[test]
    fn test_f64_to_i64_overflow() {
        let value = OVERFLOW_THRESHOLD + 1.0;
        let result = f64_to_i64(value);
        assert!(result.is_err());
    }
}
