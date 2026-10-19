//! Exact fixed-point numbers for prices, share sizes and USDC amounts.
//!
//! Polymarket sends prices and sizes as decimal strings ("0.68", "452.04"). Parsing them into
//! `f64` makes book levels compare unreliably, so everything that is keyed or summed uses
//! [`Fixed6`]: an `i64` count of millionths (USDC itself has 6 decimals).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

pub const SCALE: i64 = 1_000_000;
const DECIMALS: usize = 6;

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fixed6(pub i64);

pub type Price = Fixed6;
pub type Size = Fixed6;

#[derive(Debug, thiserror::Error)]
#[error("invalid decimal {0:?}")]
pub struct ParseFixedError(pub String);

impl Fixed6 {
    pub const ZERO: Fixed6 = Fixed6(0);
    pub const ONE: Fixed6 = Fixed6(SCALE);

    pub const fn from_raw(micros: i64) -> Self {
        Fixed6(micros)
    }

    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Rounds to the nearest millionth.
    pub fn from_f64(x: f64) -> Self {
        Fixed6((x * SCALE as f64).round() as i64)
    }

    pub fn to_f64(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// `self * other`, both fixed-point (e.g. price × size = notional), rounded half away from zero.
    pub fn mul_fixed(self, other: Fixed6) -> Fixed6 {
        let p = self.0 as i128 * other.0 as i128;
        let half = SCALE as i128 / 2;
        let q = if p >= 0 {
            (p + half) / SCALE as i128
        } else {
            (p - half) / SCALE as i128
        };
        Fixed6(q as i64)
    }

    /// `1 - self`: the complementary outcome price.
    pub fn complement(self) -> Fixed6 {
        Fixed6(SCALE - self.0)
    }

    /// Largest multiple of `step` at or below `self` (e.g. a price on the tick grid). A
    /// non-positive `step` returns `self`.
    pub fn floor_to(self, step: Fixed6) -> Fixed6 {
        if step.0 <= 0 {
            return self;
        }
        Fixed6(self.0.div_euclid(step.0) * step.0)
    }

    /// Smallest multiple of `step` at or above `self`.
    pub fn ceil_to(self, step: Fixed6) -> Fixed6 {
        if step.0 <= 0 {
            return self;
        }
        Fixed6(-((-self.0).div_euclid(step.0) * step.0))
    }

    /// `self / other` for two fixed-point numbers (e.g. USDC / price = shares), rounded down.
    pub fn div_fixed(self, other: Fixed6) -> Option<Fixed6> {
        (other.0 != 0).then(|| Fixed6((self.0 as i128 * SCALE as i128 / other.0 as i128) as i64))
    }
}

impl std::ops::Add for Fixed6 {
    type Output = Fixed6;
    fn add(self, o: Fixed6) -> Fixed6 {
        Fixed6(self.0 + o.0)
    }
}

impl std::ops::Sub for Fixed6 {
    type Output = Fixed6;
    fn sub(self, o: Fixed6) -> Fixed6 {
        Fixed6(self.0 - o.0)
    }
}

impl std::ops::AddAssign for Fixed6 {
    fn add_assign(&mut self, o: Fixed6) {
        self.0 += o.0
    }
}

impl std::ops::SubAssign for Fixed6 {
    fn sub_assign(&mut self, o: Fixed6) {
        self.0 -= o.0
    }
}

impl FromStr for Fixed6 {
    type Err = ParseFixedError;

    /// Plain decimals are parsed exactly (digits past the 6th are rounded half up);
    /// anything else (exponents) falls back to `f64`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseFixedError(s.to_string());
        let t = s.trim();
        let (neg, body) = match t.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, t.strip_prefix('+').unwrap_or(t)),
        };
        if body.is_empty() {
            return Err(err());
        }
        if body.bytes().any(|b| b == b'e' || b == b'E') {
            return t.parse::<f64>().map(Fixed6::from_f64).map_err(|_| err());
        }
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        if !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return Err(err());
        }
        let mut v: i64 = if int.is_empty() {
            0
        } else {
            int.parse().map_err(|_| err())?
        };
        v = v.checked_mul(SCALE).ok_or_else(err)?;
        let fb = frac.as_bytes();
        let mut f: i64 = 0;
        for i in 0..DECIMALS {
            f = f * 10 + fb.get(i).map_or(0, |b| (b - b'0') as i64);
        }
        if fb.get(DECIMALS).is_some_and(|&b| b >= b'5') {
            f += 1;
        }
        v += f;
        Ok(Fixed6(if neg { -v } else { v }))
    }
}

impl fmt::Display for Fixed6 {
    /// Shortest exact decimal: 680000 -> "0.68".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let a = self.0.unsigned_abs();
        let (i, fr) = (a / SCALE as u64, a % SCALE as u64);
        if fr == 0 {
            return write!(f, "{sign}{i}");
        }
        let s = format!("{fr:06}");
        write!(f, "{sign}{i}.{}", s.trim_end_matches('0'))
    }
}

impl fmt::Debug for Fixed6 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for Fixed6 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Fixed6 {
    /// Accepts both `"0.68"` and `0.68` (RTDS sends floats, the CLOB sends strings).
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl de::Visitor<'_> for V {
            type Value = Fixed6;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a decimal string or number")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Fixed6, E> {
                v.parse().map_err(E::custom)
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Fixed6, E> {
                Ok(Fixed6::from_f64(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Fixed6, E> {
                v.checked_mul(SCALE).map(Fixed6).ok_or_else(|| E::custom("overflow"))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Fixed6, E> {
                i64::try_from(v).map_err(E::custom).and_then(|v| self.visit_i64(v))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_print() {
        for (s, raw, out) in [
            ("0.68", 680_000, "0.68"),
            ("452.04", 452_040_000, "452.04"),
            ("1", 1_000_000, "1"),
            (".5", 500_000, "0.5"),
            ("0.8599999007130845", 860_000, "0.86"),
            ("-0.001", -1_000, "-0.001"),
            ("1e-3", 1_000, "0.001"),
        ] {
            let v: Fixed6 = s.parse().unwrap();
            assert_eq!(v.raw(), raw, "{s}");
            assert_eq!(v.to_string(), out);
        }
        assert!("abc".parse::<Fixed6>().is_err());
        assert!("".parse::<Fixed6>().is_err());
    }

    #[test]
    fn serde_both_forms() {
        let a: Fixed6 = serde_json::from_str("\"0.15\"").unwrap();
        let b: Fixed6 = serde_json::from_str("0.15").unwrap();
        assert_eq!(a, b);
        assert_eq!(serde_json::to_string(&a).unwrap(), "\"0.15\"");
    }

    #[test]
    fn arithmetic() {
        let p: Fixed6 = "0.68".parse().unwrap();
        let s: Fixed6 = "55.03".parse().unwrap();
        assert_eq!(p.mul_fixed(s).to_string(), "37.4204");
        assert_eq!(p.complement().to_string(), "0.32");
        let tick: Fixed6 = "0.01".parse().unwrap();
        let x: Fixed6 = "0.845".parse().unwrap();
        assert_eq!(x.floor_to(tick).to_string(), "0.84");
        assert_eq!(x.ceil_to(tick).to_string(), "0.85");
        assert_eq!(p.floor_to(tick), p, "already on the grid");
        assert_eq!(
            Fixed6::from_raw(5_000_000).div_fixed(p).unwrap().to_string(),
            "7.352941"
        );
        assert!(p.div_fixed(Fixed6::ZERO).is_none());
    }
}
