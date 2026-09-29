use chrono::{DateTime, Utc};

pub(crate) fn to_ns(value: DateTime<Utc>) -> i64 {
    value
        .timestamp_nanos_opt()
        .expect("session timestamp must fit signed nanoseconds")
}

pub(crate) fn from_ns(value: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(
        value.div_euclid(1_000_000_000),
        value.rem_euclid(1_000_000_000) as u32,
    )
    .expect("session timestamp nanoseconds must be a valid UTC instant")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_submicrosecond_instants() {
        let instant = DateTime::from_timestamp(1_700_000_000, 123).unwrap();
        assert_eq!(from_ns(to_ns(instant)), instant);
    }
}
