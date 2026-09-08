//! Classify file sizes into fixed buckets for ops gauges.

pub const BUCKET_LT_1MB: &str = "lt_1mb";
pub const BUCKET_1_8MB: &str = "1_8mb";
pub const BUCKET_8_64MB: &str = "8_64mb";
pub const BUCKET_GTE_64MB: &str = "gte_64mb";

pub fn size_bucket(bytes: u64) -> &'static str {
    const MB: u64 = 1024 * 1024;
    if bytes < MB {
        BUCKET_LT_1MB
    } else if bytes < 8 * MB {
        BUCKET_1_8MB
    } else if bytes < 64 * MB {
        BUCKET_8_64MB
    } else {
        BUCKET_GTE_64MB
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets() {
        assert_eq!(size_bucket(100), BUCKET_LT_1MB);
        assert_eq!(size_bucket(2 * 1024 * 1024), BUCKET_1_8MB);
        assert_eq!(size_bucket(16 * 1024 * 1024), BUCKET_8_64MB);
        assert_eq!(size_bucket(100 * 1024 * 1024), BUCKET_GTE_64MB);
    }
}
