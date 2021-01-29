/*!
 * Backports of useful unstable Rust features.
 */

/*
 * feature(option_expect_none)
 */
pub trait ExpectNone {
    fn expect_none(self, message: &'static str);
}

impl<T> ExpectNone for Option<T> {
    fn expect_none(self, message: &'static str) {
        assert!(self.is_none(), message);
    }
}

#[cfg(test)]
mod test {
    use super::ExpectNone;

    #[test]
    fn test_some() {
        let x: Option<()> = None;
        x.expect_none("hello");
    }

    #[test]
    #[should_panic(expected = "boom")]
    fn test_none() {
        let x = Some(());
        x.expect_none("boom");
    }
}
