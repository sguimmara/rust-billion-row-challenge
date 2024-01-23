use super::{parse_row_naive, RowParser};

#[derive(Default, Debug)]
pub struct NaiveRowParser {}

impl RowParser for NaiveRowParser {
    fn parse_row(
        buf: &[u8],
        start: usize,
        callback: &mut impl FnMut(&[u8], &[u8]),
    ) -> Option<usize> {
        parse_row_naive(buf, start, callback)
    }
}
