use super::{parse_row_vectorized, RowParser};

#[derive(Default)]
pub struct VectorizedRowParser {}

impl RowParser for VectorizedRowParser {
    fn parse_row(
        buf: &[u8],
        start: usize,
        callback: &mut impl FnMut(&[u8], &[u8]),
    ) -> Option<usize> {
        parse_row_vectorized(buf, start, callback)
    }
}
