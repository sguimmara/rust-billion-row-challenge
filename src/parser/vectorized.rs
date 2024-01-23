use super::{parse_row_vectorized, seek_row_vectorized, RowParser};

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

    fn seek_row(buf: &[u8], start: usize) -> Option<usize> {
        seek_row_vectorized(buf, start)
    }

    fn parse_row_buffer(buf: &crate::reader::RowBuffer, callback: &mut impl FnMut(&[u8], &[u8])) {
        let mut start = 0;

        loop {
            let count = parse_row_vectorized(&buf.buffer, start, callback).unwrap();
            start += count;

            if start >= buf.buffer.len() {
                return;
            }
        }
    }
}
