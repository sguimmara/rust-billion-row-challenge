use std::{fs::File, os::unix::fs::FileExt, path::Path};

use super::{parse_row, Row};

const BUF_SIZE: usize = 256;

pub struct FdIterator {
    offset: usize,
    fd: File,
    buf: [u8; BUF_SIZE],
}

impl FdIterator {
    pub fn new(path: &Path) -> Self {
        let fd = File::open(path).unwrap();
        Self {
            fd,
            offset: 0,
            buf: [0; BUF_SIZE],
        }
    }

    fn fill_buffer(&mut self) -> bool {
        self.buf.fill(0);
        match self.fd.read_at(&mut self.buf, self.offset as u64) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

impl Iterator for FdIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.fill_buffer();
        let mut row_offset = 0;
        let result = parse_row(&self.buf, &mut row_offset);
        if result.is_some() {
            self.offset += row_offset;
        }
        result
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::parser::{fd_source::FdIterator, Row};

    #[test]
    fn test_1_row() {
        let mut iterator = FdIterator::new(Path::new("./data/1-row.csv"));

        let first_row = iterator.next();

        assert!(first_row.is_some());
        if let Some(row) = first_row {
            assert_eq!(row.temperature, 1f64);
            assert_eq!(row.station, "foo");
        }

        let second_row = iterator.next();

        assert!(second_row.is_none());
    }

    #[test]
    fn test_3_rows() {
        let iterator = FdIterator::new(Path::new("./data/3-rows.csv"));

        let rows: Vec<Row> = iterator.collect();

        assert_eq!(3, rows.len());
        assert_eq!(rows[0].station, "Paris");
        assert_eq!(rows[1].station, "London");
        assert_eq!(rows[2].station, "Jakarta");

        assert_eq!(rows[0].temperature, 10.2f64);
        assert_eq!(rows[1].temperature, 8.1f64);
        assert_eq!(rows[2].temperature, 80.3);
    }
}
