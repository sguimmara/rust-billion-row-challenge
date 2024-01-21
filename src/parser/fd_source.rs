use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};

use super::{parse_row, Parser};

const BUF_SIZE: usize = 16384;

pub struct FdIterator {
    offset: usize,
    fd: File,
    buf: [u8; BUF_SIZE],
    file_size: u64,
    buf_offset: usize,
}

impl FdIterator {
    pub fn new(path: &Path) -> Self {
        let fd = File::open(path).unwrap();
        let file_size = fd.metadata().unwrap().size();

        let mut res = Self {
            fd,
            file_size,
            offset: 0,
            buf_offset: 0,
            buf: [0; BUF_SIZE],
        };

        res.fill_buffer();

        res
    }

    fn fill_buffer(&mut self) -> bool {
        self.fd.read_at(&mut self.buf, self.offset as u64).is_ok()
    }
}

impl Parser for FdIterator {
    fn parse(mut self, f: &mut impl FnMut(&[u8], f32)) {
        loop {
            if self.offset >= (self.file_size as usize) {
                break;
            }

            match parse_row(&self.buf, self.buf_offset, f) {
                Some(count) => {
                    self.buf_offset += count;
                    self.offset += count;
                }
                None => {
                    self.fill_buffer();
                    self.buf_offset = 0;
                }
            }
        }
    }

    fn new(path: &Path) -> Self {
        Self::new(path)
    }
}


#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::parser::{Parser, test::Row, fd_source::FdIterator};

    #[test]
    fn test_mmap_iterator() {
        let parser = FdIterator::new(Path::new("./data/1-row.csv"));

        let mut vec: Vec<Row> = Vec::with_capacity(1);

        parser.parse(&mut |name, temp| {
            vec.push(Row::new(&String::from_utf8_lossy(name), temp))
        });

        assert_eq!(vec.len(), 1);

        assert_eq!(vec[0].temperature, 1f32);
        assert_eq!(vec[0].station, "foo");
    }

    #[test]
    fn test_mmap_3_rows() {
        let parser = FdIterator::new(Path::new("./data/3-rows.csv"));

        let mut rows: Vec<Row> = Vec::with_capacity(1);

        parser.parse(&mut |name, temp| {
            rows.push(Row::new(&String::from_utf8_lossy(name), temp))
        });

        assert_eq!(3, rows.len());
        assert_eq!(rows[0].station, "Paris");
        assert_eq!(rows[1].station, "London");
        assert_eq!(rows[2].station, "Jakarta");

        assert_eq!(rows[0].temperature, 10.2f32);
        assert_eq!(rows[1].temperature, 8.1f32);
        assert_eq!(rows[2].temperature, 80.3);
    }
}
