use std::{
    fs::File,
    marker::PhantomData,
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};

use crate::parser::{NaiveRowParser, RowParser};

use super::SequentialCsvReader;

const CHUNK_SIZE: usize = 16384;

/// A parser that reads the CSV in fixed size chunks.
pub struct ChunkReader<R = NaiveRowParser>
where
    R: RowParser,
{
    offset: usize,
    fd: File,
    buf: [u8; CHUNK_SIZE],
    file_size: u64,
    buf_offset: usize,
    marker: PhantomData<R>,
}

impl<R: RowParser> ChunkReader<R> {
    pub fn new(path: &Path) -> Self {
        let fd = File::open(path).unwrap();
        let file_size = fd.metadata().unwrap().size();

        let mut res = Self {
            fd,
            file_size,
            offset: 0,
            buf_offset: 0,
            buf: [0; CHUNK_SIZE],
            marker: PhantomData,
        };

        res.fill_buffer();

        res
    }

    fn fill_buffer(&mut self) -> bool {
        self.fd.read_at(&mut self.buf, self.offset as u64).is_ok()
    }
}

impl<R: RowParser> SequentialCsvReader for ChunkReader<R> {
    fn visit_all_rows(&mut self, visitor: &mut impl FnMut(&[u8], &[u8])) {
        loop {
            if self.offset >= (self.file_size as usize) {
                break;
            }

            match R::parse_row(&self.buf, self.buf_offset, visitor) {
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

    use crate::{
        parser::{test::Row, NaiveRowParser},
        reader::{ChunkReader, SequentialCsvReader},
    };

    #[test]
    fn parse_1_row() {
        let mut parser = ChunkReader::<NaiveRowParser>::new(Path::new("./data/1-row.csv"));

        let mut vec: Vec<Row> = Vec::with_capacity(1);

        parser.visit_all_rows(&mut |name, temp| {
            vec.push(Row::new(
                &String::from_utf8_lossy(name),
                fast_float::parse(temp).unwrap(),
            ))
        });

        assert_eq!(vec.len(), 1);

        assert_eq!(vec[0].temperature, 1f32);
        assert_eq!(vec[0].station, "foo");
    }

    #[test]
    fn parse_3_rows() {
        let mut parser = ChunkReader::<NaiveRowParser>::new(Path::new("./data/3-rows.csv"));

        let mut rows: Vec<Row> = Vec::with_capacity(1);

        parser.visit_all_rows(&mut |name, temp| {
            rows.push(Row::new(
                &String::from_utf8_lossy(name),
                fast_float::parse(temp).unwrap(),
            ))
        });

        assert_eq!(3, rows.len());

        assert_eq!(rows[0].station, "Paris");
        assert_eq!(rows[0].temperature, 10.2f32);

        assert_eq!(rows[1].station, "London");
        assert_eq!(rows[1].temperature, 8.1f32);

        assert_eq!(rows[2].station, "Jakarta");
        assert_eq!(rows[2].temperature, 80.3);
    }
}
