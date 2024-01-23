use std::{fs::File, marker::PhantomData, path::Path};

use memmap::{Mmap, MmapOptions};

use crate::parser::RowParser;

use super::{BulkCsvReader, RowBuffer, SequentialCsvReader};

/// Parses the CSV file using a memory mapped file.
pub struct MemoryMappedReader<R>
where
    R: RowParser,
{
    offset: usize,
    mmap: Mmap,
    marker: PhantomData<R>,
}

impl<R: RowParser> MemoryMappedReader<R> {
    pub fn new(path: &Path) -> Self {
        let file = File::open(path).unwrap();
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        Self {
            mmap,
            offset: 0,
            marker: PhantomData,
        }
    }
}

const BULK_SIZE: usize = 64 * 1024 * 1024; // 64 MB

impl<R: RowParser> Iterator for MemoryMappedReader<R> {
    type Item = RowBuffer;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.offset;
        let mut byte_count = 0;
        let mut row_count = 0;

        loop {
            match R::seek_row(&self.mmap, self.offset) {
                Some(count) => {
                    self.offset += count;
                    byte_count += count;
                    row_count += 1;
                    if byte_count >= BULK_SIZE {
                        break;
                    }
                }
                None => return None,
            }
        }

        let buffer = RowBuffer::new(row_count, &self.mmap[start..start + byte_count]);

        Some(buffer)
    }
}

impl<R: RowParser> BulkCsvReader for MemoryMappedReader<R> {
    fn new(path: &Path) -> Self {
        Self::new(path)
    }
}

impl<R: RowParser> SequentialCsvReader for MemoryMappedReader<R> {
    fn visit_all_rows(&mut self, f: &mut impl FnMut(&[u8], &[u8])) {
        loop {
            match R::parse_row(&self.mmap, self.offset, f) {
                Some(count) => {
                    self.offset += count;
                }
                None => break,
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
        reader::{MemoryMappedReader, SequentialCsvReader},
    };

    #[test]
    fn parse_1_row() {
        let mut parser = MemoryMappedReader::<NaiveRowParser>::new(Path::new("./data/1-row.csv"));

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
        let mut parser = MemoryMappedReader::<NaiveRowParser>::new(Path::new("./data/3-rows.csv"));

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
