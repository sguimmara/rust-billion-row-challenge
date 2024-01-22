use std::{fs::File, path::Path};

use memmap::{Mmap, MmapOptions};

use super::{parse_row, CSVParser};

/// Parses the CSV file using a memory mapped file.
pub struct MemoryMappedParser {
    offset: usize,
    mmap: Mmap,
}

impl MemoryMappedParser {
    pub fn new(path: &Path) -> Self {
        let file = File::open(path).unwrap();
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        Self { mmap, offset: 0 }
    }
}

impl CSVParser for MemoryMappedParser {
    fn visit_all_rows(&mut self, f: &mut impl FnMut(&[u8], &[u8])) {
        loop {
            match parse_row(&self.mmap, self.offset, f) {
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

    use crate::parser::{memory_mapped::MemoryMappedParser, test::Row, CSVParser};

    #[test]
    fn parse_1_row() {
        let mut parser = MemoryMappedParser::new(Path::new("./data/1-row.csv"));

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
        let mut parser = MemoryMappedParser::new(Path::new("./data/3-rows.csv"));

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
