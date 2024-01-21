use std::{fs::File, path::Path};

use memmap::{Mmap, MmapOptions};

use super::{Parser, parse_row};

pub struct MmapIterator {
    pos: usize,
    mmap: Mmap,
}

impl MmapIterator {
    pub fn new(path: &Path) -> Self {
        let file = File::open(path).unwrap();
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        Self { mmap, pos: 0 }
    }
}

impl Parser for MmapIterator {
    fn parse(mut self, f: &mut impl FnMut(&[u8], f32)) {
        loop {
            match parse_row(&self.mmap, self.pos, f) {
                Some(count) => {
                    self.pos += count;
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

    use crate::parser::{mmap_source::MmapIterator, Parser, test::Row};

    #[test]
    fn test_mmap_iterator() {
        let parser = MmapIterator::new(Path::new("./data/1-row.csv"));

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
        let parser = MmapIterator::new(Path::new("./data/3-rows.csv"));

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
