use std::{fs::File, path::Path};

use memmap::{Mmap, MmapOptions};

use super::{parse_row, Row};

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

impl Iterator for MmapIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let mut row = Row::default();
        match parse_row(&self.mmap, self.pos, &mut row) {
            Some(count) => {
                self.pos += count;
                Some(row)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::parser::{mmap_source::MmapIterator, Row};

    #[test]
    fn test_mmap_iterator() {
        let mut iterator = MmapIterator::new(Path::new("./data/1-row.csv"));

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
    fn test_mmap_3_rows() {
        let iterator = MmapIterator::new(Path::new("./data/3-rows.csv"));

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
