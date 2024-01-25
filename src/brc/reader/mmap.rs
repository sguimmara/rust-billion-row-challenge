use std::fs::File;

use memmap::{Mmap, MmapOptions};

use crate::brc::{RowView, NEWLINE_CODE};

use super::Reader;

pub struct MmapReader {
    offset: usize,
    mmap: Mmap,
}

impl Reader for MmapReader {
    fn new(path: &std::path::Path) -> Self {
        let file = File::open(path).unwrap();
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        Self { mmap, offset: 0 }
    }

    fn read_row(&mut self) -> Option<crate::brc::RowView> {
        match memchr::memchr(NEWLINE_CODE, &self.mmap[self.offset..]) {
            Some(count) => {
                let start = self.offset;
                let size = count;
                let result = RowView::new(&self.mmap[start..start + size], start, size);
                self.offset += count + 1;
                Some(result)
            }
            None => None,
        }
    }

    fn read_range(&self, start: usize, length: usize) -> Option<RowView> {
        Some(RowView::new(
            &self.mmap[start..start + length],
            start,
            length,
        ))
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::brc::reader::Reader;

    use super::MmapReader;

    #[test]
    fn test_1_row() {
        let mut reader = MmapReader::new(Path::new("./data/1-row.csv"));

        let view = reader.read_row();

        assert!(view.is_some());
        let row = view.unwrap();

        assert_eq!("foo", row.name());
        assert_eq!(1f32, row.value());
    }

    #[test]
    fn test_3_rows() {
        let mut reader = MmapReader::new(Path::new("./data/3-rows.csv"));

        let row1 = reader.read_row().unwrap();

        assert_eq!(row1.name(), "Paris");
        assert_eq!(row1.value(), 10.2f32);

        let row2 = reader.read_row().unwrap();
        assert_eq!(row2.name(), "London");
        assert_eq!(row2.value(), 8.1f32);

        let row3 = reader.read_row().unwrap();
        assert_eq!(row3.name(), "Jakarta");
        assert_eq!(row3.value(), 80.3f32);
    }
}
