use std::{error::Error, fmt::Display, fs::File, path::Path};

use memmap::{Mmap, MmapOptions};

pub struct MmapIterator {
    pos: usize,
    file: File,
    mmap: Mmap,
}

#[derive(Debug)]
pub struct Row {
    station: String,
    temperature: f64,
}

impl Row {
    pub fn new(station: String, temperature: f64) -> Self {
        Self {
            station,
            temperature,
        }
    }
}

impl MmapIterator {
    pub fn new(file: File) -> Self {
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        Self { file, mmap, pos: 0 }
    }
}

const NEWLINE: u8 = 10;
const SEMICOLON: u8 = 59;

fn parse_station(buf: &[u8], offset: &mut usize) -> Option<String> {
    let start = *offset;
    loop {
        if buf.len() <= *offset {
            return None;
        }
        let c = buf[*offset];
        if c == SEMICOLON {
            let station = String::from_utf8_lossy(&buf[start..*offset]).into_owned();

            *offset += 1;
            return Some(station);
        }
        *offset += 1;
    }
}

fn parse_row(buf: &[u8], offset: &mut usize) -> Option<Row> {
    if let Some(station) = parse_station(&buf, offset) {
        if let Some(temperature) = parse_temp(&buf, offset) {
            return Some(Row::new(station, temperature));
        }
    }

    return None;
}

fn parse_temp(buf: &[u8], offset: &mut usize) -> Option<f64> {
    let start = *offset;
    loop {
        if buf.len() <= *offset {
            return None;
        }
        let c = buf[*offset];
        if c == NEWLINE {
            let temp: f64 = String::from_utf8_lossy(&buf[start..*offset])
                .parse()
                .unwrap();
            *offset += 1;
            return Some(temp);
        }
        *offset += 1;
    }
}

impl Iterator for MmapIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        return parse_row(&self.mmap, &mut self.pos)

        // loop {
        //     let c = *self.mmap.get(offset + k).unwrap();

        //     if c == SEMICOLON {
        //         let station = String::from_utf8_lossy(&self.mmap[offset..(offset + k + 1)]).into_owned();
        //         end_of_first_col = offset + k + 1;
        //     } else if c == NEWLINE {
        //         let temp: f64 =
        //             String::from_utf8_lossy(&self.mmap[end_of_first_col..(offset + k + 1)])
        //                 .parse()
        //                 .unwrap();
        //     }

        //     k += 1;
        // }

        // loop {
        //     let c = *self.mmap.get(offset + k).unwrap();
        //     if c == SEMICOLON {
        //         let station =
        //             String::from_utf8_lossy(&self.mmap[offset..(offset + k + 1)]).into_owned();
        //         loop {
        //             let c = *self.mmap.get(offset + k).unwrap();
        //             if c == NEWLINE {
        //                 let temp: f64 =
        //                     String::from_utf8_lossy(&self.mmap[offset..(offset + k + 1)])
        //                         .parse()
        //                         .unwrap();
        //                 self.pos = offset + k;
        //                 return Some(Row::new(station, temp));
        //             }
        //             k += 1;
        //         }
        //     }
        //     k += 1;
        // }
    }
}

#[derive(Debug)]
pub struct ParserError {
    message: String,
}

impl ParserError {
    pub fn new(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parse error: {}", self.message)?;

        Ok(())
    }
}

impl Error for ParserError {}

pub fn parse_csv(path: &Path) -> Result<MmapIterator, ParserError> {
    let fd = File::open(path).unwrap(); // TODO handle error

    Ok(MmapIterator::new(fd))
}

#[cfg(test)]
mod test {
    use std::{os::linux::raw::stat, path::Path};

    use crate::parser::{parse_temp, parse_row};

    use super::{parse_csv, parse_station};

    #[test]
    fn test_parse_station() {
        let buf = "foo;1";
        let mut offset = 0;
        let station = parse_station(buf.as_bytes(), &mut offset);

        assert!(station.is_some());
        assert_eq!(station.unwrap(), "foo");
        assert_eq!(offset, 4);
    }

    #[test]
    fn test_parse_temperature() {
        let buf = "10.2\n";
        let mut offset = 0;
        let temp = parse_temp(buf.as_bytes(), &mut offset);

        assert!(temp.is_some());
        assert_eq!(temp.unwrap(), 10.2f64);
        assert_eq!(offset, 5);
    }

    #[test]
    fn test_parse_row() {
        let buf = "St. Petersburg;10.2\n";
        let mut offset = 0;

        let res = parse_row(buf.as_bytes(), &mut offset);

        assert!(res.is_some());
        if let Some(row) = res {
            assert_eq!(row.station, "St. Petersburg");
            assert_eq!(row.temperature, 10.2f64);
        }
    }

    #[test]
    fn test_parse_row_multiple() {
        let buf = "St. Petersburg;10.2\nParis;44.2\n";
        let mut offset = 0;

        let res1 = parse_row(buf.as_bytes(), &mut offset);
        let res2 = parse_row(buf.as_bytes(), &mut offset);

        assert!(res1.is_some());
        if let Some(row) = res1 {
            assert_eq!(row.station, "St. Petersburg");
            assert_eq!(row.temperature, 10.2f64);
        }

        assert!(res2.is_some());
        if let Some(row) = res2 {
            assert_eq!(row.station, "Paris");
            assert_eq!(row.temperature, 44.2f64);
        }

        let last = parse_row(buf.as_bytes(), &mut offset);

        assert!(last.is_none());
    }

    #[test]
    fn iterator() {
        let mut iterator = parse_csv(Path::new("./data/simple.csv")).unwrap();

        let first_row = iterator.next();

        assert!(first_row.is_some());
        if let Some(row) = first_row {
            assert_eq!(row.temperature, 1f64);
            assert_eq!(row.station, "foo");
        }

        let second_row = iterator.next();

        assert!(second_row.is_none());
    }
}
