use std::path::Path;

use self::{fd_source::FdIterator, mmap_source::MmapIterator};

mod fd_source;
mod mmap_source;

#[derive(Debug, Clone, Default)]
pub struct Row {
    pub station: String,
    pub temperature: f64,
}

const NEWLINE: u8 = 10;
const SEMICOLON: u8 = 59;

fn parse_station(buf: &[u8], offset: usize) -> Option<(String, usize)> {
    parse_cell(buf, offset, SEMICOLON).map(|(value, bytes_read)| (value.to_string(), bytes_read))
}

fn parse_cell(buf: &[u8], offset: usize, limiter: u8) -> Option<(String, usize)> {
    let start = offset;
    let mut bytes_read = 0;
    loop {
        if buf.len() <= start + bytes_read {
            return None;
        }
        let c = buf[start + bytes_read];
        if c == limiter {
            let value = String::from_utf8_lossy(&buf[start..(start + bytes_read)]);
            bytes_read += 1;
            return Some((value.to_string(), bytes_read));
        }
        bytes_read += 1;
    }
}

fn parse_row(buf: &[u8], offset: usize, dst: &mut Row) -> Option<usize> {
    if let Some((station, bytes_read)) = parse_station(buf, offset) {
        if let Some((temperature, bytes_read_2)) = parse_temp(buf, offset + bytes_read) {
            dst.station = station;
            dst.temperature = temperature;
            return Some(bytes_read + bytes_read_2);
        }
    }

    None
}

fn parse_temp(buf: &[u8], offset: usize) -> Option<(f64, usize)> {
    parse_cell(buf, offset, NEWLINE).map(|(value, bytes_read)| (value.parse().unwrap(), bytes_read))
}

pub enum ParseMethod {
    Mmap,
    Fd,
}

pub fn parse_csv(path: &Path, method: ParseMethod) -> Option<Box<dyn Iterator<Item = Row>>> {
    match method {
        ParseMethod::Mmap => Some(Box::new(MmapIterator::new(path))),
        ParseMethod::Fd => Some(Box::new(FdIterator::new(path))),
    }
}

#[cfg(test)]
mod test {
    use crate::parser::{parse_row, parse_station, parse_temp, Row};

    #[test]
    fn test_parse_station() {
        let buf = "foo;1";
        let result = parse_station(buf.as_bytes(), 0);

        assert!(result.is_some());
        if let Some((station, offset)) = result {
            assert_eq!(station, "foo");
            assert_eq!(offset, 4);
        }
    }

    #[test]
    fn test_parse_temperature() {
        let buf = "10.2\n";
        let temp = parse_temp(buf.as_bytes(), 0);

        assert!(temp.is_some());
        assert_eq!(temp.unwrap().0, 10.2f64);
        assert_eq!(temp.unwrap().1, 5);
    }

    #[test]
    fn test_parse_row() {
        let buf = "St. Petersburg;10.2\n";

        let mut row = Row::default();
        let res = parse_row(buf.as_bytes(), 0, &mut row);

        assert!(res.is_some());
        if let Some(count) = res {
            assert_eq!(row.station, "St. Petersburg");
            assert_eq!(row.temperature, 10.2f64);
            assert_eq!(count, 20);
        }
    }

    #[test]
    fn test_parse_row_multiple() {
        let buf = "St. Petersburg;10.2\nParis;44.2\n";

        let mut row = Row::default();
        let res1 = parse_row(buf.as_bytes(), 0, &mut row);

        assert!(res1.is_some());
        if let Some(count) = res1 {
            assert_eq!(row.station, "St. Petersburg");
            assert_eq!(row.temperature, 10.2f64);

            let res2 = parse_row(buf.as_bytes(), count, &mut row);

            assert!(res2.is_some());
            if let Some(count2) = res2 {
                assert_eq!(row.station, "Paris");
                assert_eq!(row.temperature, 44.2f64);

                let last = parse_row(buf.as_bytes(), count + count2, &mut row);

                assert!(last.is_none());
            }
        }
    }
}
