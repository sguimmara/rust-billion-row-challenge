use std::path::Path;

pub mod fd_source;
pub mod mmap_source;

pub trait Parser {
    fn parse(self, f: &mut impl FnMut(&[u8], f64));
    fn new(path: &Path) -> Self;
}

const NEWLINE: u8 = 10;
const SEMICOLON: u8 = 59;

fn parse_station(buf: &[u8], offset: usize) -> Option<(&[u8], usize)> {
    parse_cell(buf, offset, SEMICOLON).map(|(value, bytes_read)| (value, bytes_read))
}

fn parse_cell(buf: &[u8], offset: usize, limiter: u8) -> Option<(&[u8], usize)> {
    let start = offset;
    let mut bytes_read = 0;
    loop {
        if buf.len() <= start + bytes_read {
            return None;
        }
        let c = buf[start + bytes_read];
        if c == limiter {
            let slice = &buf[start..(start + bytes_read)];
            // let value = String::from_utf8_lossy(slice);
            bytes_read += 1;
            return Some((slice, bytes_read));
        }
        bytes_read += 1;
    }
}

fn parse_row(buf: &[u8], offset: usize, f: &mut impl FnMut(&[u8], f64)) -> Option<usize> {
    if let Some((station, bytes_read)) = parse_station(buf, offset) {
        if let Some((temperature, bytes_read_2)) = parse_temp(buf, offset + bytes_read) {
            f(station, temperature);
            return Some(bytes_read + bytes_read_2);
        }
    }

    None
}

fn parse_temp(buf: &[u8], offset: usize) -> Option<(f64, usize)> {
    parse_cell(buf, offset, NEWLINE)
        .map(|(value, bytes_read)| (fast_float::parse(value).unwrap(), bytes_read))
}

#[cfg(test)]
mod test {
    use crate::parser::{parse_row, parse_station, parse_temp};

    #[derive(Debug, Clone, Default)]
    pub struct Row {
        pub station: String,
        pub temperature: f64,
    }

    impl Row {
        pub fn new(station: &str, temperature: f64) -> Self {
            Self {
                station: station.to_owned(),
                temperature,
            }
        }
    }

    #[test]
    fn test_parse_station() {
        let buf = "foo;1";
        let result = parse_station(buf.as_bytes(), 0);

        assert!(result.is_some());
        if let Some((station, offset)) = result {
            assert_eq!(station, "foo".as_bytes());
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

        let res = parse_row(buf.as_bytes(), 0, &mut |station, temperature| {
            assert_eq!(temperature, 10.2f64);
            assert_eq!(station, "St. Petersburg".as_bytes());
        });

        assert!(res.is_some());
    }

    #[test]
    fn test_parse_row_multiple() {
        let buf = "St. Petersburg;10.2\nParis;44.2\n";

        let mut rows: Vec<Row> = Vec::with_capacity(2);
        let mut offset = 0;

        loop {
            let res = parse_row(buf.as_bytes(), offset, &mut |station, temperature| {
                rows.push(Row::new(&String::from_utf8_lossy(station), temperature));
            });

            if res.is_none() {
                break;
            }
            offset += res.unwrap();
        }

        assert_eq!(rows[0].station, "St. Petersburg");
        assert_eq!(rows[0].temperature, 10.2f64);

        assert_eq!(rows[1].station, "Paris");
        assert_eq!(rows[1].temperature, 44.2f64);
    }
}
