use std::path::Path;

use self::{mmap_source::MmapIterator, fd_source::FdIterator};

mod mmap_source;
mod fd_source;

#[derive(Debug)]
pub struct Row {
    pub station: String,
    pub temperature: f64,
}

impl Row {
    pub fn new(station: String, temperature: f64) -> Self {
        Self {
            station,
            temperature,
        }
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

pub enum ParseMethod {
    Mmap,
    Fd,
}

pub fn parse_csv(path: &Path, method: ParseMethod) -> Option<Box<dyn Iterator<Item = Row>>> {
    match method {
        ParseMethod::Mmap => Some(Box::new(MmapIterator::new(path))),
        ParseMethod::Fd => Some(Box::new(FdIterator::new(path)))
    }
}

#[cfg(test)]
mod test {
    use crate::parser::{parse_temp, parse_row, parse_station};

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
}
