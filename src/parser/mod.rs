use std::path::Path;

pub mod fd_source;
pub mod mmap_source;

pub trait Parser {
    fn parse(self, f: &mut impl FnMut(&[u8], &[u8]));
    fn new(path: &Path) -> Self;
}

const NEWLINE: u8 = 10;
const SEMICOLON: u8 = 59;

fn parse_row(buf: &[u8], offset: usize, f: &mut impl FnMut(&[u8], &[u8])) -> Option<usize> {
    let mut end_of_station = 0;
    let mut end_of_temperature = 0;
    let mut complete = false;

    for i in offset..buf.len() {
        match buf[i] {
            SEMICOLON => end_of_station = i,
            NEWLINE => {
                end_of_temperature = i;
                complete = true;
                break;
            },
            _ => {}
        }
    }

    if complete {
        f(&buf[offset..end_of_station], &buf[(end_of_station + 1)..end_of_temperature]);

        Some(end_of_temperature - offset + 1)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use crate::parser::parse_row;

    #[derive(Debug, Clone, Default)]
    pub struct Row {
        pub station: String,
        pub temperature: f32,
    }

    impl Row {
        pub fn new(station: &str, temperature: f32) -> Self {
            Self {
                station: station.to_owned(),
                temperature,
            }
        }
    }

    #[test]
    fn test_parse_row() {
        let buf = "St. Petersburg;10.2\n";

        let res = parse_row(buf.as_bytes(), 0, &mut |station, t| {
            assert_eq!(station, "St. Petersburg".as_bytes());
            let temp: f32 = fast_float::parse(t).unwrap();
            assert_eq!(temp, 10.2f32);
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
                rows.push(Row::new(&String::from_utf8_lossy(station), fast_float::parse(temperature).unwrap()));
            });

            if res.is_none() {
                break;
            }
            offset += res.unwrap();
        }

        assert_eq!(rows[0].station, "St. Petersburg");
        assert_eq!(rows[0].temperature, 10.2f32);

        assert_eq!(rows[1].station, "Paris");
        assert_eq!(rows[1].temperature, 44.2f32);
    }
}
