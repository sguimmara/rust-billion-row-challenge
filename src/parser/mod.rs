pub mod chunked;
pub mod memory_mapped;

use std::path::Path;

pub use crate::parser::chunked::ChunkParser;
pub use crate::parser::memory_mapped::MemoryMappedParser;

pub trait CSVParser {
    fn new(path: &Path) -> Self;
    /// Applies the visitor callback on all rows in the file. This operation is zero-copy.
    fn visit_all_rows(&mut self, visitor: &mut impl FnMut(&[u8], &[u8]));
    fn visit_row_at(&mut self, temp_buf: &mut [u8], offset: usize, visitor: &mut impl FnMut(&[u8], &[u8]));
}

/// ASCII code for newline
const NEWLINE_CODE: u8 = 10;
/// ASCII code for semicolon
const SEMICOLON_CODE: u8 = 59;

fn parse_row(buf: &[u8], offset: usize, callback: &mut impl FnMut(&[u8], &[u8])) -> Option<usize> {
    let mut end_of_station = 0;
    let mut end_of_temperature = 0;
    let mut complete = false;

    for i in offset..buf.len() {
        match buf[i] {
            SEMICOLON_CODE => end_of_station = i,
            NEWLINE_CODE => {
                end_of_temperature = i;
                complete = true;
                break;
            }
            _ => {}
        }
    }

    if complete {
        callback(
            &buf[offset..end_of_station],
            &buf[(end_of_station + 1)..end_of_temperature],
        );

        Some(end_of_temperature - offset + 1)
    } else {
        None
    }
}

#[cfg(test)]
pub mod test {
    use std::path::Path;

    use crate::{parser::parse_row, processor::Processor};

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
                rows.push(Row::new(
                    &String::from_utf8_lossy(station),
                    fast_float::parse(temperature).unwrap(),
                ));
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

    pub fn run_test_1_row<P: Processor>() {
        let mut processor = P::new(Path::new("./data/1-row.csv"));

        let results = processor.process();

        assert_eq!(results.len(), 1);

        assert_eq!(results[0].max, 1f32);
        assert_eq!(results[0].mean, 1f32);
        assert_eq!(results[0].min, 1f32);
        assert_eq!(results[0].name, "foo");
    }

    pub fn run_test_3_rows<P: Processor>() {
        let mut processor = P::new(Path::new("./data/3-rows.csv"));

        let results = processor.process();

        assert_eq!(results.len(), 3);

        let paris = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Paris")
            .unwrap();
        assert_eq!(paris.name, "Paris");
        assert_eq!(paris.max, 10.2f32);
        assert_eq!(paris.mean, 10.2f32);
        assert_eq!(paris.min, 10.2f32);

        let london = results
            .clone()
            .into_iter()
            .find(|x| x.name == "London")
            .unwrap();
        assert_eq!(london.name, "London");
        assert_eq!(london.max, 8.1f32);
        assert_eq!(london.mean, 8.1f32);
        assert_eq!(london.min, 8.1f32);

        let jakarta = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Jakarta")
            .unwrap();
        assert_eq!(jakarta.name, "Jakarta");
        assert_eq!(jakarta.max, 80.3f32);
        assert_eq!(jakarta.mean, 80.3f32);
        assert_eq!(jakarta.min, 80.3f32);
    }

    pub fn run_test_9_rows_duplicate_stations<P: Processor>() {
        let mut processor = P::new(Path::new("./data/9-rows-duplicate-stations.csv"));

        let results = processor.process();

        assert_eq!(results.len(), 3);

        let paris = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Paris")
            .unwrap();
        assert_eq!(paris.name, "Paris");
        assert_eq!(paris.min, 8.1f32);
        assert_eq!(paris.max, 80.3f32);
        assert_eq!(paris.mean, 32.9f32);

        let london = results
            .clone()
            .into_iter()
            .find(|x| x.name == "London")
            .unwrap();
        assert_eq!(london.name, "London");
        assert_eq!(london.min, -9.2f32);
        assert_eq!(london.max, 55.3f32);
        assert_eq!(london.mean, 24.4f32);

        let jakarta = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Jakarta")
            .unwrap();
        assert_eq!(jakarta.name, "Jakarta");
        assert_eq!(jakarta.min, 2.2f32);
        assert_eq!(jakarta.max, 90.3f32);
        assert_eq!(jakarta.mean, 32.9f32);
    }
}
