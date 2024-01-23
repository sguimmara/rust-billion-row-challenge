pub mod naive;
pub mod vectorized;

pub use crate::parser::naive::NaiveRowParser;
pub use crate::parser::vectorized::VectorizedRowParser;

pub trait RowParser: Default {
    fn parse_row(
        buf: &[u8],
        start: usize,
        callback: &mut impl FnMut(&[u8], &[u8]),
    ) -> Option<usize>;
}

/// ASCII code for newline
const NEWLINE_CODE: u8 = 10;
/// ASCII code for semicolon
const SEMICOLON_CODE: u8 = 59;

fn parse_row_naive(
    buf: &[u8],
    start: usize,
    callback: &mut impl FnMut(&[u8], &[u8]),
) -> Option<usize> {
    let mut end_of_station = 0;
    let mut end_of_temperature = 0;
    let mut complete = false;

    for i in start..buf.len() {
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
            &buf[start..end_of_station],
            &buf[(end_of_station + 1)..end_of_temperature],
        );

        Some(end_of_temperature - start + 1)
    } else {
        None
    }
}

fn parse_row_vectorized(
    buf: &[u8],
    start: usize,
    callback: &mut impl FnMut(&[u8], &[u8]),
) -> Option<usize> {
    let len = buf.len();

    if let Some(semi_index) = memchr::memchr(SEMICOLON_CODE, &buf[start..len]) {
        if let Some(newline_index) = memchr::memchr(NEWLINE_CODE, &buf[start..len]) {
            let end_of_station = start + semi_index;
            let end_of_temperature = start + newline_index;

            callback(
                &buf[start..end_of_station],
                &buf[(end_of_station + 1)..(end_of_temperature)],
            );

            return Some(end_of_temperature - start + 1);
        }
    }

    None
}

#[cfg(test)]
pub mod test {
    use std::path::Path;

    use crate::{
        parser::{parse_row_naive, parse_row_vectorized},
        processor::Processor,
    };

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
    fn test_parse_row_naive_1_row() {
        let buf = "St. Petersburg;10.2\n";

        let res = parse_row_naive(buf.as_bytes(), 0, &mut |station, t| {
            assert_eq!(station, "St. Petersburg".as_bytes());
            let temp: f32 = fast_float::parse(t).unwrap();
            assert_eq!(temp, 10.2f32);
        });

        assert!(res.is_some());
    }

    #[test]
    fn test_parse_row_vectorized_1_row() {
        let buf = "St. Petersburg;10.2\n";

        let res = parse_row_vectorized(buf.as_bytes(), 0, &mut |station, t| {
            assert_eq!(String::from_utf8_lossy(station), "St. Petersburg");
            let temp: f32 = fast_float::parse(t).unwrap();
            assert_eq!(temp, 10.2f32);
        });

        assert!(res.is_some());
    }

    #[test]
    fn test_parse_row_vectorized_multiple() {
        let buf = "St. Petersburg;10.2\nParis;44.2\n";

        let mut rows: Vec<Row> = Vec::with_capacity(2);
        let mut offset = 0;

        loop {
            let res = parse_row_vectorized(buf.as_bytes(), offset, &mut |station, temperature| {
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

    #[test]
    fn test_parse_row_naive_multiple() {
        let buf = "St. Petersburg;10.2\nParis;44.2\n";

        let mut rows: Vec<Row> = Vec::with_capacity(2);
        let mut offset = 0;

        loop {
            let res = parse_row_naive(buf.as_bytes(), offset, &mut |station, temperature| {
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
