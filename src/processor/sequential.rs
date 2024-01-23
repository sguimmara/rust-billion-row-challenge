use std::path::Path;

use nohash::IntMap;

use crate::{
    parser::naive::NaiveRowParser,
    reader::{ChunkReader, CsvReader},
};

use super::{hash_station_name, Processor, Station};

struct Entry {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
    name: String,
}

impl Entry {
    pub fn new(temperature: f32, s: &[u8]) -> Self {
        Self {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1,
            name: String::from_utf8_lossy(s).to_string(),
        }
    }
}

/// A single-threaded, sequential processor.
pub struct SequentialProcessor<P: CsvReader = ChunkReader<NaiveRowParser>> {
    parser: P,
}

impl<P: CsvReader> Processor for SequentialProcessor<P> {
    fn new(path: &Path) -> Self {
        Self {
            parser: P::new(path),
        }
    }

    fn process(&mut self) -> Vec<Station> {
        let mut map: IntMap<u64, Entry> = IntMap::default();

        self.parser.visit_all_rows(&mut |station, t| {
            let temperature = fast_float::parse(t).unwrap();

            let k = hash_station_name(station);

            if let Some(v) = map.get_mut(&k) {
                v.min = f32::min(v.min, temperature);
                v.max = f32::max(v.max, temperature);
                v.sum += temperature;
                v.count += 1;
            } else {
                map.insert(k, Entry::new(temperature, station));
            }
        });

        let mut result = Vec::with_capacity(map.len());

        for (_key, entry) in map {
            let raw_mean = entry.sum / entry.count as f32;
            let mean = (raw_mean * 10f32).round() / 10f32;
            let station = entry.name;
            result.push(Station::new(
                station.to_string(),
                entry.min,
                entry.max,
                mean,
            ))
        }

        result.sort_unstable_by_key(|x| x.name.clone());

        result
    }
}

#[cfg(test)]
mod test {

    use crate::{parser, reader::ChunkReader};

    use super::SequentialProcessor;

    #[test]
    fn test_1_row() {
        parser::test::run_test_1_row::<SequentialProcessor<ChunkReader>>();
    }

    #[test]
    fn test_3_rows() {
        parser::test::run_test_3_rows::<SequentialProcessor<ChunkReader>>();
    }

    #[test]
    fn test_9_rows_duplicate_stations() {
        parser::test::run_test_9_rows_duplicate_stations::<SequentialProcessor<ChunkReader>>();
    }
}
