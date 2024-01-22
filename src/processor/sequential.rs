use std::path::Path;

use nohash::IntMap;

use crate::parser::CSVParser;

use super::{hash_station_name, Station};

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
pub struct SequentialProcessor<P: CSVParser> {
    parser: P,
}

impl<P: CSVParser> SequentialProcessor<P> {
    pub fn new(path: &Path) -> Self {
        Self { parser: P::new(path) }
    }

    pub fn collect(self) -> Vec<Station> {
        let mut map: IntMap<u64, Entry> = IntMap::default();

        self.parser.parse(&mut |station, t| {
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
    use std::path::Path;

    use crate::parser::ChunkParser;

    use super::SequentialProcessor;

    #[test]
    fn test_1_row() {
        let analyzer: SequentialProcessor<ChunkParser> = SequentialProcessor::<ChunkParser>::new(Path::new("./data/1-row.csv"));

        let results = analyzer.collect();

        assert_eq!(results.len(), 1);

        assert_eq!(results[0].max, 1f32);
        assert_eq!(results[0].mean, 1f32);
        assert_eq!(results[0].min, 1f32);
        assert_eq!(results[0].name, "foo");
    }

    #[test]
    fn test_3_rows() {
        let analyzer: SequentialProcessor<ChunkParser> = SequentialProcessor::<ChunkParser>::new(Path::new("./data/3-rows.csv"));

        let results = analyzer.collect();

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

    #[test]
    fn test_9_rows_duplicate_stations() {
        let analyzer: SequentialProcessor<ChunkParser> =
            SequentialProcessor::<ChunkParser>::new(Path::new("./data/9-rows-duplicate-stations.csv"));

        let results = analyzer.collect();

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
