use std::path::Path;

use dashmap::DashMap;
use rayon::iter::{ParallelDrainRange, ParallelIterator};

use crate::{parser::CSVParser, processor::hash_station_name};

use super::{Processor, Station};

const BUFFER_SIZE: usize = 4096;

/// A parallel processor using Rayon
pub struct ParallelRayonProcessor<P: CSVParser> {
    parser: P,
}

struct Entry {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
    name: String,
}

impl Entry {
    pub fn new(temperature: f32, s: &str) -> Self {
        Self {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1,
            name: s.to_string(),
        }
    }
}

struct WorkBuffer {
    queue: Vec<(String, String)>,
}

impl Default for WorkBuffer {

    fn default() -> Self {
        Self { queue: Vec::with_capacity(BUFFER_SIZE) }
    }
}

impl WorkBuffer {
    fn is_full(&self) -> bool {
        self.queue.len() == self.queue.capacity()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn push(&mut self, name: String, temperature: String) {
        self.queue.push((name, temperature));
    }

    fn run(&mut self, map: &DashMap<u64, Entry>) {
       self.queue
        .par_drain(0..self.queue.len())
        .for_each(|(n, t)| {
            let k = hash_station_name(n.as_bytes());
            let temperature: f32 = fast_float::parse(t).unwrap();

            if let Some(mut v) = map.get_mut(&k) {
                v.min = f32::min(v.min, temperature);
                v.max = f32::max(v.max, temperature);
                v.sum += temperature;
                v.count += 1;
            } else {
                map.insert(k, Entry::new(temperature, &n));
            }
        });
    }
}

impl<P: CSVParser> Processor for ParallelRayonProcessor<P> {
    fn process(self) -> Vec<super::Station> {
        let mut work_buffer = WorkBuffer::default();
        let map: DashMap<u64, Entry> = DashMap::with_capacity(10000);

        self.parser.parse(&mut |name_row, temp_row| {
            let name = String::from_utf8_lossy(name_row).to_string();
            let temp = String::from_utf8_lossy(temp_row).to_string();

            work_buffer.push(name, temp);

            if work_buffer.is_full() {
                work_buffer.run(&map);
            }
        });

        if work_buffer.len() > 0 {
            work_buffer.run(&map);
        }

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

    fn new(path: &Path) -> Self {
        Self {
            parser: P::new(path),
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::{parser::ChunkParser, processor::Processor};

    use super::ParallelRayonProcessor;

    #[test]
    fn test_1_row() {
        let processor = ParallelRayonProcessor::<ChunkParser>::new(Path::new("./data/1-row.csv"));

        let results = processor.process();

        assert_eq!(results.len(), 1);

        assert_eq!(results[0].max, 1f32);
        assert_eq!(results[0].mean, 1f32);
        assert_eq!(results[0].min, 1f32);
        assert_eq!(results[0].name, "foo");
    }

    #[test]
    fn test_3_rows() {
        let analyzer = ParallelRayonProcessor::<ChunkParser>::new(Path::new("./data/3-rows.csv"));

        let results = analyzer.process();

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
        let analyzer = ParallelRayonProcessor::<ChunkParser>::new(Path::new(
            "./data/9-rows-duplicate-stations.csv",
        ));

        let results = analyzer.process();

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
