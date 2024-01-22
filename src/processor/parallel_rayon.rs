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
    fn process(&mut self) -> Vec<super::Station> {
        let mut work_buffer = WorkBuffer::default();
        let map: DashMap<u64, Entry> = DashMap::with_capacity(10000);

        self.parser.visit_all_rows(&mut |name_row, temp_row| {
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

    use crate::parser::ChunkParser;
    use crate::parser;

    use super::ParallelRayonProcessor;

    #[test]
    fn test_1_row() {
        parser::test::run_test_1_row::<ParallelRayonProcessor::<ChunkParser>>();
    }

    #[test]
    fn test_3_rows() {
        parser::test::run_test_3_rows::<ParallelRayonProcessor::<ChunkParser>>();
    }

    #[test]
    fn test_9_rows_duplicate_stations() {
        parser::test::run_test_9_rows_duplicate_stations::<ParallelRayonProcessor::<ChunkParser>>();
    }
}
