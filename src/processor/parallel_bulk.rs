use std::{
    marker::PhantomData,
    sync::Arc,
    thread::{self, JoinHandle},
};

use dashmap::DashMap;

#[derive(Clone)]
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

use crate::{
    parser::RowParser,
    reader::{BulkCsvReader, RowBuffer},
};

use super::{hash_station_name, Processor, Station};

pub struct ParallelBulkProcessor<P: RowParser, R: BulkCsvReader> {
    reader: R,
    marker: PhantomData<P>,
}

fn process_block<P: RowParser>(block: RowBuffer, map: &DashMap<u64, Entry>) {
    P::parse_row_buffer(&block, &mut |name, temp| {
        let temperature = fast_float::parse(temp).unwrap();
        let k = hash_station_name(name);

        if let Some(mut entry) = map.get_mut(&k) {
            entry.max = f32::max(entry.max, temperature);
            entry.min = f32::min(entry.min, temperature);
            entry.sum += temperature;
            entry.count += 1;
        } else {
            map.insert(k, Entry::new(temperature, &String::from_utf8_lossy(name)));
        }
    });
}

impl<P: RowParser, R: BulkCsvReader> Processor for ParallelBulkProcessor<P, R> {
    fn new(path: &std::path::Path) -> Self {
        Self {
            reader: R::new(path),
            marker: PhantomData,
        }
    }

    fn process(&mut self) -> Vec<super::Station> {
        let map: Arc<DashMap<u64, Entry>> = Arc::new(DashMap::default());

        let mut handles: Vec<JoinHandle<_>> = Vec::new();

        for block in &mut self.reader {
            let cloned_map = map.clone();

            let handle = thread::spawn(move || process_block::<P>(block, &cloned_map));

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut result: Vec<Station> = Vec::with_capacity(map.len());

        for entry in map.iter() {
            let raw_mean = entry.sum / entry.count as f32;
            let mean = (raw_mean * 10f32).round() / 10f32;
            let station = &entry.name;
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
