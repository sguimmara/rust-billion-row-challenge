use crate::brc::{reader::Reader, Station};

use super::{Entry, Processor};

pub struct Sequential<R: Reader> {
    reader: R,
}

impl<R: Reader> Sequential<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: Reader> Processor for Sequential<R> {
    fn process(&mut self) -> Vec<Station> {
        let mut map: nohash::IntMap<u64, Entry> = nohash::IntMap::default();

        while let Some(view) = self.reader.read_row() {
            let k = view.key();
            let t = view.value();

            if let Some(entry) = map.get_mut(&k) {
                entry.min = f32::min(entry.min, t);
                entry.max = f32::max(entry.max, t);
                entry.sum += t;
                entry.count += 1;
            } else {
                map.insert(k, Entry::new(t, view.name()));
            }
        }

        let mut result: Vec<Station> = Vec::with_capacity(map.len());

        for (_, entry) in map.iter() {
            let raw_mean = entry.sum / entry.count as f32;
            let mean = (raw_mean * 10f32).round() / 10f32;
            let station = entry.name.to_string();
            result.push(Station::new(station, entry.min, entry.max, mean))
        }

        result.sort_unstable_by_key(|x| x.name.clone());

        result
    }
}
