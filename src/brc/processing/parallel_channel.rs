use std::{
    marker::PhantomData,
    path::Path,
    thread::{self, JoinHandle},
};

use crate::brc::{reader::Reader, Station};

use super::{Entry, Processor};
use crossbeam_channel::{self, Receiver};

pub struct ParallelChannel<R: Reader> {
    path: String,
    marker: PhantomData<R>,
}

impl<R: Reader> ParallelChannel<R> {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_str().unwrap().to_owned(),
            marker: PhantomData,
        }
    }
}

fn run_thread<R: Reader>(
    receiver: &Receiver<Vec<(usize, u16)>>,
    path: String,
) -> nohash::IntMap<u64, Entry> {
    let reader = R::new(Path::new(&path));
    let mut map: nohash::IntMap<u64, Entry> = nohash::IntMap::default();

    while let Ok(rows) = receiver.recv() {
        for (start, length) in rows {
            if let Some(view) = reader.read_range(start, length as usize) {
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
        }
    }

    map
}

fn feed(src: nohash::IntMap<u64, Entry>, dst: &mut nohash::IntMap<u64, Entry>) {
    for (k, v) in src {
        if let Some(entry) = dst.get_mut(&k) {
            entry.add(v);
        } else {
            dst.insert(k, v);
        }
    }
}

impl<R: Reader + Send> Processor for ParallelChannel<R> {
    fn process(&mut self) -> Vec<Station> {
        let (sender, receiver) = crossbeam_channel::unbounded();

        const THREAD_COUNT: usize = 32;
        let mut handles: Vec<JoinHandle<_>> = Vec::new();

        for _ in 0..THREAD_COUNT {
            let path = self.path.clone();
            let rcv = receiver.clone();
            let handle = thread::spawn(move || run_thread::<R>(&rcv, path));

            handles.push(handle);
        }

        let mut reader = R::new(Path::new(&self.path.clone()));

        let mut buf: Vec<(usize, u16)> = Vec::with_capacity(4096);

        while let Some(view) = reader.read_row() {
            buf.push((view.start, view.length as u16));
            if buf.len() == buf.capacity() {
                sender.send(buf.clone()).unwrap();
                buf.clear();
            }
        }

        if !buf.is_empty() {
            sender.send(buf).unwrap();
        }

        drop(sender);

        let mut map: nohash::IntMap<u64, Entry> = nohash::IntMap::default();

        for handle in handles {
            let thread_map = handle.join().unwrap();

            feed(thread_map, &mut map);
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
