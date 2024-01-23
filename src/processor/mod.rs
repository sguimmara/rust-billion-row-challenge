pub mod noop;
pub mod parallel_rayon;
pub mod sequential;

use std::path::Path;

pub use crate::processor::noop::NoopProcessor;
pub use crate::processor::parallel_rayon::ParallelRayonProcessor;
pub use crate::processor::sequential::SequentialProcessor;

fn hash_station_name(s: &[u8]) -> u64 {
    let mut result: u64 = 23;
    // h.write_usize(s.len());
    for i in 0..s.len() {
        result += 23 * (s[i] as u64);
    }

    result
}

#[derive(Clone)]
pub struct Station {
    pub name: String,
    pub min: f32,
    pub max: f32,
    pub mean: f32,
}

impl Station {
    pub fn new(name: String, min: f32, max: f32, mean: f32) -> Self {
        Self {
            name,
            min,
            max,
            mean,
        }
    }
}

pub trait Processor {
    fn new(path: &Path) -> Self;
    fn process(&mut self) -> Vec<Station>;
}
