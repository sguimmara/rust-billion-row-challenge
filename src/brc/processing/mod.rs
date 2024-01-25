use super::Station;

pub mod parallel_channel;
pub mod sequential;

pub trait Processor {
    fn process(&mut self) -> Vec<Station>;
}

struct Entry {
    pub min: f32,
    pub max: f32,
    pub sum: f32,
    pub count: u32,
    pub name: String,
}

impl Entry {
    pub fn new(t: f32, name: String) -> Self {
        Self {
            min: t,
            max: t,
            sum: t,
            count: 1,
            name,
        }
    }

    fn add(&mut self, v: Entry) {
        self.min = f32::min(self.min, v.min);
        self.max = f32::max(self.max, v.max);
        self.sum += v.sum;
        self.count += v.count;
    }
}
