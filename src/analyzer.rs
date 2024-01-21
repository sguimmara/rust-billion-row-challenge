use std::{collections::HashMap, path::Path};

use crate::parser::Parser;

#[derive(Clone)]
pub struct Station {
    pub name: String,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
}

impl Station {
    pub fn new(name: String, min: f64, max: f64, mean: f64) -> Self {
        Self {
            name,
            min,
            max,
            mean,
        }
    }
}

struct Entry {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
}

impl Entry {
    pub fn new(temperature: f64) -> Self {
        Self {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1,
        }
    }
}

pub struct Analyzer<P: Parser> {
    parser: P,
}

impl<P: Parser> Analyzer<P> {
    pub fn new(path: &Path) -> Self {
        let parser = P::new(path);
        Self {
            parser,
        }
    }

    pub fn collect(self) -> Vec<Station> {
        let mut map: HashMap<Vec<u8>, Entry> = HashMap::with_capacity(4096);

        self.parser.parse(&mut |key, temperature| {
            if let Some(v) = map.get_mut(key) {
                v.min = f64::min(v.min, temperature);
                v.max = f64::max(v.max, temperature);
                v.sum += temperature;
                v.count += 1;
            } else {
                map.insert(key.to_vec(), Entry::new(temperature));
            }
        });

        let mut result = Vec::with_capacity(map.len());

        for (key, entry) in map {
            let raw_mean = entry.sum / entry.count as f64;
            let mean = (raw_mean * 10f64).round() / 10f64;
            let station = String::from_utf8(key).unwrap();
            result.push(Station::new(station.to_string(), entry.min, entry.max, mean))
        }

        result.sort_unstable_by_key(|x| x.name.clone());

        result
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::parser::mmap_source::MmapIterator;

    use super::Analyzer;

    #[test]
    fn test_1_row() {
        let analyzer: Analyzer<MmapIterator> = Analyzer::new(Path::new("./data/1-row.csv"));

        let results = analyzer.collect();

        assert_eq!(results.len(), 1);

        assert_eq!(results[0].max, 1f64);
        assert_eq!(results[0].mean, 1f64);
        assert_eq!(results[0].min, 1f64);
        assert_eq!(results[0].name, "foo");
    }

    #[test]
    fn test_3_rows() {
        let analyzer: Analyzer<MmapIterator> = Analyzer::new(Path::new("./data/3-rows.csv"));

        let results = analyzer.collect();

        assert_eq!(results.len(), 3);

        let paris = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Paris")
            .unwrap();
        assert_eq!(paris.name, "Paris");
        assert_eq!(paris.max, 10.2f64);
        assert_eq!(paris.mean, 10.2f64);
        assert_eq!(paris.min, 10.2f64);

        let london = results
            .clone()
            .into_iter()
            .find(|x| x.name == "London")
            .unwrap();
        assert_eq!(london.name, "London");
        assert_eq!(london.max, 8.1f64);
        assert_eq!(london.mean, 8.1f64);
        assert_eq!(london.min, 8.1f64);

        let jakarta = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Jakarta")
            .unwrap();
        assert_eq!(jakarta.name, "Jakarta");
        assert_eq!(jakarta.max, 80.3f64);
        assert_eq!(jakarta.mean, 80.3f64);
        assert_eq!(jakarta.min, 80.3f64);
    }

    #[test]
    fn test_9_rows_duplicate_stations() {
        let analyzer: Analyzer<MmapIterator> = Analyzer::new(
            Path::new("./data/9-rows-duplicate-stations.csv"),
        );

        let results = analyzer.collect();

        assert_eq!(results.len(), 3);

        let paris = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Paris")
            .unwrap();
        assert_eq!(paris.name, "Paris");
        assert_eq!(paris.min, 8.1f64);
        assert_eq!(paris.max, 80.3f64);
        assert_eq!(paris.mean, 32.9f64);

        let london = results
            .clone()
            .into_iter()
            .find(|x| x.name == "London")
            .unwrap();
        assert_eq!(london.name, "London");
        assert_eq!(london.min, -9.2f64);
        assert_eq!(london.max, 55.3f64);
        assert_eq!(london.mean, 24.4f64);

        let jakarta = results
            .clone()
            .into_iter()
            .find(|x| x.name == "Jakarta")
            .unwrap();
        assert_eq!(jakarta.name, "Jakarta");
        assert_eq!(jakarta.min, 2.2f64);
        assert_eq!(jakarta.max, 90.3f64);
        assert_eq!(jakarta.mean, 32.9f64);
    }
}
