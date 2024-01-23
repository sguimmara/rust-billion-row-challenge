use crate::reader::SequentialCsvReader;

use super::Processor;

/// A processor that does nothing
pub struct NoopProcessor<P: SequentialCsvReader> {
    parser: P,
}

impl<P: SequentialCsvReader> Processor for NoopProcessor<P> {
    fn new(path: &std::path::Path) -> Self {
        Self {
            parser: P::new(path),
        }
    }

    fn process(&mut self) -> Vec<super::Station> {
        self.parser.visit_all_rows(&mut |_, _| {});

        Vec::with_capacity(0)
    }
}
