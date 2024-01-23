mod chunked;
mod memory_mapped;

use std::path::Path;

pub use crate::reader::chunked::ChunkReader;
pub use crate::reader::memory_mapped::MemoryMappedReader;

pub trait CsvReader {
    fn new(path: &Path) -> Self;
    /// Applies the visitor callback on all rows in the file. This operation is zero-copy.
    fn visit_all_rows(&mut self, visitor: &mut impl FnMut(&[u8], &[u8]));
}
