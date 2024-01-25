use std::path::Path;

use super::RowView;

pub mod mmap;

/// Interface for reading CSV files.
pub trait Reader {
    fn new(path: &Path) -> Self;
    /// Reads the next row in the file.
    fn read_row(&mut self) -> Option<RowView>;
    /// Reads a range and returns a [RowView] if this range matches a valid row.
    fn read_range(&self, start: usize, length: usize) -> Option<RowView>;
}
