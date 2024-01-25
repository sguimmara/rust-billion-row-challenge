use std::path::Path;

use super::RowView;

pub mod mmap;

pub trait Reader {
    fn new(path: &Path) -> Self;
    fn read_row(&mut self) -> Option<RowView>;
    fn read_view(&self, start: usize, length: usize) -> Option<RowView>;
}
