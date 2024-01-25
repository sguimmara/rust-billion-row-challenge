/// ASCII code for newline
const NEWLINE_CODE: u8 = 10;
/// ASCII code for semicolon
const SEMICOLON_CODE: u8 = 59;

pub mod processing;
pub mod reader;

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

fn hash_station_name(s: &[u8]) -> u64 {
    let mut result: u64 = 23;
    for item in s {
        if *item == SEMICOLON_CODE {
            return result;
        }
        result += 23 * (*item as u64);
    }

    result
}

pub struct RowView<'a> {
    pub start: usize,
    pub length: usize,
    view: &'a [u8],
}

impl<'a> RowView<'a> {
    pub fn key(&self) -> u64 {
        hash_station_name(self.view)
    }

    pub fn name(&self) -> String {
        let delimiter = memchr::memchr(SEMICOLON_CODE, self.view).unwrap();
        String::from_utf8_lossy(&self.view[0..delimiter]).to_string()
    }

    pub fn value(&self) -> f32 {
        let delimiter = memchr::memchr(SEMICOLON_CODE, self.view).unwrap();
        let value: f32 = fast_float::parse(&self.view[delimiter + 1..]).unwrap();

        value
    }

    pub fn new(view: &'a [u8], start: usize, length: usize) -> Self {
        Self {
            view,
            start,
            length,
        }
    }
}

#[cfg(test)]
mod test {
    use super::RowView;

    #[test]
    fn row_view_into_row() {
        let buf = b"Hello world;2.4";
        let row = RowView::new(buf, 0, buf.len());

        assert_eq!(row.name(), "Hello world");
        assert_eq!(row.value(), 2.4f32);
    }
}
