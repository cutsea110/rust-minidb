#[derive(Debug, Clone)]
pub enum SearchMode {
    Start,
    Key(Vec<u8>),
}
