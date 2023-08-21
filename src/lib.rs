#[cfg(test)]
mod test_utils;

mod engine;
mod format;
mod memtable;
mod sstable;
mod compactor;
pub mod storage;

use serde::{Deserialize, Serialize};

const SEGMENTS_NAME: &'static str = "sstable";
const WAL_NAME: &'static str = "write-ahead-log";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Stored {
    Tombstone,
    Value(Vec<u8>),
}
