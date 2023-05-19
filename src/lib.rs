#[cfg(test)]
mod test_utils;

mod engine;
mod format;
mod memtable;
mod sstable;
mod compactor;
pub mod storage;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Stored {
    Tombstone,
    Value(Vec<u8>),
}
