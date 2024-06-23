#[cfg(test)]
mod test_utils;

mod compactor;
mod format;
mod lsm_tree;
mod memtable;
mod sstable;
mod wal;

pub mod lsm_tree_service;

use serde::{Deserialize, Serialize};

const SEGMENTS_NAME: &str = "sstable";
const WAL_NAME: &str = "write-ahead-log";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Stored {
    Tombstone,
    Value(Vec<u8>),
}
