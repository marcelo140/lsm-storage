use std::sync::Arc;

use crate::memtable::MemTable;
use crate::sstable::{SSTable, SSTableReader};

/// The storage engine. It holds the current memtable and the set of sstables
pub struct Engine {
    pub active_memtable: MemTable,
    pub memtables: Vec<Arc<MemTable>>,
    pub sstables0: Vec<SSTable>,
    pub sstables1: Vec<SSTable>,
    pub sstable_readers0: Vec<SSTableReader>,
    pub sstable_readers1: Vec<SSTableReader>,
}
