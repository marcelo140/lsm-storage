use crate::memtable::MemTable;
use crate::sstable::SSTable;

/// The storage engine. It holds the current memtable and the set of sstables
pub struct Engine {
    pub seq_logs: usize,
    pub memtable: MemTable,
    pub sstables: Vec<SSTable>,
}
