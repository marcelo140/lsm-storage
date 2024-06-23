use std::collections::HashMap;

use anyhow::Result;

use crate::memtable::MemTable;
use crate::sstable::SSTable;

/// The storage engine. It holds the current memtable and the set of sstables
pub struct LsmTree {
    memtables: Vec<(MemTable, MemtableStatus)>,
    sstables: Vec<(SSTable, SSTableStatus)>,
}

enum MemtableStatus {
    Active,
    Persisting,
    Persisted,
    Deleted,
}

enum SSTableStatus {
    Creating,
    Active,
    Merging,
    Merged,
    Deleted,
}

struct LsmTreeStatus {
    memtable_status: HashMap<String, MemtableStatus>,
    sstable_status: HashMap<String, SSTableStatus>,
}

impl LsmTree {
    /// Inserts a value into the memtable. If the memtable size reaches its threshold, converts it
    /// into a sstable.
    ///
    /// TODO:
    /// - the memtable is swapped with an empty one before it is persisted. concurrent readers will
    /// see the storage in a past state state.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        self.get_active_memtable().insert(key, value)?;

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.get_active_memtable().remove(key)?;

        Ok(())
    }

    /// Performs a read by trying to find the value in the memtable and falling back to the
    /// sstables if not successful.
    pub fn lookup(&self, key: &str) -> Option<Vec<u8>> {
        self.lookup_memtables(key)
            .or_else(|| self.lookup_sstables(key))
    }

    fn lookup_memtables(&self, key: &str) -> Option<Vec<u8>> {
        self.get_readable_memtables()
            .iter()
            .find_map(|memtable| memtable.lookup(key))
            .map(|v| v.to_vec())
    }

    fn lookup_sstables(&self, key: &str) -> Option<Vec<u8>> {
        for table in self.get_readable_sstables().iter() {
            let v = table.lookup(key).unwrap();

            if v.is_some() {
                return v;
            }
        }

        None
    }

    fn get_active_memtable(&mut self) -> &mut MemTable {
        &mut self
            .memtables
            .iter_mut()
            .find(|(_memtable, status)| matches!(status, MemtableStatus::Active))
            .expect("no active memtable")
            .0
    }

    fn get_readable_memtables(&self) -> Vec<&MemTable> {
        self.memtables
            .iter()
            .filter(|(_memtable, status)| {
                matches!(status, MemtableStatus::Active)
                    || matches!(status, MemtableStatus::Persisting)
            })
            .map(|(memtable, _status)| memtable)
            .collect()
    }

    fn get_readable_sstables(&self) -> Vec<&SSTable> {
        self.sstables
            .iter()
            .filter(|(_sstable, status)| {
                matches!(status, SSTableStatus::Active) 
                    || matches!(status, SSTableStatus::Merging)
            })
            .map(|(sstable, _status)| sstable)
            .collect()
    }
}
