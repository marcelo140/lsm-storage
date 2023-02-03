use anyhow::Result;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::Stored;
use crate::sstable::SSTable;

/// An in-memory data-structure that keeps entries ordered by key.
///
/// It is hard to keep a mutable on-disk data structure ordered without losing performance. To
/// overcome this, new entries are inserted into an in-memory table. Once the size of the table
/// exceeds a certain threshold, it is persisted as a SSTable.
///
/// In order to recover from a crash without losing the in-memory data, every insertion should be
/// inserted into a Write-Ahead Log. As such, insertions and removes can fail if they are unable
/// to persist to disk.
///
/// In case of remove operations, the original key-pair may already be persisted in a persisted
/// SSTable and thus cannot be simply removed. This is why we insert a Tombstone in remove 
/// operations.
///
/// TODO: discard WAL after persisting
pub struct MemTable {
    tree: BTreeMap<String, Stored>,
    wal: File,
}

impl MemTable {
    /// Creates an empty MemTable.
    pub fn new(path: &Path) -> Self {
        let mut path = path.to_path_buf();
        path.push("write-ahead-log");

        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        MemTable {
            tree: BTreeMap::new(),
            wal,
        }
    }

    pub fn recover(path: &Path) -> Result<Self> {
        let mut path = path.to_path_buf();
        path.push("write-ahead-log");

        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        let mut tree = BTreeMap::new();
        let mut bytes_read = 0;

        while let Ok((key, value)) = bincode::deserialize_from(&wal) {
            bytes_read += bincode::serialized_size(&(&key, &value)).unwrap();
            tree.insert(key, value);
        }

        wal.set_len(bytes_read)?;

        Ok(MemTable {
            tree,
            wal,
        })
    }

    /// Inserts a new entry into the MemTable, persisting it in the WAL for recovery purposes.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        bincode::serialize_into(&mut self.wal, &(&key, Stored::Value(value.clone())))?;
        self.tree.insert(key, Stored::Value(value));

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        bincode::serialize_into(&mut self.wal, &(&key, Stored::Tombstone))?;
        self.tree.insert(key, Stored::Tombstone);

        Ok(())
    }

    /// The number of entries in the MemTable.
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Returns the value corresponding to the given key, if present.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self.tree.get(key).cloned() {
            Some(Stored::Value(v)) => Some(v),
            _ => None,
        }
    }

    /// Persists the MemTable to disk storing its entries in-order.
    ///
    /// Returns the corresponding SSTable.
    pub fn persist(self, path: &Path) -> Result<SSTable> {
        let mut fd = File::create(path)?;

        let kvs: Vec<(String, Stored)> = self.tree.into_iter().collect();
        for (key, value) in kvs {
            bincode::serialize_into(&mut fd, &(&key, value))?;
        }

        SSTable::new(path.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::memtable::MemTable;
    use crate::test_utils::*;

    #[test]
    fn memtable_gets_and_inserts_are_successful() {
        let (uuid, _path, mut memtable) = setup_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned()).unwrap();
        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned()).unwrap();
        memtable.insert("key3".to_string(), "value1".as_bytes().to_owned()).unwrap();

        assert_eq!(memtable.get("key1"), Some("value1".as_bytes().to_owned()));
        assert_eq!(memtable.get("key4"), None);

        clean(&uuid);
    }

    #[test]
    fn memtables_deletes_are_successful() {
        let (uuid, _path, mut memtable) = setup_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned()).unwrap();
        memtable.remove("key2".to_string()).unwrap();

        assert_eq!(memtable.get("key2"), None);

        clean(&uuid);
    }

    #[test]
    fn recovering_through_wal_yields_the_same_tree() {
        let (uuid, path, mut memtable) = setup_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned()).unwrap();
        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned()).unwrap();
        memtable.insert("key3".to_string(), "value1".as_bytes().to_owned()).unwrap();

        let recovered = MemTable::recover(&path).unwrap();

        assert_eq!(memtable.tree, recovered.tree);

        clean(&uuid);
    }

    #[test]
    fn it_recovers_from_corrupted_wal() {
        let (uuid, path, mut memtable) = setup_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned()).unwrap();
        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned()).unwrap();
        memtable.insert("key3".to_string(), "value1".as_bytes().to_owned()).unwrap();
        memtable.remove("key1".to_string()).unwrap();

        let mut wal_path = path.to_path_buf();
        wal_path.push("write-ahead-log");
        let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();

        bincode::serialize_into(&mut wal, &"5").unwrap();

        let recovered = MemTable::recover(&path).unwrap();
        assert_eq!(memtable.tree, recovered.tree);

        clean(&uuid);
    }

    #[test]
    fn corrupted_log_is_truncated() {
        let (uuid, path, mut memtable) = setup_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned()).unwrap();
        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned()).unwrap();
        memtable.insert("key3".to_string(), "value1".as_bytes().to_owned()).unwrap();

        let mut wal_path = path.to_path_buf();
        wal_path.push("write-ahead-log");

        let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();
        let wal_metadata = wal.metadata().unwrap();
        let wal_length = wal_metadata.len();

        bincode::serialize_into(&mut wal, &"5").unwrap();

        let wal_metadata = wal.metadata().unwrap();
        let corrupted_wal_length = wal_metadata.len();

        assert!(corrupted_wal_length > wal_length);

        let _recovered = MemTable::recover(&path).unwrap();
        let wal_metadata = wal.metadata().unwrap();
        let recovered_wal_length = wal_metadata.len();

        assert_eq!(wal_length, recovered_wal_length);

        clean(&uuid);
    }
}
