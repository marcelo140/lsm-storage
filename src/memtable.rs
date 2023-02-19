use anyhow::Result;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::Stored;

static WAL_PATH: &str = "write-ahead-log";

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
pub struct MemTable {
    pub(crate) tree: BTreeMap<String, Stored>,
    wal: File,
}

impl MemTable {
    /// Creates an empty MemTable.
    pub fn new(path: &Path) -> Result<Self> {
        let wal = MemTable::create_wal(path)?;

        Ok(MemTable {
            tree: BTreeMap::new(),
            wal,
        })
    }

    /// Creates a MemTable from a write-ahead-log
    pub fn recover(path: &Path) -> Result<Self> {
        let wal = MemTable::open_wal(path)?;

        let mut tree = BTreeMap::new();
        let mut bytes_read = 0;

        while let Ok(deserialized_value) = bincode::deserialize_from(&wal) {
            bytes_read += bincode::serialized_size(&deserialized_value)?;
            let (key, value) = deserialized_value;
            tree.insert(key, value);
        }

        wal.set_len(bytes_read)?;

        Ok(MemTable { tree, wal })
    }

    /// Inserts a new entry into the MemTable.
    /// The new entry is persisted into the WAL for recovery purposes.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let value = Stored::Value(value);
        bincode::serialize_into(&mut self.wal, &(&key, &value))?;
        self.tree.insert(key, value);

        Ok(())
    }

    /// Removes an entry from the MemTable putting a tombstone in its place.
    /// The tombstone is persisted into the WAL for recovery purposes.
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
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        match self.tree.get(key) {
            Some(Stored::Value(v)) => Some(v),
            _ => None,
        }
    }

    fn create_wal(path: &Path) -> std::io::Result<File> {
        let mut path = path.to_path_buf();
        path.push(WAL_PATH);

        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
    }

    fn open_wal(path: &Path) -> std::io::Result<File> {
        let mut path = path.to_path_buf();
        path.push(WAL_PATH);

        OpenOptions::new().read(true).write(true).open(&path)
    }
}

#[cfg(test)]
mod tests {
    use crate::memtable::MemTable;
    use crate::test_utils::*;

    use anyhow::{Ok, Result};

    #[test]
    fn memtable_gets_and_inserts_are_successful() -> Result<()> {
        let test = Test::new();
        let mut memtable = test.create_memtable();

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;

        assert_eq!(memtable.get("key4"), None);
        assert_eq!(memtable.get("key1"), Some("value1".as_bytes()));
        test.clean();
        Ok(())
    }

    #[test]
    fn memtables_deletes_are_successful() -> Result<()> {
        let test = Test::new();
        let mut memtable = test.create_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.remove("key2".to_string())?;

        assert_eq!(memtable.get("key2"), None);
        test.clean();
        Ok(())
    }

    #[test]
    fn recovering_through_wal_yields_the_same_tree() -> Result<()> {
        let test = Test::new();
        let mut memtable = test.create_memtable();

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;

        let recovered = MemTable::recover(&test.path)?;

        assert_eq!(memtable.tree, recovered.tree);
        test.clean();
        Ok(())
    }

    #[test]
    fn it_recovers_from_corrupted_wal() -> Result<()> {
        let test = Test::new();
        let mut memtable = test.create_memtable();

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.insert("key3".to_string(), "value1".as_bytes().to_owned())?;
        memtable.remove("key1".to_string())?;

        test.corrupt_wal();

        let recovered = MemTable::recover(&test.path)?;
        assert_eq!(memtable.tree, recovered.tree);

        test.clean();
        Ok(())
    }

    #[test]
    fn corrupted_log_is_truncated() -> Result<()> {
        let test = Test::new();
        let mut memtable = test.create_memtable();

        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key3".to_string(), "value1".as_bytes().to_owned())?;

        let wal = MemTable::open_wal(&test.path)?;
        let wal_metadata = wal.metadata()?;
        let wal_length = wal_metadata.len();

        test.corrupt_wal();

        MemTable::recover(&test.path)?;
        let wal_metadata = wal.metadata()?;
        let recovered_wal_length = wal_metadata.len();

        assert_eq!(wal_length, recovered_wal_length);
        test.clean();
        Ok(())
    }
}
