use crate::format;
use crate::sstable::SSTable;
use crate::wal::WriteAheadLog;
use crate::Stored;
use anyhow::Result;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

/// An in-memory data-structure that keeps entries ordered by key.
///
/// It is hard to keep a mutable on-disk data structure ordered so new entries are inserted into an in-memory table.
/// Once the size of the table exceeds a certain threshold, it is persisted as a SSTable.
///
/// In order to recover from a crash without losing the in-memory data, every insertion should be
/// inserted into a write-ahead log. As such, insertions and removes can fail if they are unable
/// to persist to disk.
///
/// In case of remove operations, the original key-value pair may already be persisted in a persisted
/// SSTable and thus cannot be simply removed. This is why we insert a Tombstone in remove operations.
pub struct MemTable {
    pub(crate) tree: BTreeMap<String, Stored>,
    wal: WriteAheadLog<(String, Stored)>,
}

impl MemTable {
    /// Creates an empty MemTable and its write-ahead log.
    pub fn new(wal_path: &Path) -> Result<Self> {
        Ok(MemTable {
            tree: BTreeMap::new(),
            wal: WriteAheadLog::new(wal_path)?,
        })
    }

    /// Creates a MemTable from a write-ahead log.
    ///
    /// Panics if the WAL has a corrupted header.
    /// Truncates the WAL in the presence of corrupted values.
    pub fn recover(wal_path: &Path) -> Result<Self> {
        let mut tree = BTreeMap::new();

        let wal = WriteAheadLog::recover(wal_path, |(key, value)| {
            tree.insert(key, value);
        })?;

        Ok(MemTable { tree, wal })
    }

    /// Inserts a new entry into the MemTable.
    /// The new entry is persisted into the WAL for recovery purposes.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let value = Stored::Value(value);
        let change = (key, value);

        self.wal.append(&change)?;
        self.tree.insert(change.0, change.1);

        Ok(())
    }

    /// Removes an entry from the MemTable by appending a tombstone.
    /// The tombstone is persisted into the WAL for recovery purposes.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let change = (key, Stored::Tombstone);

        self.wal.append(&change)?;
        self.tree.insert(change.0, change.1);

        Ok(())
    }

    /// The number of entries in the MemTable.
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Returns the value corresponding to the given key, if present.
    pub fn lookup(&self, key: &str) -> Option<&[u8]> {
        match self.tree.get(key) {
            Some(Stored::Value(v)) => Some(v),
            _ => None,
        }
    }

    /// Persists the MemTable to disk storing its entries in-order.
    ///
    /// Returns the corresponding SSTable.
    /// TODO: remove SSTable dependency
    pub fn persist(&self, path: &Path) -> Result<SSTable> {
        let mut fd = File::create(path)?;

        let kvs: Vec<(String, Stored)> = self.tree.clone().into_iter().collect();
        for (key, value) in kvs {
            format::write_entry(&mut fd, &key, &value)?;
        }
        fd.flush()?;

        // std::fs::remove_file(self.wal_path.to_owned())?;

        SSTable::new(path)
    }

    fn iter(&self) -> std::collections::btree_map::Iter<String, Stored> {
        self.tree.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::format;
    use crate::memtable::MemTable;
    use crate::{test_utils::*, Stored};

    use anyhow::Result;

    #[test]
    fn lookup_finds_inserted_entries() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;

        assert_eq!(memtable.lookup("key2"), None);
        assert_eq!(memtable.lookup("key1"), Some("value1".as_bytes()));
        Ok(())
    }

    #[test]
    fn lookup_should_not_find_removed_entries() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.remove("key1".to_string())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.remove("key2".to_string())?;

        assert_eq!(memtable.lookup("key1"), None);
        assert_eq!(memtable.lookup("key2"), None);
        Ok(())
    }

    #[test]
    fn recover_yields_the_same_memtable() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;

        let recovered = MemTable::recover(&test.wal_path())?;

        assert_eq!(memtable.tree, recovered.tree);
        Ok(())
    }

    #[test]
    // TODO: move to WAL tests
    fn recover_loads_from_corrupted_wal() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.insert("key3".to_string(), "value3".as_bytes().to_owned())?;
        memtable.remove("key1".to_string())?;

        test.corrupt_wal()?;

        let recovered = MemTable::recover(&test.wal_path())?;
        assert_eq!(memtable.tree, recovered.tree);

        Ok(())
    }

    #[test]
    // TODO: move to WAL tests
    fn recover_should_truncate_corrupted_log() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.insert("key3".to_string(), "value3".as_bytes().to_owned())?;

        let wal = MemTable::open_wal(&test.wal_path())?;
        let wal_metadata = wal.metadata()?;
        let wal_length = wal_metadata.len();

        test.corrupt_wal()?;

        MemTable::recover(&test.wal_path())?;
        let wal_metadata = wal.metadata()?;
        let recovered_wal_length = wal_metadata.len();

        assert_eq!(wal_length, recovered_wal_length);
        Ok(())
    }

    #[test]
    // TODO: move to SSTable
    fn persist_should_store_all_elements_in_order() -> Result<()> {
        let test = Test::new()?;

        let mut memtable = test.create_memtable()?;
        memtable.insert("c".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("a".to_string(), "value3".as_bytes().to_owned())?;
        memtable.remove("a".to_string())?;
        memtable.insert("b".to_string(), "value2".as_bytes().to_owned())?;

        let sstable_path = test.path("sstable-1");
        memtable.persist(&sstable_path)?;

        let fd = File::open(sstable_path)?;
        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            ("a".to_string(), Stored::Tombstone)
        );
        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            (
                "b".to_string(),
                Stored::Value("value2".as_bytes().to_owned())
            )
        );
        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            (
                "c".to_string(),
                Stored::Value("value1".as_bytes().to_owned())
            )
        );

        Ok(())
    }

    #[test]
    // TODO: remove
    fn persisting_memtable_should_delete_wal() -> Result<()> {
        let test = Test::new()?;

        let mut memtable = test.create_memtable()?;
        memtable.insert("c".to_string(), "value1".as_bytes().to_owned())?;

        let sstable_path = test.path("sstable-1");
        memtable.persist(&sstable_path)?;

        let wal_path = test.wal_path();
        let wal = File::open(wal_path);

        assert_eq!(wal.unwrap_err().kind(), std::io::ErrorKind::NotFound);

        Ok(())
    }
}
