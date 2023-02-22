use anyhow::Result;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::Stored;
use crate::format;

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
    wal_path: PathBuf,
    wal: File,
}

impl MemTable {
    /// Creates an empty MemTable.
    pub fn new(wal_path: &Path) -> Result<Self> {
        let wal = MemTable::create_wal(wal_path)?;

        Ok(MemTable {
            tree: BTreeMap::new(),
            wal_path: wal_path.to_path_buf(),
            wal,
        })
    }

    /// Creates a MemTable from a write-ahead-log
    pub fn recover(wal_path: &Path) -> Result<Self> {
        let wal = MemTable::open_wal(wal_path)?;

        let mut tree = BTreeMap::new();
        let mut bytes_read = 0;

        while let Ok(deserialized_value) = format::read_entry(&wal) {
            bytes_read += format::entry_size(&deserialized_value)?;
            tree.insert(deserialized_value.0, deserialized_value.1);
        }

        wal.set_len(bytes_read)?;

        Ok(MemTable {
            tree,
            wal_path: wal_path.to_path_buf(),
            wal,
        })
    }

    /// Inserts a new entry into the MemTable.
    /// The new entry is persisted into the WAL for recovery purposes.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let value = Stored::Value(value);
        format::write_entry(&mut self.wal, &key, &value)?;
        self.tree.insert(key, value);

        Ok(())
    }

    /// Removes an entry from the MemTable putting a tombstone in its place.
    /// The tombstone is persisted into the WAL for recovery purposes.
    pub fn remove(&mut self, key: String) -> Result<()> {
        format::write_entry(&mut self.wal, &key, &Stored::Tombstone)?;
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

    /// Persists the MemTable to disk storing its entries in-order.
    ///
    /// Returns the corresponding SSTable.
    pub fn persist(self, path: &Path) -> Result<()> {
        let mut fd = File::create(path)?;

        let kvs: Vec<(String, Stored)> = self.tree.into_iter().collect();
        for (key, value) in kvs {
            format::write_entry(&mut fd, &key, &value)?;
        }

        std::fs::remove_file(self.wal_path)?;

        Ok(())
    }

    fn create_wal(path: &Path) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
    }

    fn open_wal(path: &Path) -> std::io::Result<File> {
        OpenOptions::new().read(true).write(true).open(path)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::memtable::MemTable;
    use crate::format;
    use crate::{test_utils::*, Stored};

    use anyhow::Result;

    #[test]
    fn get_should_see_inserted_entries() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;

        assert_eq!(memtable.get("key2"), None);
        assert_eq!(memtable.get("key1"), Some("value1".as_bytes()));
        test.clean()
    }

    #[test]
    fn get_should_not_see_deleted_entries() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.remove("key1".to_string())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.remove("key2".to_string())?;

        assert_eq!(memtable.get("key1"), None);
        assert_eq!(memtable.get("key2"), None);
        test.clean()
    }

    #[test]
    fn recover_should_yield_the_same_memtable() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;

        let recovered = MemTable::recover(&test.wal_path())?;

        assert_eq!(memtable.tree, recovered.tree);
        test.clean()
    }

    #[test]
    fn recover_should_load_from_corrupted_wal() -> Result<()> {
        let test = Test::new()?;
        let mut memtable = test.create_memtable()?;

        memtable.insert("key1".to_string(), "value1".as_bytes().to_owned())?;
        memtable.insert("key2".to_string(), "value2".as_bytes().to_owned())?;
        memtable.insert("key3".to_string(), "value3".as_bytes().to_owned())?;
        memtable.remove("key1".to_string())?;

        test.corrupt_wal()?;

        let recovered = MemTable::recover(&test.wal_path())?;
        assert_eq!(memtable.tree, recovered.tree);

        test.clean()
    }

    #[test]
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
        test.clean()
    }

    #[test]
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
        assert_eq!(format::read_entry(&fd)?, ("a".to_string(), Stored::Tombstone));
        assert_eq!(
            format::read_entry(&fd)?,
            (
                "b".to_string(),
                Stored::Value("value2".as_bytes().to_owned())
            )
        );
        assert_eq!(
            format::read_entry(&fd)?,
            (
                "c".to_string(),
                Stored::Value("value1".as_bytes().to_owned())
            )
        );

        test.clean()
    }

    #[test]
    fn persisting_memtable_should_delete_wal() -> Result<()> {
        let test = Test::new()?;

        let mut memtable = test.create_memtable()?;
        memtable.insert("c".to_string(), "value1".as_bytes().to_owned())?;

        let sstable_path = test.path("sstable-1");
        memtable.persist(&sstable_path)?;

        let wal_path = test.wal_path();
        let wal = File::open(wal_path);

        assert_eq!(wal.unwrap_err().kind(), std::io::ErrorKind::NotFound);

        test.clean()
    }
}
