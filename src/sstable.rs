use anyhow::Result;

use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use crate::Stored;
use crate::format;

/// A data structure that allows read-only access into an ordered set of <key, value> pairs persisted on-disk.
///
/// Upon initialization, all entries are read to build an index with the offset for each key. This
/// allows for quick reads into the log by seeking directly into the correct offset.
pub struct SSTable {
    fd: File,
    indexes: HashMap<String, u64>,
}

impl SSTable {
    /// Initializes a SSTable for the provided path and scans the log to build the in-memory index.
    pub fn new(path: PathBuf) -> Result<Self> {
        let fd = File::open(path)?;
        let indexes = SSTable::build_index_table(&fd)?;

        Ok(SSTable { fd, indexes })
    }

    fn build_index_table(fd: &File) -> Result<HashMap<String, u64>> {
        let mut indexes = HashMap::new();

        let mut bytes_read = 0;

        while let Ok(entry) = format::read_entry(fd) {
            let pair_size = format::entry_size(&entry)?;
            indexes.insert(entry.0, bytes_read);
            bytes_read += pair_size;
        }

        Ok(indexes)
    }

    /// Returns the value for the provided key if it is stored in the SSTable.
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        // TODO: this shouldn't need to be mutable
        let value_position = &self.indexes.get(key);

        if value_position.is_none() {
            return Ok(None);
        }

        self.fd.seek(SeekFrom::Start(*value_position.unwrap()))?;
        let (_key, value) = format::read_entry(&self.fd)?;

        match value {
            Stored::Value(v) => Ok(Some(v)),
            Stored::Tombstone => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_utils::*;

    #[test]
    fn constructor_should_load_sstable_correctly() -> Result<()> {
        Ok(())
    }

    #[test]
    fn get_should_return_expected_value() -> Result<()> {
        let (uuid, mut engine) = setup();

        let times = engine.config.threshold + 1;
        inject(&mut engine, times);

        let mut engine = engine.db.lock().unwrap();
        let table = &mut engine.sstables[0];
        let value = String::from_utf8(table.get("key-3").unwrap().unwrap()).unwrap();
        assert_eq!("value-3", value);

        clean(&uuid);
        Ok(())
    }

    #[test]
    fn merging_should_keep_all_new_values() -> Result<()> {
        Ok(())
    }

    #[test]
    fn merging_should_keep_non_overriden_values() -> Result<()> {
        Ok(())
    }
}
