use anyhow::Result;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::{PathBuf, Path};

use crate::Stored;
use crate::memtable::MemTable;

/// A data structure that allows read-only access into an ordered set of <key, value> pairs persisted on-disk.
///
/// Upon initialization, all entries are read to build an index with the offset for each key. This
/// allows for quick reads into the log by seeking directly into the correct offset.
pub struct SSTable {
    path: PathBuf,
    indexes: HashMap<String, u64>,
}

impl SSTable {
    /// Initializes a SSTable for the provided path and scans the log to build the in-memory index.
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut indexes = HashMap::new();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        let mut bytes_read = 0;

        while let Ok((key, value)) = bincode::deserialize_from::<_, (String, Stored)>(&file) {
            let pair_size = bincode::serialized_size(&(&key, &value)).unwrap();
            indexes.insert(key, bytes_read);
            bytes_read += pair_size;
        }

        Ok(SSTable { path, indexes })
    }

    /// Persists the MemTable to disk storing its entries in-order.
    ///
    /// Returns the corresponding SSTable.
    pub fn from_memtable(memtable: MemTable, path: &Path) -> Result<Self> {
        let mut fd = File::create(path)?;

        let kvs: Vec<(String, Stored)> = memtable.tree.into_iter().collect();
        for (key, value) in kvs {
            bincode::serialize_into(&mut fd, &(&key, value))?;
        }

        SSTable::new(path.to_path_buf())
    }

    /// Returns the value for the provided key if it is stored in the SSTable.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value_position = &self.indexes.get(key);
        let mut file = OpenOptions::new().read(true).open(&self.path)?;

        if value_position.is_none() {
            return Ok(None);
        }

        file.seek(SeekFrom::Start(*value_position.unwrap()))?;
        let (_key, value) = bincode::deserialize_from::<&File, (String, Stored)>(&file).unwrap();

        match value {
            Stored::Value(v) => Ok(Some(v)),
            Stored::Tombstone => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;

    #[test]
    fn read_from_table() {
        let (uuid, mut engine) = setup();

        let times = engine.config.threshold+1;
        inject(&mut engine, times);

        let engine = engine.db.lock().unwrap();
        let table = &engine.sstables[0];
        let value = String::from_utf8(table.get("key-3").unwrap().unwrap()).unwrap();
        assert_eq!("value-3", value);

        clean(&uuid);
    }
}
