use std::collections::BTreeMap;
use std::path::Path;
use std::fs::File;
use std::io::Write;

use anyhow::Result;

use crate::sstable::SSTable;

pub struct MemTable {
    tree: BTreeMap<String, Vec<u8>>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable { tree: BTreeMap::new() }
    }

    pub fn insert(&mut self, key: String, value: Vec<u8>) {
        self.tree.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.tree.get(key).cloned()
    }

    pub fn persist(self, path: &Path) -> Result<SSTable> {
        let mut fd = File::create(&path)?;

        let kvs: Vec<(String, Vec<u8>)> = self.tree.into_iter().collect();
        let serialized_kv = bincode::serialize(&kvs).unwrap();
        fd.write_all(&serialized_kv)?;

        Ok(SSTable::new(path.to_path_buf()))
    }
}

