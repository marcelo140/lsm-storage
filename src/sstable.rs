use std::fs::File;
use std::path::PathBuf;
use std::io::Read;

use crate::Stored;

pub struct SSTable {
    path: PathBuf,
}

/// Ordered set of <key, values> pairs stored in disk.
impl SSTable {
    pub fn new(path: PathBuf) -> Self {
        SSTable { path }
    }

    /// Gets the value for a given key, if it is stored in this SSTable.
    ///
    /// TODO
    /// - optimize it. this thing is scanning the entire file for each read.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut file = File::open(&self.path).unwrap();
        let mut contents = Vec::new();

        file.read_to_end(&mut contents).unwrap();
        let contents: Vec<(String, Stored)> = bincode::deserialize(&contents).unwrap();

        let value = contents.iter()
            .find(|(k, _)| *k == key).cloned()
            .map(|(_, v)| v);

        match value {
            Some(Stored::Value(v)) => Some(v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;

    #[test]
    fn read_from_table() {
        let (uuid, mut engine) = setup();

        let times = engine.config.threshold;
        inject(&mut engine, times);

        let engine = engine.db.lock().unwrap();
        let table = &engine.sstables[0];
        let value = String::from_utf8(table.get("key-3").unwrap()).unwrap();
        assert_eq!("value-3", value);

        clean(&uuid);
    }
}
