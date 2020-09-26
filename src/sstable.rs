use std::fs::File;
use std::path::PathBuf;
use std::io::Read;

pub struct SSTable {
    path: PathBuf,
}

impl SSTable {
    pub fn new(path: PathBuf) -> Self {
        SSTable { path }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut file = File::open(&self.path).unwrap();
        let mut contents = Vec::new();

        file.read_to_end(&mut contents).unwrap();
        let contents: Vec<(String, Vec<u8>)> = bincode::deserialize(&contents).unwrap();

        contents.iter()
            .find(|(k, _)| *k == key).cloned()
            .map(|(_, v)| v)
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

        let table = &engine.sstables[0];
        let value = String::from_utf8(table.get("key-3").unwrap()).unwrap();
        assert_eq!("value-3", value);

        clean(&uuid);
    }
}