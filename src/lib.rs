use std::collections::BTreeMap;
use std::path::{PathBuf, Path};
use std::fs::{File, OpenOptions};
use std::io::{Write, Read};

use anyhow::Result;

struct SSTable {
    path: PathBuf,
}

struct MemTable {
    tree: BTreeMap<String, Vec<u8>>,
}

impl SSTable {
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

impl MemTable {
    fn new() -> Self {
        MemTable { tree: BTreeMap::new() }
    }

    fn insert(&mut self, key: String, value: Vec<u8>) {
        self.tree.insert(key, value);
    }

    fn len(&self) -> usize {
        self.tree.len()
    }

    fn persist(self, path: &Path) -> Result<SSTable> {
        let mut fd = File::create(&path)?;

        let kvs: Vec<(String, Vec<u8>)> = self.tree.into_iter().collect();
        let serialized_kv = bincode::serialize(&kvs).unwrap();
        fd.write_all(&serialized_kv)?;

        Ok(SSTable { path: path.to_path_buf() })
    }
}

struct Config {
    segments_path: PathBuf,
    segments_name: String,
    threshold: usize,
}

pub struct EngineBuilder {
    config: Config,
}

impl EngineBuilder {
    pub fn new() -> Self {
        let mut path = PathBuf::new();
        path.push(".");

        EngineBuilder {
            config: Config {
                segments_path: path,
                segments_name: "seg-logs".to_owned(),
                threshold: 1024,
            },
        }
    }

    pub fn segments_path(mut self, segments_path: PathBuf) -> Self {
        self.config.segments_path = segments_path;
        self
    }

    pub fn segments_name(mut self, segments_name: String) -> Self {
        self.config.segments_name = segments_name;
        self
    }

    pub fn build(self) -> Result<Engine> {
        let mut seq_logs = 0;
        let mut sstables = Vec::new();

        std::fs::create_dir_all(&self.config.segments_path)?;

        // TODO: a sstable may be corrupted due to a crash while being written. Fix this later.
        for entry in std::fs::read_dir(&self.config.segments_path)? {
            let path = entry?.path();
            let filename = path.file_name().unwrap().to_str().unwrap();

            if filename.starts_with(&self.config.segments_name) {
                let id = filename.rsplit('-').next().unwrap();
                let id: usize = id.parse()?;

                sstables.push((id, SSTable { path }));
                seq_logs += 1;
            }
        }

        sstables.sort_by_key(|t| t.0);
        let sstables = sstables.into_iter().map(|t| t.1).collect();

        Ok(Engine { config: self.config, sstables, seq_logs, memtable: MemTable::new() })
    }
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Engine {
    config: Config,
    seq_logs: usize,
    memtable: MemTable,
    sstables: Vec<SSTable>,
}

pub struct WriteHandler<'engine> {
    engine: &'engine mut Engine,
}

pub struct ReadHandler<'engine> {
    engine: &'engine Engine,
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::new()
    }

    pub fn new() -> Result<Self> {
        EngineBuilder::new().build()
    }

    pub fn open_as_writer(&mut self) -> Result<WriteHandler> {
        let lock = self.lock_path();

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock)
            .map(move |_| WriteHandler{ engine: self })
            .map_err(From::from)
    }

    pub fn open_as_reader(&self) -> ReadHandler {
        ReadHandler { engine: self }
    }

    fn lock_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(&self.config.segments_path);
        path.push("lock");

        path
    }

    fn segment_path(&self, seg_id: usize) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(&self.config.segments_path);
        path.push(format!("{}-{}", &self.config.segments_name, seg_id));

        path
    }
}

impl<'engine> WriteHandler<'engine> {
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        self.engine.memtable.insert(key, value);

        // TODO: data race here, reads will get wrong results.
        if self.engine.memtable.len() == self.engine.config.threshold {
            let path = self.engine.segment_path(self.engine.seq_logs);

            let memtable = std::mem::replace(&mut self.engine.memtable, MemTable::new());
            let sstable = memtable.persist(&path)?;

            self.engine.sstables.push(sstable);
            self.engine.seq_logs += 1;
        }

        Ok(())
    }
}

impl<'engine> Drop for WriteHandler<'engine> {
    fn drop(&mut self) {
        let lock = self.engine.lock_path();
        std::fs::remove_file(&lock).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;
    use uuid::Uuid;
    use std::path::PathBuf;

    fn setup() -> (String, Engine) {
        let uuid = Uuid::new_v4().to_hyphenated().to_string();
        let engine = engine_from_uuid(&uuid);

        (uuid, engine)
    }

    fn clean(uuid: &str) {
        let mut path = PathBuf::new();
        path.push(".");
        path.push(&uuid);

        std::fs::remove_dir_all(path).unwrap();
    }

    fn engine_from_uuid(uuid: &str) -> Engine {
        let mut path = PathBuf::new();
        path.push(".");
        path.push(&uuid);

        Engine::builder()
            .segments_path(path)
            .build()
            .unwrap()
    }

    fn inject(engine: &mut Engine, times: usize) {
        let mut writer = engine.open_as_writer().unwrap();

        for i in 0..times {
            let k = format!("key-{}", i);
            let v = format!("value-{}", i).as_bytes().to_owned();
            writer.insert(k, v).unwrap();
        }
    }

    #[test]
    fn memtable_is_fresh() {
        let (uuid, mut engine) = setup();

        let times = engine.config.threshold*2;
        inject(&mut engine, times);

        assert_eq!(engine.sstables.len(), 2);
        assert_eq!(engine.memtable.len(), 0);

        clean(&uuid);
    }

    #[test]
    fn engine_recovers_sstables() {
        let (uuid, mut engine) = setup();

        let times = engine.config.threshold*2;
        inject(&mut engine, times);

        let engine = engine_from_uuid(&uuid);
        assert_eq!(engine.sstables.len(), 2);

        clean(&uuid);
    }

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
