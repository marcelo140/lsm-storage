#[cfg(test)]
mod test_utils;

mod memtable;
mod sstable;

use std::path::PathBuf;
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use sstable::SSTable;
use memtable::MemTable;

struct Config {
    segments_path: PathBuf,
    segments_name: String,
    threshold: usize,
}

pub struct Engine {
    seq_logs: usize,
    memtable: MemTable,
    sstables: Vec<SSTable>,
}

pub struct Storage {
    db: Arc<Mutex<Engine>>,
    config: Arc<Config>,
}

pub struct StorageBuilder {
    config: Config,
}

impl StorageBuilder {
    pub fn new() -> Self {
        let mut path = PathBuf::new();
        path.push(".");

        StorageBuilder {
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

    pub fn build(self) -> Result<Storage> {
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

                sstables.push((id, SSTable::new(path)));
                seq_logs += 1;
            }
        }

        sstables.sort_by_key(|t| t.0);
        let sstables = sstables.into_iter().map(|t| t.1).collect();

        let engine = Engine {
            sstables,
            seq_logs,
            memtable: MemTable::new(),
        };

        Ok(Storage {
            config: Arc::new(self.config),
            db: Arc::new(Mutex::new(engine)),
        })
    }
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WriteHandler<'engine> {
    engine: &'engine mut Storage,
}

impl Storage {
    pub fn builder() -> StorageBuilder {
        StorageBuilder::new()
    }

    pub fn new() -> Result<Self> {
        StorageBuilder::new().build()
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

    pub fn open_as_writer(&mut self) -> Result<WriteHandler> {
        let lock = self.lock_path();

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock)
            .map(move |_| WriteHandler{ engine: self })
            .map_err(From::from)
    }

    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        let engine = self.db.lock().unwrap();

        engine.memtable.get(key)
            .or_else(|| {
                for table in engine.sstables.iter().rev() {
                    let v = table.get(key);

                    if v.is_some() {
                        return v;
                    }
                }

                None
            })
    }
}

impl<'engine> WriteHandler<'engine> {
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let mut engine = self.engine.db.lock().unwrap();

        engine.memtable.insert(key, value);

        // TODO: data race here, reads will get wrong results.
        if engine.memtable.len() == self.engine.config.threshold {
            let path = self.engine.segment_path(engine.seq_logs);

            let memtable = std::mem::replace(&mut engine.memtable, MemTable::new());
            let sstable = memtable.persist(&path)?;

            engine.sstables.push(sstable);
            engine.seq_logs += 1;
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
    use crate::test_utils::*;

    #[test]
    fn memtable_is_fresh() {
        let (uuid, mut engine) = setup();

        let times = engine.config.threshold*2;
        inject(&mut engine, times);

        let engine = engine.db.lock().unwrap();

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
        let engine = engine.db.lock().unwrap();

        assert_eq!(engine.sstables.len(), 2);

        clean(&uuid);
    }

    #[test]
    fn read() {
        let (uuid, mut engine) = setup();
        let threshold = engine.config.threshold;

        let v1 = engine.read("key-500");
        let v2 = engine.read("key-1500");
        assert_eq!(None, v1);
        assert_eq!(None, v2);

        inject(&mut engine, threshold);

        let v1 = String::from_utf8(engine.read("key-500").unwrap()).unwrap();
        let v2 = engine.read("key-1500");
        assert_eq!("value-500", v1);
        assert_eq!(None, v2);

        inject_from(&mut engine, threshold, threshold);

        let v1 = String::from_utf8(engine.read("key-500").unwrap()).unwrap();
        let v2 = String::from_utf8(engine.read("key-1500").unwrap()).unwrap();
        assert_eq!("value-500", v1);
        assert_eq!("value-1500", v2);

        clean(&uuid);
    }
}
