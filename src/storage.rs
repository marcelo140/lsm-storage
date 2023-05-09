use std::borrow::BorrowMut;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::engine::Engine;
use crate::memtable::MemTable;
use crate::sstable::SSTable;

use anyhow::Result;

/// Defines the configuration for the storage necessary to handle sstables.
struct Config {
    /// The path where the segments are stored.
    segments_path: PathBuf,
    /// The path where the segments are stored.
    wal_path: PathBuf,
    /// The pattern for the segments name.
    segments_name: String,
    /// The size at which a memtable is converted into a sstable.
    threshold: usize,
}

/// The engine and its configuration. Why isn't the configuration inside the engine itself?
/// Maybe because it's read-only.
#[derive(Clone)]
pub struct Storage {
    engine: Arc<Mutex<Engine>>,
    config: Arc<Config>,
}

pub struct StorageBuilder {
    config: Config,
}

/// Builder to create the storage.
impl StorageBuilder {
    pub fn new() -> Self {
        let mut path = PathBuf::new();
        path.push(".");

        StorageBuilder {
            config: Config {
                segments_path: path.clone(),
                wal_path: path,
                segments_name: "seg-logs".to_owned(),
                threshold: 1024,
            },
        }
    }

    pub fn segments_path(mut self, segments_path: PathBuf) -> Self {
        // TODO: this shouldn't be here but just hacking away for now
        let mut wal_path = segments_path.clone();
        wal_path.push("memtable_wal");

        self.config.segments_path = segments_path;
        self.config.wal_path = wal_path;

        self
    }

    pub fn segments_name(mut self, segments_name: String) -> Self {
        self.config.segments_name = segments_name;
        self
    }

    /// Builds the storage.
    /// - ensures the directory where the segments will be stored exists
    /// - builds a vector of sstables based on the files on that directory that match the segment
    /// name
    /// - creates an empty memtable
    pub fn build(self) -> Result<Storage> {
        std::fs::create_dir_all(&self.config.segments_path)?;

        let sstables = self.find_sstables()?;
        let seq_logs = sstables.len();
        let memtable = self.bootstrap_memtable()?;

        let engine = Engine {
            sstables,
            seq_logs,
            memtable,
        };

        Ok(Storage {
            config: Arc::new(self.config),
            engine: Arc::new(Mutex::new(engine)),
        })
    }

    fn bootstrap_memtable(&self) -> Result<MemTable> {
        if self.config.wal_path.exists() {
            MemTable::recover(&self.config.wal_path)
        } else {
            MemTable::new(&self.config.wal_path)
        }
    }

    // TODO: a sstable may be corrupted due to a crash while being written. Fix this later.
    fn find_sstables(&self) -> Result<Vec<SSTable>> {
        let mut sstables = Vec::new();

        for entry in std::fs::read_dir(&self.config.segments_path)? {
            let path = entry?.path();
            let filename = path.file_name().unwrap().to_str().unwrap();

            if filename.starts_with(&self.config.segments_name) {
                let id = filename.rsplit('-').next().unwrap();
                let id: usize = id.parse()?;

                sstables.push((id, SSTable::new(path)));
            }
        }

        sstables.sort_by_key(|t| t.0);

        Ok(sstables.into_iter().flat_map(|t| t.1).collect())
    }
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A borrow of the storage that allows writing.
pub struct WriteHandler<'engine> {
    storage: &'engine mut Storage,
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

    /// Creates the lock file to avoid other engines writing concurrently and returns
    /// a WriteHandler.
    pub fn open_as_writer(&mut self) -> Result<WriteHandler> {
        let lock = self.lock_path();

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock)
            .map(move |_| WriteHandler { storage: self })
            .map_err(From::from)
    }

    /// Performs a read by trying to find the value in the memtable and falling back to the
    /// sstables if not successful.
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        let engine = &mut self.engine.lock().unwrap();

        engine.memtable.get(key).map(|v| v.to_vec()).or_else(|| {
            for table in engine.sstables.iter_mut().rev().borrow_mut() {
                let v = table.get(key).unwrap();

                if v.is_some() {
                    return v;
                }
            }

            None
        })
    }
}

impl<'engine> WriteHandler<'engine> {
    /// Inserts a value into the memtable. If the memtable size reaches its threshold, converts it
    /// into a sstable.
    ///
    /// TODO:
    /// - the memtable is swapped with an empty one before it is persisted. concurrent readers will
    /// see the storage in a past state state.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let mut engine = self.storage.engine.lock().unwrap();

        engine.memtable.insert(key, value).unwrap();

        if engine.memtable.len() == self.storage.config.threshold {
            let path = self.storage.segment_path(engine.seq_logs);

            let memtable = std::mem::replace(
                &mut engine.memtable,
                MemTable::new(&self.storage.config.wal_path).unwrap(),
            );

            memtable.persist(&path)?;
            let sstable = SSTable::new(path)?;

            engine.sstables.push(sstable);
            engine.seq_logs += 1;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let mut engine = self.storage.engine.lock().unwrap();

        engine.memtable.remove(key).unwrap();

        if engine.memtable.len() == self.storage.config.threshold {
            let path = self.storage.segment_path(engine.seq_logs);

            let memtable = std::mem::replace(
                &mut engine.memtable,
                MemTable::new(&self.storage.config.wal_path).unwrap(),
            );

            memtable.persist(&path)?;
            let sstable = SSTable::new(path)?;

            engine.sstables.push(sstable);
            engine.seq_logs += 1;
        }

        Ok(())
    }
}

impl<'engine> Drop for WriteHandler<'engine> {
    fn drop(&mut self) {
        let lock = self.storage.lock_path();
        std::fs::remove_file(lock).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{storage::Storage, test_utils::*};

    #[test]
    fn memtable_is_fresh() -> Result<()> {
        let test = Test::new()?;
        let path = test.simple_path();
        let mut storage = Storage::builder().segments_path(path).build()?;

        let times = storage.config.threshold * 2;
        inject(&mut storage, times);

        let engine = storage.engine.lock().unwrap();

        assert_eq!(engine.sstables.len(), 2);
        assert_eq!(engine.memtable.len(), 0);

        Ok(())
    }

    #[test]
    fn engine_recovers_sstables() -> Result<()> {
        let test = Test::new()?;
        let path = test.simple_path();
        let mut storage = Storage::builder().segments_path(path.clone()).build()?;

        let times = storage.config.threshold * 2;
        inject(&mut storage, times);

        let storage = Storage::builder().segments_path(path).build()?;
        let engine = storage.engine.lock().unwrap();

        assert_eq!(engine.sstables.len(), 2);

        Ok(())
    }

    #[test]
    fn read() -> Result<()> {
        let test = Test::new()?;
        let path = test.simple_path();
        let mut storage = Storage::builder().segments_path(path.clone()).build()?;
        let threshold = storage.config.threshold;

        let v1 = storage.read("key-500");
        let v2 = storage.read("key-1500");
        assert_eq!(None, v1);
        assert_eq!(None, v2);

        inject(&mut storage, threshold);

        let v1 = String::from_utf8(storage.read("key-500").unwrap()).unwrap();
        let v2 = storage.read("key-1500");
        assert_eq!("value-500", v1);
        assert_eq!(None, v2);

        inject_from(&mut storage, threshold, threshold);

        let v1 = String::from_utf8(storage.read("key-500").unwrap()).unwrap();
        let v2 = String::from_utf8(storage.read("key-1500").unwrap()).unwrap();
        assert_eq!("value-500", v1);
        assert_eq!("value-1500", v2);

        Ok(())
    }

    fn inject(engine: &mut Storage, times: usize) {
        let mut writer = engine.open_as_writer().unwrap();

        for i in 0..times {
            let k = format!("key-{}", i);
            let v = format!("value-{}", i).as_bytes().to_owned();
            writer.insert(k, v).unwrap();
        }
    }

    fn inject_from(engine: &mut Storage, times: usize, start: usize) {
        let mut writer = engine.open_as_writer().unwrap();

        for i in start..start + times {
            let k = format!("key-{}", i);
            let v = format!("value-{}", i).as_bytes().to_owned();
            writer.insert(k, v).unwrap();
        }
    }
}
