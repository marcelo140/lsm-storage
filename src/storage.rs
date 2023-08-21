use std::borrow::BorrowMut;
use std::path::{PathBuf, Path};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;

use crate::{SEGMENTS_NAME, WAL_NAME, memtable};
use crate::compactor::start_compaction;
use crate::engine::Engine;
use crate::memtable::MemTable;
use crate::sstable::SSTable;

use anyhow::Result;
use tokio::sync::mpsc::UnboundedSender;

/// Defines the configuration for the storage necessary to handle sstables.
#[derive(Clone)]
pub(crate) struct Config {
    /// The path where the segments are stored.
    segments_path: PathBuf,
    /// The path where the WALs are stored.
    wal_path: PathBuf,
    /// The size at which a memtable is converted into a sstable.
    pub threshold: usize,
}

/// The engine and its configuration. Why isn't the configuration inside the engine itself?
/// Maybe because it's read-only.
#[derive(Clone)]
pub struct Storage{
    pub(crate) engine: Arc<Mutex<Engine>>,
    pub(crate) config: Config,
    persistence_sender: tokio::sync::mpsc::UnboundedSender<String>,
    sequence_number: usize,
    compactor: Arc<JoinHandle<()>>,
}

pub struct StorageBuilder {
    config: Config,
}

/// Builder to create the storage.
impl StorageBuilder {
    pub fn new() -> Self {
        let mut current_path = PathBuf::new();
        current_path.push(".");

        let mut segments_path = current_path.clone();
        segments_path.push(SEGMENTS_NAME);

        let mut wal_path = current_path;
        wal_path.push(WAL_NAME);

        StorageBuilder {
            config: Config {
                segments_path,
                wal_path,
                threshold: 1024,
            },
        }
    }

    pub fn segments_path(mut self, segments_path: PathBuf) -> Self {
        self.config.segments_path = segments_path;

        self
    }

    pub fn wal_path(mut self, wal_path: PathBuf) -> Self {
        self.config.wal_path = wal_path;

        self
    }

    /// Builds the storage.
    /// - ensures the directory where the sstables and WALs will be stored exists
    /// - builds a vector of sstables based on the files on that directory that match the segment
    /// name
    /// - creates an empty memtable
    pub fn build(self) -> Result<Storage> {
        std::fs::create_dir_all(&self.config.segments_path)?;
        std::fs::create_dir_all(&self.config.wal_path)?;

        let sstables0 = self.load_sstables()?;
        let sstable_readers0 = sstables0.iter().flat_map(|sstable| sstable.reader()).collect();
        let (active_memtable, memtables) = self.load_memtables()?;

        let engine = Arc::new(Mutex::new(Engine {
            sstables0,
            sstables1: Vec::new(),
            sstable_readers0,
            sstable_readers1: Vec::new(),
            active_memtable,
            memtables,
        }));

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let compactor_engine = engine.clone();
        let compactor_thread = thread::spawn(move || {
            start_compaction(compactor_engine, receiver);
        });

        Ok(Storage {
            config: self.config,
            engine,
            persistence_sender: sender,
            compactor: Arc::new(compactor_thread),
            sequence_number: 0,
        })
    }

    fn load_memtables(&self) -> Result<(MemTable, Vec<Arc<MemTable>>)> {
        let mut memtables = Vec::new();

        for entry in std::fs::read_dir(&self.config.wal_path)? {
            let path = entry?.path();
            let filename = path.file_name().unwrap().to_str().unwrap();

            if filename.starts_with(WAL_NAME) {
                let memtable = MemTable::recover(&path)?;
                memtables.push(memtable);
            }
        }
    
        memtables.sort_by_key(|t| t.id);
        let memtable = memtables.pop();
    
        match memtable {
            None => {
                let mut wal_path = self.config.wal_path.clone();
                wal_path.push(format!("{}-{}", WAL_NAME, 0));

                let memtable = MemTable::new(0, &wal_path)?;
                Ok((memtable, vec![]))
            }
            Some(memtable) => {
                let memtables = memtables.into_iter().map(|t| Arc::new(t)).collect();
                Ok((memtable, memtables))
            }
        }
    }

    // TODO: a sstable may be corrupted due to a crash while being written. Fix this later.
    fn load_sstables(&self) -> Result<Vec<SSTable>> {
        let mut sstables = Vec::new();

        for entry in std::fs::read_dir(&self.config.segments_path)? {
            let path = entry?.path();
            let filename = path.file_name().unwrap().to_str().unwrap();

            if filename.starts_with(SEGMENTS_NAME) {
                let id = filename.rsplit('-').next().unwrap();
                let id: usize = id.parse()?;

                sstables.push((id, SSTable::new(&path)));
            }
        }

        sstables.sort_by_key(|t| t.0);

        Ok(sstables.into_iter().map(|t| t.1).collect())
    }
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn builder() -> StorageBuilder {
        StorageBuilder::new()
    }

    pub fn new() -> Result<Self> {
        StorageBuilder::new().build()
    }

    fn segment_path(&self, seg_id: usize) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(&self.config.segments_path);
        path.push(format!("{}-{}", SEGMENTS_NAME, seg_id));

        path
    }

    /// Performs a read by trying to find the value in the memtable and falling back to the
    /// sstables if not successful.
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        let engine = &mut self.engine.lock().unwrap();

        engine.memtables
            .iter()
            .rev()
            .find_map(|memtable| memtable.get(key))
            .map(|v| v.to_vec())
            .or_else(|| {
                for table in engine.sstable_readers0.iter_mut().rev().borrow_mut() {
                    let v = table.get(key).unwrap();

                    if v.is_some() {
                        return v;
                    }
                }

                None
            })
    }

    /// Inserts a value into the memtable. If the memtable size reaches its threshold, converts it
    /// into a sstable.
    ///
    /// TODO:
    /// - the memtable is swapped with an empty one before it is persisted. concurrent readers will
    /// see the storage in a past state state.
    pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let mut engine = self.engine.lock().unwrap();

        engine.active_memtable.insert(key, value).unwrap();

        if engine.active_memtable.len() == self.config.threshold {
            Storage::replace_memtable(&self.persistence_sender, &mut self.sequence_number, &mut engine, &self.config.wal_path)?;
            self.persistence_sender.send("message".to_string())?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let mut engine = self.engine.lock().unwrap();

        engine.active_memtable.remove(key).unwrap();

        if engine.active_memtable.len() == self.config.threshold {
            Storage::replace_memtable(&self.persistence_sender, &mut self.sequence_number, &mut engine, &self.config.wal_path)?;
        }

        Ok(())
    }

    fn replace_memtable(sender: &UnboundedSender<String>, sequence_number: &mut usize, engine: &mut MutexGuard<Engine>, path: &Path) -> Result<()> {
        *sequence_number += 1;
        let new_memtable = MemTable::new(*sequence_number, &path)?;
        let old_memtable = std::mem::replace(&mut engine.active_memtable, new_memtable);
        engine.memtables.push(Arc::new(old_memtable));

        sender.send("message".to_string())?;

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use anyhow::Result;

    use crate::{storage::Storage, test_utils::*};

    #[test]
    fn memtables_are_converted_to_sstables_when_threshold_is_reached() -> Result<()> {
        let test = Test::new()?;
        let mut storage = test.create_storage()?;

        let number_of_rows = storage.config.threshold * 2;
        inject_rows(&mut storage, 0..number_of_rows);

        let engine = storage.engine.lock().unwrap();

        assert_eq!(engine.sstables0.len(), 2);
        assert_eq!(engine.memtable.len(), 0);

        Ok(())
    }

    #[test]
    fn engine_loads_sstables_and_wal_when_it_starts() -> Result<()> {
        let test = Test::new()?;
        let mut storage = test.create_storage()?;

        let number_of_rows = storage.config.threshold * 2;
        inject_rows(&mut storage, 0..number_of_rows);

        let storage = test.create_storage()?;
        let engine = storage.engine.lock().unwrap();

        assert_eq!(engine.sstables0.len(), 2);
        assert_eq!(engine.memtable.len(), 0); // TODO: We have no guarantee that the WAL was flushed to disk so there might be data missing.

        Ok(())
    }

    #[test]
    fn reads_from_memtable_and_sstable() -> Result<()> {
        let test = Test::new()?;
        let mut storage = test.create_storage()?;
        let threshold = storage.config.threshold;

        let v1 = storage.read("key-500");
        let v2 = storage.read("key-1500");
        assert_eq!(None, v1);
        assert_eq!(None, v2);

        inject_rows(&mut storage, 0..threshold);

        let v1 = String::from_utf8(storage.read("key-500").unwrap()).unwrap();
        let v2 = storage.read("key-1500");
        assert_eq!("value-500", v1);
        assert_eq!(None, v2);

        inject_rows(&mut storage, threshold..threshold*2);

        let v1 = String::from_utf8(storage.read("key-500").unwrap()).unwrap();
        let v2 = String::from_utf8(storage.read("key-1500").unwrap()).unwrap();
        assert_eq!("value-500", v1);
        assert_eq!("value-1500", v2);

        Ok(())
    }

    fn inject_rows(engine: &mut Storage, range_of_keys: Range<usize>) {
        let mut writer = engine.open_as_writer().unwrap();

        for i in range_of_keys {
            let k = format!("key-{}", i);
            let v = format!("value-{}", i).as_bytes().to_owned();
            writer.insert(k, v).unwrap();
        }
    }
}
