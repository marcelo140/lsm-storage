// use std::borrow::BorrowMut;
// use std::path::PathBuf;
// use std::sync::{Arc, Mutex};

// use crate::lsm_tree::{self, LsmTree};
// use crate::memtable::MemTable;
// use crate::sstable::{SSTable, SSTableReader};
// use crate::{SEGMENTS_NAME, WAL_NAME};

// use anyhow::Result;

// /// Defines the configuration for the storage necessary to handle sstables.
// #[derive(Clone)]
// pub(crate) struct Config {
//     /// The path where the segments are stored.
//     segments_path: PathBuf,
//     /// The path where the WALs are stored.
//     wal_path: PathBuf,
//     /// The size at which a memtable is converted into a sstable.
//     pub threshold: usize,
// }

// /// The engine and its configuration. Why isn't the configuration inside the engine itself?
// /// Maybe because it's read-only.
// #[derive(Clone)]
// pub struct LsmTreeService {
//     pub(crate) lsm_tree: Arc<Mutex<LsmTree>>,
//     pub(crate) config: Config,
//     sequence_number: usize,
// }

// pub struct LsmTreeServiceBuilder {
//     config: Config,
// }

// /// Builder to create the storage.
// impl LsmTreeServiceBuilder {
//     pub fn new() -> Self {
//         let mut current_path = PathBuf::new();
//         current_path.push(".");

//         let mut segments_path = current_path.clone();
//         segments_path.push(SEGMENTS_NAME);

//         let mut wal_path = current_path;
//         wal_path.push(WAL_NAME);

//         LsmTreeServiceBuilder {
//             config: Config {
//                 segments_path,
//                 wal_path,
//                 threshold: 1024,
//             },
//         }
//     }

//     pub fn segments_path(mut self, segments_path: PathBuf) -> Self {
//         self.config.segments_path = segments_path;

//         self
//     }

//     pub fn wal_path(mut self, wal_path: PathBuf) -> Self {
//         self.config.wal_path = wal_path;

//         self
//     }

//     /// Builds the LSM-Tree service.
//     /// - ensures the directory where the SSTables and WALs will be stored exist.
//     /// - builds a vector of sstables based on the files on that directory that match the segment name
//     /// - creates an empty memtable
//     pub fn build(self) -> Result<LsmTreeService> {
//         std::fs::create_dir_all(&self.config.segments_path)?;
//         std::fs::create_dir_all(&self.config.wal_path)?;

//         let sstables = self.load_sstables()?;
//         let sstable_readers: Vec<SSTableReader> = sstables
//             .iter()
//             .flat_map(|sstable| sstable.reader())
//             .collect();
//         let (active_memtable, memtables) = self.load_memtables()?;

//         let lsm_tree = Arc::new(Mutex::new(LsmTree {
//             sstables: Vec::new(),
//             sstable_readers: Vec::new(),
//             active_memtable,
//             memtables,
//         }));

//         Ok(LsmTreeService {
//             config: self.config,
//             lsm_tree,
//             sequence_number: 0,
//         })
//     }

//     fn load_memtables(&self) -> Result<(MemTable, Vec<Arc<MemTable>>)> {
//         let mut memtables = Vec::new();

//         for entry in std::fs::read_dir(&self.config.wal_path)? {
//             let path = entry?.path();
//             let filename = path.file_name().unwrap().to_str().unwrap();

//             if filename.starts_with(WAL_NAME) {
//                 let memtable = MemTable::recover(&path)?;
//                 memtables.push(memtable);
//             }
//         }

//         memtables.sort_by_key(|t| t.id);
//         let memtable = memtables.pop();

//         match memtable {
//             None => {
//                 let mut wal_path = self.config.wal_path.clone();
//                 wal_path.push(format!("{}-{}", WAL_NAME, 0));

//                 let memtable = MemTable::new(0, &wal_path)?;
//                 Ok((memtable, vec![]))
//             }
//             Some(memtable) => {
//                 let memtables = memtables.into_iter().map(|t| Arc::new(t)).collect();
//                 Ok((memtable, memtables))
//             }
//         }
//     }

//     // TODO: a sstable may be corrupted due to a crash while being written. Fix this later.
//     fn load_sstables(&self) -> Result<Vec<SSTable>> {
//         let mut sstables = Vec::new();

//         for entry in std::fs::read_dir(&self.config.segments_path)? {
//             let path = entry?.path();
//             let filename = path.file_name().unwrap().to_str().unwrap();

//             if filename.starts_with(SEGMENTS_NAME) {
//                 let id = filename.rsplit('-').next().unwrap();
//                 let id: usize = id.parse()?;

//                 sstables.push((id, SSTable::new(&path)));
//             }
//         }

//         sstables.sort_by_key(|t| t.0);

//         Ok(sstables.into_iter().map(|t| t.1).collect())
//     }
// }

// impl Default for LsmTreeServiceBuilder {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl LsmTreeService {
//     pub fn builder() -> LsmTreeServiceBuilder {
//         LsmTreeServiceBuilder::new()
//     }

//     pub fn new() -> Result<Self> {
//         LsmTreeServiceBuilder::new().build()
//     }

//     fn segment_path(&self, seg_id: usize) -> PathBuf {
//         let mut path = PathBuf::new();
//         path.push(&self.config.segments_path);
//         path.push(format!("{}-{}", SEGMENTS_NAME, seg_id));

//         path
//     }

//     pub fn read(&self, key: &str) -> Option<Vec<u8>> {
//         let lsm_tree = &mut self.lsm_tree.lock().unwrap();

//         lsm_tree.lookup(key)
//     }

//     /// Inserts a value into the memtable. If the memtable size reaches its threshold, converts it
//     /// into a sstable.
//     ///
//     /// TODO:
//     /// - the memtable is swapped with an empty one before it is persisted. concurrent readers will
//     /// see the storage in a past state state.
//     pub fn insert(&mut self, key: String, value: Vec<u8>) -> Result<()> {
//         let mut lsm_tree = self.lsm_tree.lock().unwrap();

//         lsm_tree.insert(key, value)?;

//         // if engine.active_memtable.len() == self.config.threshold {
//         //     LsmTreeService::replace_memtable(
//         //         &self.persistence_sender,
//         //         &mut self.sequence_number,
//         //         &mut engine,
//         //         &self.config.wal_path,
//         //     )?;
//         //     self.persistence_sender.send("message".to_string())?;
//         // }

//         Ok(())
//     }

//     pub fn remove(&mut self, key: String) -> Result<()> {
//         let mut lsm_tree = self.lsm_tree.lock().unwrap();

//         lsm_tree.active_memtable.remove(key).unwrap();

//         // if engine.active_memtable.len() == self.config.threshold {
//         //     LsmTreeService::replace_memtable(
//         //         &self.persistence_sender,
//         //         &mut self.sequence_number,
//         //         &mut engine,
//         //         &self.config.wal_path,
//         //     )?;
//         // }

//         Ok(())
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::ops::Range;

//     use anyhow::Result;

//     use crate::{lsm_tree_service::LsmTreeService, test_utils::*};

//     #[test]
//     fn memtables_are_converted_to_sstables_when_threshold_is_reached() -> Result<()> {
//         let test = Test::new()?;
//         let mut storage = test.create_storage()?;

//         let number_of_rows = storage.config.threshold * 2;
//         inject_rows(&mut storage, 0..number_of_rows);

//         let engine = storage.lsm_tree.lock().unwrap();

//         assert_eq!(engine.sstables0.len(), 2);
//         assert_eq!(engine.memtable.len(), 0);

//         Ok(())
//     }

//     #[test]
//     fn engine_loads_sstables_and_wal_when_it_starts() -> Result<()> {
//         let test = Test::new()?;
//         let mut storage = test.create_storage()?;

//         let number_of_rows = storage.config.threshold * 2;
//         inject_rows(&mut storage, 0..number_of_rows);

//         let storage = test.create_storage()?;
//         let engine = storage.lsm_tree.lock().unwrap();

//         assert_eq!(engine.sstables0.len(), 2);
//         assert_eq!(engine.memtable.len(), 0); // TODO: We have no guarantee that the WAL was flushed to disk so there might be data missing.

//         Ok(())
//     }

//     #[test]
//     fn reads_from_memtable_and_sstable() -> Result<()> {
//         let test = Test::new()?;
//         let mut storage = test.create_storage()?;
//         let threshold = storage.config.threshold;

//         let v1 = storage.read("key-500");
//         let v2 = storage.read("key-1500");
//         assert_eq!(None, v1);
//         assert_eq!(None, v2);

//         inject_rows(&mut storage, 0..threshold);

//         let v1 = String::from_utf8(storage.read("key-500").unwrap()).unwrap();
//         let v2 = storage.read("key-1500");
//         assert_eq!("value-500", v1);
//         assert_eq!(None, v2);

//         inject_rows(&mut storage, threshold..threshold * 2);

//         let v1 = String::from_utf8(storage.read("key-500").unwrap()).unwrap();
//         let v2 = String::from_utf8(storage.read("key-1500").unwrap()).unwrap();
//         assert_eq!("value-500", v1);
//         assert_eq!("value-1500", v2);

//         Ok(())
//     }

//     fn inject_rows(engine: &mut LsmTreeService, range_of_keys: Range<usize>) {
//         let mut writer = engine.open_as_writer().unwrap();

//         for i in range_of_keys {
//             let k = format!("key-{}", i);
//             let v = format!("value-{}", i).as_bytes().to_owned();
//             writer.insert(k, v).unwrap();
//         }
//     }
// }
