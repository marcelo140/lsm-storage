use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;

use crate::memtable::WAL_PATH;
use crate::MemTable;
use crate::Storage;
use anyhow::Result;

use uuid::Uuid;

pub struct Test {
    pub path: PathBuf,
}

impl Test {
    pub fn new() -> Self {
        let uuid = Uuid::new_v4().to_hyphenated().to_string();

        let mut path = PathBuf::from(".");
        path.push(&uuid);

        Test { path }
    }

    pub fn create_memtable(&self) -> Result<MemTable> {
        fs::create_dir_all(&self.path)?;

        Ok(MemTable::new(&self.path)?)
    }

    pub fn corrupt_wal(&self) -> Result<()> {
        let mut wal_path = self.path.to_path_buf();
        wal_path.push(WAL_PATH);
        let mut wal = OpenOptions::new().append(true).open(&wal_path)?;

        bincode::serialize_into(&mut wal, &"5")?;
        Ok(())
    }

    pub fn clean(self) -> Result<()> {
        std::fs::remove_dir_all(self.path)?;
        Ok(())
    }

    pub fn path(&self, path: &str) -> PathBuf {
        let mut new_path = self.path.clone();
        new_path.push(path);

        new_path
    }
}

pub fn setup() -> (String, Storage) {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();
    let engine = engine_from_uuid(&uuid);

    (uuid, engine)
}

pub fn setup_memtable() -> (String, PathBuf, MemTable) {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();

    let mut path = PathBuf::new();
    path.push(".");
    path.push(&uuid);

    fs::create_dir_all(&path).unwrap();

    let memtable = MemTable::new(&path).unwrap();

    (uuid, path, memtable)
}

pub fn clean(uuid: &str) {
    let mut path = PathBuf::new();
    path.push(".");
    path.push(&uuid);

    std::fs::remove_dir_all(path).unwrap();
}

pub fn engine_from_uuid(uuid: &str) -> Storage {
    let mut path = PathBuf::new();
    path.push(".");
    path.push(&uuid);

    Storage::builder().segments_path(path).build().unwrap()
}

pub fn inject(engine: &mut Storage, times: usize) {
    let mut writer = engine.open_as_writer().unwrap();

    for i in 0..times {
        let k = format!("key-{}", i);
        let v = format!("value-{}", i).as_bytes().to_owned();
        writer.insert(k, v).unwrap();
    }
}

pub fn inject_from(engine: &mut Storage, times: usize, start: usize) {
    let mut writer = engine.open_as_writer().unwrap();

    for i in start..start + times {
        let k = format!("key-{}", i);
        let v = format!("value-{}", i).as_bytes().to_owned();
        writer.insert(k, v).unwrap();
    }
}
