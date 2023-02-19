use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;

use crate::MemTable;
use crate::Storage;

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

    pub fn create_memtable(&self) -> MemTable {
        fs::create_dir_all(&self.path).unwrap();

        MemTable::new(&self.path).unwrap()
    }

    pub fn corrupt_wal(&self) {
        let mut wal_path = self.path.to_path_buf();
        wal_path.push("write-ahead-log");
        let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();

        bincode::serialize_into(&mut wal, &"5").unwrap();
    }

    pub fn clean(self) {
        std::fs::remove_dir_all(self.path).unwrap();
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
