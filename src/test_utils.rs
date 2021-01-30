use std::fs;
use std::path::PathBuf;

use crate::Storage;
use crate::MemTable;

use uuid::Uuid;

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

    let memtable = MemTable::new(&path);

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

    Storage::builder()
        .segments_path(path)
        .build()
        .unwrap()
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

    for i in start..start+times {
        let k = format!("key-{}", i);
        let v = format!("value-{}", i).as_bytes().to_owned();
        writer.insert(k, v).unwrap();
    }
}

