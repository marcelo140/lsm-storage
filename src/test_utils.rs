use crate::format;
use crate::memtable::MemTable;
use crate::sstable::SSTable;
use crate::storage::Storage;
use crate::Stored;

use anyhow::Ok;
use anyhow::Result;
use tempfile::tempdir as create_tempdir;
use tempfile::TempDir;

use std::fs::File;
use std::fs::OpenOptions;
use std::path::PathBuf;
use uuid::Uuid;

static WAL_PATH: &str = "write-ahead-log";
static SSTABLE_PATH: &str = "sstable";

pub struct Test {
    tempdir: TempDir,
}

impl Test {
    pub fn new() -> Result<Self> {
        Ok(Test {
            tempdir: create_tempdir()?,
        })
    }

    pub fn create_memtable(&self) -> Result<MemTable> {
        let wal_path = self.wal_path();

        Ok(MemTable::new(&wal_path)?)
    }

    pub(crate) fn generate_sstable(
        &self,
        name: &str,
        values: &[(String, Stored)],
    ) -> Result<SSTable> {
        let path = self.path(&format!("{}-{}", SSTABLE_PATH, name));
        let mut fd = File::create(path.clone())?;

        for (key, value) in values {
            format::write_entry(&mut fd, key, value)?;
        }

        SSTable::new(path)
    }

    pub fn corrupt_wal(&self) -> Result<()> {
        let wal_path = self.wal_path();
        let mut wal = OpenOptions::new().append(true).open(&wal_path)?;

        bincode::serialize_into(&mut wal, &"5")?;
        Ok(())
    }

    pub fn path(&self, path: &str) -> PathBuf {
        let mut new_path = self.tempdir.path().to_owned();
        new_path.push(path);

        new_path
    }

    pub fn wal_path(&self) -> PathBuf {
        let mut wal_path = self.tempdir.path().to_owned();
        wal_path.push(WAL_PATH);

        wal_path
    }

    pub fn sstable_path(&self, name: &str) -> PathBuf {
        let mut sstable_path = self.tempdir.path().to_owned();
        sstable_path.push(format!("{}-{}", SSTABLE_PATH, name));

        sstable_path
    }
}

pub fn setup() -> (String, Storage) {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();
    let engine = engine_from_uuid(&uuid);

    (uuid, engine)
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
