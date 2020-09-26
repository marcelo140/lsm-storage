use crate::Engine;
use uuid::Uuid;

use std::path::PathBuf;

pub fn setup() -> (String, Engine) {
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

pub fn engine_from_uuid(uuid: &str) -> Engine {
    let mut path = PathBuf::new();
    path.push(".");
    path.push(&uuid);

    Engine::builder()
        .segments_path(path)
        .build()
        .unwrap()
}

pub fn inject(engine: &mut Engine, times: usize) {
    let mut writer = engine.open_as_writer().unwrap();

    for i in 0..times {
        let k = format!("key-{}", i);
        let v = format!("value-{}", i).as_bytes().to_owned();
        writer.insert(k, v).unwrap();
    }
}

