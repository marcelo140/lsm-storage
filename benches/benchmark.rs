use lsm_storage::Storage;

use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use uuid::Uuid;

fn storage_read_same_key(storage: &Storage, key: &str) {
    for _ in 0..3_000 {
        storage.read(key).unwrap();
    }
}

fn setup(size: usize) -> lsm_storage::Storage {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();
    let mut path = PathBuf::new();
    path.push(".");
    path.push(&uuid);

    let mut storage = Storage::builder().segments_path(path).build().unwrap();

    let mut writer = storage.open_as_writer().unwrap();

    for i in 0..size {
        let k = format!("key-{}", i);
        let v = format!("value-{}", i).as_bytes().to_owned();
        writer.insert(k, v).unwrap();
    }

    drop(writer);
    storage
}

fn read_same_key(c: &mut Criterion) {
    let storage = setup(3_000);

    c.bench_function("read same key 1", |b| {
        b.iter(|| storage_read_same_key(&storage, black_box("key-1")))
    });
    c.bench_function("read same key 1000", |b| {
        b.iter(|| storage_read_same_key(&storage, black_box("key-1000")))
    });
    c.bench_function("read same key 2000", |b| {
        b.iter(|| storage_read_same_key(&storage, black_box("key-2000")))
    });
    c.bench_function("read same key 2999", |b| {
        b.iter(|| storage_read_same_key(&storage, black_box("key-2999")))
    });
}

fn storage_scan(engine: &Storage) {
    for i in 0..3_000 {
        engine.read(&format!("key-{}", i));
    }
}

fn bench_storage_scan(c: &mut Criterion) {
    let storage = setup(10_000);

    c.bench_function("storage scan", |b| b.iter(|| storage_scan(&storage)));
}

fn bench_many_writes(c: &mut Criterion) {
    c.bench_function("many writes", |b| b.iter(|| setup(10_250)));
}

fn many_writes_few_keys(storage: &mut Storage) {
    let mut writer = storage.open_as_writer().unwrap();

    for _ in 0..10 {
        for i in 0..1025 {
            let k = format!("key-{}", i);
            let v = format!("value-{}", i).as_bytes().to_owned();
            writer.insert(k, v).unwrap();
        }
    }
}

// TODO: compare this with bench_many_writes
fn bench_many_writes_few_keys(c: &mut Criterion) {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();
    let mut path = PathBuf::new();
    path.push(".");
    path.push(&uuid);

    let mut storage = Storage::builder().segments_path(path).build().unwrap();

    c.bench_function("many writes few keys", |b| {
        b.iter(|| many_writes_few_keys(&mut storage))
    });
}

// TODO:
// - benchmark get of deleted key. compare with get of early key.

criterion_group!(
    benches,
    bench_many_writes,
    read_same_key,
    bench_storage_scan,
    bench_many_writes_few_keys
);

criterion_main!(benches);
