use std::path::PathBuf;

use lsm_storage::Storage;

fn main() {
    let mut path = PathBuf::new();
    path.push("./table/");

    Storage::builder()
        .segments_path(path)
        .build()
        .unwrap();
}
