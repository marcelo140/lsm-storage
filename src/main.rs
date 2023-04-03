use std::path::PathBuf;

use axum::http::StatusCode;
use lsm_storage::storage::Storage;

use axum::extract::{Path, State};
use axum::{routing::get, Router};

#[tokio::main]
async fn main() {
    let segments = PathBuf::from(std::env::args().nth(1).unwrap());
    let storage = Storage::builder().segments_path(segments).build().unwrap();

    let app = Router::new()
        .route("/key/:key", get(kv_get).post(kv_insert))
        .with_state(storage);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn kv_get(
    State(storage): State<Storage>,
    Path(key): Path<String>,
) -> Result<String, StatusCode> {
    let value = storage
        .read(&key)
        .and_then(|bytes| String::from_utf8(bytes).ok());

    match value {
        Some(value) => Ok(value),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn kv_insert(
    State(mut storage): State<Storage>,
    Path(key): Path<String>,
    body: String,
) -> Result<(), StatusCode> {
    let mut writer = storage.open_as_writer().unwrap();
    writer.insert(key, body.into_bytes()).unwrap();

    Ok(())
}
