use std::{
    fs::{File, OpenOptions},
    io::Write,
    marker::PhantomData,
    path::{Path, PathBuf},
};

use anyhow::{bail, Result};
use bincode::ErrorKind;
use serde::{de::DeserializeOwned, Serialize};

pub(crate) struct WriteAheadLog<T: Serialize + DeserializeOwned> {
    path: PathBuf,
    file: File,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> WriteAheadLog<T> {
    pub(crate) fn new(path: &Path) -> Result<WriteAheadLog<T>> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        Ok(WriteAheadLog {
            path: path.to_owned(),
            file: fd,
            _marker: PhantomData,
        })
    }

    pub(crate) fn recover<F>(path: &Path, mut consumer: F) -> Result<WriteAheadLog<T>>
    where
        F: FnMut(T),
    {
        let fd = OpenOptions::new().read(true).write(true).open(path)?;

        let mut wal = WriteAheadLog {
            path: path.to_owned(),
            file: fd,
            _marker: PhantomData,
        };

        let mut bytes_read = 0;

        while let Ok(Some(deserialized_value)) = wal.read() {
            bytes_read += bincode::serialized_size(&deserialized_value)?;
            consumer(deserialized_value);
        }

        wal.file.set_len(bytes_read)?;

        Ok(wal)
    }

    pub(crate) fn append(&mut self, value: &T) -> Result<()> {
        bincode::serialize_into(&self.file, value)?;
        self.file.flush()?;

        Ok(())
    }

    fn read(&mut self) -> Result<Option<T>> {
        match bincode::deserialize_from::<_, T>(&self.file) {
            Ok(entry) => Ok(Some(entry)),
            Err(error) if reached_eof(&error) => Ok(None),
            Err(error) => bail!(error),
        }
    }
}

fn reached_eof(error: &ErrorKind) -> bool {
    if let bincode::ErrorKind::Io(ref root_cause) = *error {
        root_cause.kind() == std::io::ErrorKind::UnexpectedEof
    } else {
        false
    }
}
