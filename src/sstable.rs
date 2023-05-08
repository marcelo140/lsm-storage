use crate::format;
use crate::Stored;
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

/// A data structure that allows read-only access into an ordered set of <key, value> pairs persisted on-disk.
///
/// Upon initialization, all entries are read to build an index with the offset for each key. This
/// allows for quick reads into the log by seeking directly into the correct offset.
pub struct SSTable {
    fd: File,
    indexes: HashMap<String, u64>,
}

impl SSTable {
    /// Initializes a SSTable for the provided path and scans the log to build the in-memory index.
    pub fn new(path: PathBuf) -> Result<Self> {
        let fd = File::open(path)?;
        let indexes = SSTable::build_index_table(&fd)?;

        Ok(SSTable { fd, indexes })
    }

    fn build_index_table(fd: &File) -> Result<HashMap<String, u64>> {
        let mut indexes = HashMap::new();

        let mut bytes_read = 0;

        while let Ok(Some(entry)) = format::read_entry(fd) {
            let pair_size = format::entry_size(&entry)?;
            indexes.insert(entry.0, bytes_read);
            bytes_read += pair_size;
        }

        Ok(indexes)
    }

    /// Returns the value for the provided key if it is stored in the SSTable.
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        // TODO: this shouldn't need to be mutable
        let value_position = &self.indexes.get(key);

        if value_position.is_none() {
            return Ok(None);
        }

        self.fd.seek(SeekFrom::Start(*value_position.unwrap()))?;
        let (_key, value) = format::read_entry(&self.fd)?.unwrap();

        match value {
            Stored::Value(v) => Ok(Some(v)),
            Stored::Tombstone => Ok(None),
        }
    }

    pub(crate) fn merge(
        path: PathBuf,
        old_sstable: &mut SSTable,
        new_sstable: &mut SSTable,
    ) -> Result<SSTable> {
        old_sstable.fd.rewind()?;
        new_sstable.fd.rewind()?;

        let mut old_entry = format::read_entry(&old_sstable.fd)?;
        let mut new_entry = format::read_entry(&new_sstable.fd)?;
        let mut fd = File::create(&path)?;

        while let Some(((old_key, old_value), (new_key, new_value))) =
            old_entry.as_ref().zip(new_entry.as_ref())
        {
            match old_key.cmp(new_key) {
                std::cmp::Ordering::Equal => {
                    format::write_entry(&mut fd, new_key, new_value)?;
                    old_entry = format::read_entry(&old_sstable.fd)?;
                    new_entry = format::read_entry(&new_sstable.fd)?;
                }
                std::cmp::Ordering::Less => {
                    format::write_entry(&mut fd, old_key, old_value)?;
                    old_entry = format::read_entry(&old_sstable.fd)?;
                }
                std::cmp::Ordering::Greater => {
                    format::write_entry(&mut fd, new_key, new_value)?;
                    new_entry = format::read_entry(&new_sstable.fd)?;
                }
            }
        }

        while let Some((old_key, old_value)) = old_entry {
            format::write_entry(&mut fd, &old_key, &old_value)?;
            old_entry = format::read_entry(&old_sstable.fd)?;
        }

        while let Some((new_key, new_value)) = new_entry {
            format::write_entry(&mut fd, &new_key, &new_value)?;
            new_entry = format::read_entry(&new_sstable.fd)?;
        }

        SSTable::new(path)
    }
}

#[cfg(test)]
mod tests {
    use super::SSTable;
    use crate::{format, test_utils::*, Stored};
    use anyhow::Result;
    use std::{
        fs::File,
        io::{Seek, SeekFrom},
    };

    #[test]
    fn constructor_should_load_sstable_correctly() -> Result<()> {
        let test = Test::new()?;
        let sstable_path = test.sstable_path("table");
        let contents = vec![
            ("key-1".to_owned(), Stored::Value(b"value-1".to_vec())),
            ("key-2".to_owned(), Stored::Value(b"value-2".to_vec())),
            ("key-3".to_owned(), Stored::Value(b"value-3".to_vec())),
        ];

        test.generate_sstable("table", &contents)?;
        let mut sstable = SSTable::new(sstable_path)?;
        let index1 = sstable.indexes.get("key-1").unwrap();
        let index2 = sstable.indexes.get("key-2").unwrap();
        let index3 = sstable.indexes.get("key-3").unwrap();

        assert_eq!(contents.len(), 3);

        sstable.fd.seek(SeekFrom::Start(*index1))?;
        assert_eq!(
            format::read_entry(&sstable.fd)?.unwrap(),
            ("key-1".to_owned(), Stored::Value(b"value-1".to_vec()))
        );

        sstable.fd.seek(SeekFrom::Start(*index2))?;
        assert_eq!(
            format::read_entry(&sstable.fd)?.unwrap(),
            ("key-2".to_owned(), Stored::Value(b"value-2".to_vec()))
        );

        sstable.fd.seek(SeekFrom::Start(*index3))?;
        assert_eq!(
            format::read_entry(&sstable.fd)?.unwrap(),
            ("key-3".to_owned(), Stored::Value(b"value-3".to_vec()))
        );

        Ok(())
    }

    #[test]
    fn get_should_return_expected_value() -> Result<()> {
        let test = Test::new()?;

        let mut sstable = test.generate_sstable(
            "table",
            &vec![
                ("key-1".to_owned(), Stored::Value(b"value-1".to_vec())),
                ("key-2".to_owned(), Stored::Value(b"value-2".to_vec())),
                ("key-3".to_owned(), Stored::Value(b"value-3".to_vec())),
            ],
        )?;

        let value = sstable.get("key-1")?;
        assert!(value.is_some());

        let deserialized_value = String::from_utf8(value.unwrap())?;
        assert_eq!("value-1", deserialized_value);

        Ok(())
    }

    #[test]
    fn merging_should_write_in_order_and_merge_all_elements() -> Result<()> {
        let test = Test::new()?;

        let mut old_sstable = test.generate_sstable(
            "table1",
            &vec![
                ("key-1".to_owned(), Stored::Value(b"value-1".to_vec())),
                ("key-2".to_owned(), Stored::Value(b"value-2".to_vec())),
                ("key-3".to_owned(), Stored::Value(b"value-3".to_vec())),
                ("key-5".to_owned(), Stored::Tombstone),
            ],
        )?;

        let mut new_sstable = test.generate_sstable(
            "table2",
            &vec![
                ("key-1".to_owned(), Stored::Value(b"value-5".to_vec())),
                ("key-3".to_owned(), Stored::Tombstone),
                ("key-4".to_owned(), Stored::Value(b"value-4".to_vec())),
            ],
        )?;

        let sstable_path = test.sstable_path("merged-table");
        SSTable::merge(sstable_path.clone(), &mut old_sstable, &mut new_sstable)?;

        let fd = File::open(sstable_path)?;

        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            ("key-1".to_string(), Stored::Value(b"value-5".to_vec()))
        );

        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            ("key-2".to_string(), Stored::Value(b"value-2".to_vec()))
        );

        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            ("key-3".to_string(), Stored::Tombstone)
        );

        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            ("key-4".to_string(), Stored::Value(b"value-4".to_vec()))
        );

        assert_eq!(
            format::read_entry(&fd)?.unwrap(),
            ("key-5".to_string(), Stored::Tombstone)
        );

        Ok(())
    }
}
