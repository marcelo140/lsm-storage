use crate::Stored;
use anyhow::bail;
use anyhow::Result;
use bincode::ErrorKind;

pub(crate) fn read_entry<R>(reader: R) -> Result<Option<(String, Stored)>>
where
    R: std::io::Read,
{
    match bincode::deserialize_from::<_, (String, Stored)>(reader) {
        Ok(entry) => Ok(Some(entry)),
        Err(error) if reached_eof(&error) => Ok(None),
        Err(error) => bail!(error),
    }
}

pub(crate) fn write_entry<W>(writer: &mut W, key: &str, value: &Stored) -> Result<()>
where
    W: std::io::Write,
{
    bincode::serialize_into(writer, &(key, value))?;
    Ok(())
}

pub(crate) fn write_memtable_header<W>(writer: &mut W, id: usize) -> Result<()>
where
    W: std::io::Write,
{
    bincode::serialize_into(writer, &id)?;
    Ok(())
}

pub(crate) fn read_memtable_header<R>(reader: R) -> Result<Option<usize>>
where
    R: std::io::Read,
{
    match bincode::deserialize_from::<_, usize>(reader) {
        Ok(entry) => Ok(Some(entry)),
        Err(error) if reached_eof(&error) => Ok(None),
        Err(error) => bail!(error),
    }
}

pub(crate) fn memtable_metadata_size(metadata: usize) -> Result<u64> {
    Ok(bincode::serialized_size(&metadata)?)
}

pub(crate) fn entry_size(entry: &(String, Stored)) -> Result<u64> {
    Ok(bincode::serialized_size(&entry)?)
}

pub(crate) fn entry_size_kv(key: &str, value: &Stored) -> Result<usize> {
    Ok(bincode::serialized_size(&(key, value))? as usize)
}

fn reached_eof(error: &ErrorKind) -> bool {
    if let bincode::ErrorKind::Io(ref root_cause) = *error {
        root_cause.kind() == std::io::ErrorKind::UnexpectedEof
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use crate::{test_utils::Test, Stored};
    use anyhow::Result;

    #[test]
    fn read_entry_returns_none_when_file_ends() -> Result<()> {
        let test = Test::new()?;

        test.generate_sstable(
            "name",
            &vec![("key-1".to_owned(), Stored::Value(b"value-1".to_vec()))],
        )?;

        let fd = File::open(test.sstable_path("name"))?;

        let v = crate::format::read_entry(&fd)?;
        assert!(v.is_some());

        let v = crate::format::read_entry(&fd)?;
        assert!(v.is_none());

        Ok(())
    }
}
