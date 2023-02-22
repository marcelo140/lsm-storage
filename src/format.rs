use crate::Stored;
use anyhow::Result;

pub(crate) fn read_entry<R>(reader: R) -> Result<(String, Stored)>
where
    R: std::io::Read,
{
    let entry = bincode::deserialize_from::<_, (String, Stored)>(reader)?;
    Ok(entry)
}

pub(crate) fn write_entry<W>(writer: &mut W, key: &str, value: &Stored) -> Result<()>
where
    W: std::io::Write,
{
    bincode::serialize_into(writer, &(key, value))?;
    Ok(())
}

pub(crate) fn entry_size(entry: &(String, Stored)) -> Result<u64> {
    let size = bincode::serialized_size(&entry).unwrap();
    Ok(size)
}
