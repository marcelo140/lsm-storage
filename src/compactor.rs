use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedReceiver;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::engine::Engine;
use crate::sstable::SSTable;

pub fn start_compaction(engine: Arc<Mutex<Engine>>, mut receiver: UnboundedReceiver<String>) -> Result<()> {
    // Current behavior: Picks all L0 and L1 SSTables and merges them into a single SSTable
    //     Caveats:
    //       - The final table should be split to multiple tables of a specific size
    // Next steps:
    // - Solve the previous caveat
    //
    while receiver.blocking_recv().is_some() {
        persist_memtable(&*engine)?;
        // trigger_l0_compaction(engine.clone());
        // thread::sleep(Duration::new(120, 0));
    }

    Ok(())
}

fn persist_memtable(engine: &Mutex<Engine>) -> Result<()> {
        let engine2 = engine.lock().unwrap();
        let memtable = engine2.memtables.first().unwrap().clone();
        drop(engine2);

        let uuid = uuid::Uuid::new_v4();
        let mut path = PathBuf::new();
        path.push(".");
        path.push("sstables");
        path.push(format!("sstables-{}", uuid));

        let sstable = memtable.persist(&path)?;
        let sstable_reader = sstable.reader()?;

        let mut engine2 = engine.lock().unwrap();
        engine2.memtables.remove(0);
        engine2.sstables0.push(sstable);
        engine2.sstable_readers0.push(sstable_reader);
        drop(engine2);

        Ok(())
}

fn trigger_l0_compaction(engine: Arc<Mutex<Engine>>) {
    let mut locked_engine = engine.lock().unwrap();

    let tables_to_merge = locked_engine
        .sstables0
        .clone()
        .into_iter()
        .chain(locked_engine.sstables1.clone().into_iter());

    // TODO: merge all tables in 1 pass
    let merged_table = tables_to_merge.reduce(|acc, table| {
        let mut acc_reader = acc.reader().unwrap();
        let mut table_reader = table.reader().unwrap();

        let tempfile = tempfile::NamedTempFile::new().unwrap().into_temp_path().to_path_buf();
        SSTable::merge(tempfile, &mut acc_reader, &mut table_reader).unwrap()
    });

    merged_table.map(|merged_table| {
        let merged_table_reader = merged_table.reader().unwrap();

        locked_engine.sstable_readers0.clear();
        locked_engine.sstables0.clear();
        locked_engine.sstable_readers1.clear();
        locked_engine.sstables1.clear();

        locked_engine.sstables1.push(merged_table);
        locked_engine.sstable_readers1.push(merged_table_reader);
    });
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use crate::{test_utils::Test, compactor::trigger_l0_compaction};

    #[test]
    fn compaction_in_l0_changes_all_files_in_l1() -> Result<()> {
        let test = Test::new()?;

        let mut storage = test.create_storage()?;
        let threshold = storage.config.threshold;
        let expected_sstables = 5;

        Test::inject_data(&mut storage, threshold * expected_sstables)?;
        
        {
            let engine = storage.engine.lock().unwrap();
            assert_eq!(engine.sstables0.len(), expected_sstables);
        }

        trigger_l0_compaction(storage.engine.clone());

        let sstables;

        {
            let engine = storage.engine.lock().unwrap();
            assert_eq!(engine.sstables0.len(), 0);
            assert_eq!(engine.sstables1.len(), 1);
            sstables = Some(engine.sstables1.clone());
        }

        Test::inject_data(&mut storage, threshold * expected_sstables)?;
        trigger_l0_compaction(storage.engine.clone());

        {
            let engine = storage.engine.lock().unwrap();
            assert_eq!(engine.sstables0.len(), 0);
            assert_eq!(engine.sstables1.len(), 1);

            for original_sstable1 in sstables.unwrap() {
                assert!(!engine.sstables1.contains(&original_sstable1));
            }
        }

        Ok(())
    }

    #[test]
    fn compacted_data_after_l0_is_broken_into_ordered_files_with_capped_size() {

    }

    fn compaction_after_L1_only_touches_specific_files() {}

    fn compaction_in_last_layer_removes_tombstones() {}

    fn merged_sttables_are_removed_from_view_and_deleted() {}

    fn result_of_compaction_is_available_at_the_correct_level() {}
}
