use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::engine::Engine;
use crate::sstable::SSTable;

pub fn start_compaction(engine: Arc<Mutex<Engine>>) {
    loop {
        {
            let mut locked_engine = engine.lock().unwrap();

            let tables_to_merge = locked_engine
                .sstables0
                .clone()
                .into_iter()
                .chain(locked_engine.sstables1.clone().into_iter());

            let merged_table = tables_to_merge.reduce(|acc, table| {
                let mut acc_reader = acc.reader().unwrap();
                let mut table_reader = table.reader().unwrap();

                SSTable::merge(PathBuf::new(), &mut acc_reader, &mut table_reader).unwrap()
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

        thread::sleep(Duration::new(120, 0));
    }
}

#[cfg(test)]
mod tests {
    fn compaction_in_L0_changes_all_files_in_L1() {}

    fn compaction_after_L1_only_touches_specific_files() {}

    fn compacted_data_after_L0_is_broken_into_ordered_files_with_capped_size() {}

    fn compaction_in_last_layer_removes_tombstones() { }

    fn merged_sttables_are_removed_from_view_and_deleted() {}

    fn result_of_compaction_is_available_at_the_correct_level() {}
}