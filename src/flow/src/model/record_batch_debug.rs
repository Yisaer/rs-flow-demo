use crate::model::RecordBatch;

impl RecordBatch {
    pub fn debug_print(&self) {
        println!(
            "[RecordBatch] rows={}, columns={}",
            self.num_rows(),
            self.num_columns()
        );
        for column in self.columns() {
            println!(
                "  column {}.{} = {:?}",
                column.source_name(),
                column.name(),
                column.values()
            );
        }
    }
}
