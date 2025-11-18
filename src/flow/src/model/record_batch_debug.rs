use crate::model::RecordBatch;

impl RecordBatch {
    pub fn debug_print(&self) {
        println!("[RecordBatch] rows={}", self.num_rows());
        for (row_idx, row) in self.rows().iter().enumerate() {
            println!("  row {row_idx}:");
            for ((source, column), value) in row.entries() {
                println!("    {}.{} = {:?}", source, column, value);
            }
        }
    }
}
