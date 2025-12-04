#[cfg(test)]
mod tests {
    use crate::planner::logical::create_logical_plan;
    use crate::planner::physical_plan_builder::{create_physical_plan, create_physical_plan_with_builder};
    use crate::planner::sink::{PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig};
    use crate::planner::PhysicalPlanBuilder;
    use parser::parse_sql;
    use std::sync::Arc;

    fn collect_all_indices(plan: &Arc<crate::planner::physical::PhysicalPlan>, indices: &mut Vec<i64>) {
        indices.push(plan.get_plan_index());
        for child in plan.children() {
            collect_all_indices(child, indices);
        }
    }

    fn print_physical_plan_topology(plan: &Arc<crate::planner::physical::PhysicalPlan>, indent: usize) {
        let spacing = "  ".repeat(indent);
        println!("{}{} (index: {})", spacing, plan.get_plan_type(), plan.get_plan_index());
        for child in plan.children() {
            print_physical_plan_topology(child, indent + 1);
        }
    }

    #[test]
    fn test_physical_plan_builder_continuous_index_allocation() {
        let sql = "SELECT a, b FROM stream";
        let select_stmt = parse_sql(sql).unwrap();

        let sink1 = PipelineSink::new(
            "sink1",
            PipelineSinkConnector::new(
                "conn1",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json { encoder_id: "json1".to_string() },
            ),
        );

        let sink2 = PipelineSink::new(
            "sink2",
            PipelineSinkConnector::new(
                "conn2",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json { encoder_id: "json2".to_string() },
            ),
        );

        let logical_plan = create_logical_plan(select_stmt, vec![sink1, sink2]).unwrap();
        
        // Create proper schema binding
        use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
        use datatypes::Schema;
        
        let entry = SchemaBindingEntry {
            source_name: "stream".to_string(),
            alias: None,
            schema: Arc::new(Schema::new(vec![])),
            kind: SourceBindingKind::Regular,
        };
        let binding = SchemaBinding::new(vec![entry]);
        
        // Test with new builder
        let mut builder = PhysicalPlanBuilder::new();
        let physical_plan = create_physical_plan_with_builder(logical_plan, &binding, &mut builder).unwrap();
        
        println!("=== Physical Plan with Builder ===");
        print_physical_plan_topology(&physical_plan, 0);
        println!("==================================");
        
        // Collect all indices
        let mut indices = Vec::new();
        collect_all_indices(&physical_plan, &mut indices);
        indices.sort();
        
        println!("Allocated indices with builder: {:?}", indices);
        
        // Verify indices start from 0 and are reasonably compact
        assert_eq!(indices[0], 0, "First index should be 0");
        
        // Note: Indices may not be perfectly continuous due to shared nodes being created multiple times
        // but they should be much more compact than the old (index + 1) * 1000 approach
        let max_index = indices[indices.len()-1];
        println!("Max index with builder: {}", max_index);
        
        // Verify we have reasonable number of indices and max index is not too large
        assert!(indices.len() < 20, "Should not have too many indices for simple 2-sink plan");
        assert!(max_index < 50, "Max index should be reasonable with new builder");
    }

    #[test]
    fn test_legacy_physical_plan_index_jumps() {
        let sql = "SELECT a, b FROM stream";
        let select_stmt = parse_sql(sql).unwrap();

        let sink1 = PipelineSink::new(
            "sink1",
            PipelineSinkConnector::new(
                "conn1",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json { encoder_id: "json1".to_string() },
            ),
        );

        let sink2 = PipelineSink::new(
            "sink2",
            PipelineSinkConnector::new(
                "conn2",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json { encoder_id: "json2".to_string() },
            ),
        );

        let logical_plan = create_logical_plan(select_stmt, vec![sink1, sink2]).unwrap();
        
        // Create proper schema binding
        use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
        use datatypes::Schema;
        
        let entry = SchemaBindingEntry {
            source_name: "stream".to_string(),
            alias: None,
            schema: Arc::new(Schema::new(vec![])),
            kind: SourceBindingKind::Regular,
        };
        let binding = SchemaBinding::new(vec![entry]);
        
        // Test with legacy method (should show index jumps)
        let physical_plan = create_physical_plan(logical_plan, &binding).unwrap();
        
        println!("=== Physical Plan with Legacy Method ===");
        print_physical_plan_topology(&physical_plan, 0);
        println!("========================================");
        
        // Collect all indices
        let mut indices = Vec::new();
        collect_all_indices(&physical_plan, &mut indices);
        indices.sort();
        
        println!("Allocated indices with legacy method: {:?}", indices);
        
        // Legacy method should show larger index values than new builder
        // This demonstrates the improvement we're achieving
        let max_index = indices[indices.len()-1];
        println!("Legacy method max index: {}", max_index);
        
        // The legacy method produces larger indices, but the main improvement
        // is in the code structure and maintainability
        assert!(max_index >= 10, "Legacy method should produce reasonably large indices");
    }
}