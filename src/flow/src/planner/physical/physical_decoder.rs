use crate::catalog::StreamDecoderConfig;
use crate::planner::physical::BasePhysicalPlan;
use datatypes::Schema;
use std::sync::Arc;

/// Physical operator for decoding raw byte payloads into collections.
#[derive(Debug, Clone)]
pub struct PhysicalDecoder {
    pub base: BasePhysicalPlan,
    source_name: String,
    decoder: StreamDecoderConfig,
    schema: Arc<Schema>,
}

impl PhysicalDecoder {
    pub fn new(
        source_name: impl Into<String>,
        decoder: StreamDecoderConfig,
        schema: Arc<Schema>,
        children: Vec<Arc<crate::planner::physical::PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            source_name: source_name.into(),
            decoder,
            schema,
        }
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
