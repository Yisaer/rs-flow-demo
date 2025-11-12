//! Filter processor - corresponds to PhysicalFilter
//! 
//! This processor filters incoming data based on a predicate expression.
//! Currently just passes data through for pipeline establishment.
//! 
//! Design follows rstream's Executor pattern with tokio::spawn and dedicated filtering routine.
//! Now uses StreamData::Control signals for shutdown, eliminating separate stop channels.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData, StreamError};

/// Filter processor that corresponds to PhysicalFilter
/// 
/// This processor filters incoming data based on a predicate expression.
/// Currently just passes data through for pipeline establishment.
pub struct FilterProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channels from upstream processors
    input_receivers: Vec<broadcast::Receiver<StreamData>>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl FilterProcessor {
    /// Create a new FilterProcessor
    pub fn new(
        physical_plan: Arc<dyn PhysicalPlan>,
        input_receivers: Vec<broadcast::Receiver<StreamData>>,
        downstream_count: usize,
    ) -> Self {
        Self {
            physical_plan,
            input_receivers,
            downstream_count,
        }
    }
    
    /// Apply filter predicate to data (currently just passes through)
    fn should_include(&self, _data: &dyn crate::model::Collection) -> Result<bool, String> {
        // TODO: Implement actual filtering logic based on predicate expression
        // For now, include all data to establish pipeline
        Ok(true)
    }
}

impl StreamProcessor for FilterProcessor {
    fn start(&self) -> ProcessorView {
        // Create only result channel - no stop channel needed
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        
        // Spawn the filter routine - currently just passes data through
        let routine = self.create_filter_routine(result_tx);
        
        let join_handle = tokio::spawn(routine);
        
        // For FilterProcessor, we don't need control sender - it processes data from upstream
        ProcessorView::from_result_receiver(
            result_rx,
            ProcessorHandle::new(join_handle),
        )
    }
    
    fn downstream_count(&self) -> usize {
        self.downstream_count
    }
    
    fn input_receivers(&self) -> Vec<broadcast::Receiver<StreamData>> {
        self.input_receivers.iter()
            .map(|rx| rx.resubscribe())
            .collect()
    }
}

// Private helper methods
impl FilterProcessor {
    /// Create filter routine that runs in tokio task - currently just passes data through
    fn create_filter_routine(
        &self,
        result_tx: broadcast::Sender<StreamData>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut input_receivers = self.input_receivers();
        let downstream_count = self.downstream_count;
        let processor_name = "FilterProcessor".to_string();
        
        async move {
            println!("FilterProcessor: Starting filter routine for {} downstream processors", downstream_count);
            
            // Send stream start signal
            if result_tx.send(StreamData::stream_start()).is_err() {
                println!("FilterProcessor: Failed to send start signal");
                return;
            }
            
            // Process incoming data (currently just passes through)
            // TODO: Implement actual filtering logic
            loop {
                // Process from first input channel (simplified for now)
                // In a real implementation, would need to handle multiple inputs properly
                let result = async {
                    if let Some(receiver) = input_receivers.get_mut(0) {
                        receiver.recv().await
                    } else {
                        // No input receivers, this might be an error case
                        // For now, just wait indefinitely - in reality this should probably end the processor
                        futures::future::pending::<Result<StreamData, broadcast::error::RecvError>>().await
                    }
                };
                
                match result.await {
                    Ok(stream_data) => {
                        // Check for stop signal in the data stream
                        if utils::is_stop_signal(&stream_data) {
                            println!("FilterProcessor: Received stop signal in data stream, shutting down");
                            break;
                        }
                        
                        // Apply filter logic with error handling
                        if stream_data.is_data() {
                            if let Some(collection) = stream_data.as_collection() {
                                match Self::static_should_include_static(collection) {
                                    Ok(should_include) => {
                                        if should_include {
                                            // Data passes filter, send it downstream
                                            if result_tx.send(stream_data).is_err() {
                                                println!("FilterProcessor: All downstream receivers dropped, stopping");
                                                break;
                                            }
                                        } else {
                                            println!("FilterProcessor: Data filtered out");
                                        }
                                    }
                                    Err(filter_error) => {
                                        // Filter processing error - send as StreamData::Error instead of stopping
                                        println!("FilterProcessor: Error during filtering: {}", filter_error);
                                        let stream_error = StreamError::new(filter_error)
                                            .with_source(&processor_name)
                                            .with_timestamp(std::time::SystemTime::now());
                                        
                                        if result_tx.send(StreamData::error(stream_error)).is_err() {
                                            println!("FilterProcessor: All downstream receivers dropped, stopping");
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            // Pass through control signals and errors
                            if result_tx.send(stream_data).is_err() {
                                println!("FilterProcessor: All downstream receivers dropped, stopping");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        // Handle broadcast errors
                        let error_data = utils::handle_receive_error(e);
                        if result_tx.send(error_data).is_err() {
                            println!("FilterProcessor: All downstream receivers dropped, stopping");
                            break;
                        }
                    }
                }
            }
            
            // Send stream end signal
            if result_tx.send(StreamData::stream_end()).is_err() {
                println!("FilterProcessor: Failed to send end signal");
            }
            
            println!("FilterProcessor: Filter routine completed");
        }
    }
    
    /// Static version of should_include for use in async routine
    fn static_should_include_static(_data: &dyn crate::model::Collection) -> Result<bool, String> {
        // TODO: Implement actual filtering logic
        Ok(true)
    }
}