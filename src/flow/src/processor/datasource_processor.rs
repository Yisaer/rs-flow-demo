//! DataSource processor - corresponds to PhysicalDataSource
//! 
//! This processor acts as a source of data in the stream processing pipeline.
//! Currently generates empty data to establish the data flow pipeline.
//! 
//! Design follows rstream's Executor pattern with tokio::spawn and dedicated data generation routine.
//! Now uses StreamData::Control signals for shutdown, eliminating separate stop channels.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// DataSource processor that corresponds to PhysicalDataSource
/// 
/// This is typically the starting point of a stream processing pipeline.
/// Currently just generates empty data to establish data flow.
pub struct DataSourceProcessor {
    /// The physical plan this processor corresponds to (kept for compatibility but not used in processing)
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channels from children (upstream processors)
    /// For DataSource, this is typically empty, but we support it for flexibility
    input_receivers: Vec<broadcast::Receiver<StreamData>>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor with specified downstream count and upstream receivers
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
}

impl StreamProcessor for DataSourceProcessor {
    fn start(&self) -> ProcessorView {
        // Create only result channel - no stop channel needed
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        
        // Spawn the data source routine
        let routine = self.create_datasource_routine(result_tx);
        
        let join_handle = tokio::spawn(routine);
        
        // For DataSource, we don't need control sender - it's a source
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
impl DataSourceProcessor {
    /// Create data source routine that runs in tokio task
    /// Uses StreamData::Control signals for shutdown instead of separate stop channel
    fn create_datasource_routine(
        &self,
        result_tx: broadcast::Sender<StreamData>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let downstream_count = self.downstream_count;
        let mut input_receivers = self.input_receivers();
        
        async move {
            println!("DataSourceProcessor: Starting data source routine for {} downstream processors with {} upstream inputs", 
                     downstream_count, input_receivers.len());
            
            // Send stream start signal
            if result_tx.send(StreamData::stream_start()).is_err() {
                println!("DataSourceProcessor: Failed to send start signal");
                return;
            }
            
            // If we have upstream inputs, process them (for future extensions)
            // For now, DataSource typically doesn't have upstream, but we support it
            if !input_receivers.is_empty() {
                println!("DataSourceProcessor: Processing upstream inputs (unexpected for data source)");
                // Process upstream data - this is unusual for a data source but supports flexibility
                for receiver in &mut input_receivers {
                    loop {
                        tokio::select! {
                            result = receiver.recv() => {
                                match result {
                                    Ok(stream_data) => {
                                        // Check for stop signal in the data stream
                                        if utils::is_stop_signal(&stream_data) {
                                            println!("DataSourceProcessor: Received stop signal in upstream data, shutting down");
                                            break;
                                        }
                                        
                                        // Pass through upstream data (unusual for data source)
                                        if result_tx.send(stream_data).is_err() {
                                            println!("DataSourceProcessor: All downstream receivers dropped");
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        // Handle broadcast errors from upstream
                                        let error_data = utils::handle_receive_error(e);
                                        if result_tx.send(error_data).is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Traditional data source behavior - generate data
                // For now, return empty data to establish pipeline
                // In a real implementation, this would generate actual data
                let data_items: Vec<Box<dyn crate::model::Collection>> = vec![];
                
                for data in data_items {
                    // Check for stop signal before sending each item
                    // Use a timeout to periodically check for control signals
                    match tokio::time::timeout(tokio::time::Duration::from_millis(10), async {
                        // Try to receive any pending control signals
                        let mut temp_rx = result_tx.subscribe();
                        temp_rx.recv().await
                    }).await {
                        Ok(Ok(stop_signal)) if utils::is_stop_signal(&stop_signal) => {
                            println!("DataSourceProcessor: Received stop signal, shutting down");
                            break;
                        }
                        _ => {
                            // No stop signal, continue with normal processing
                        }
                    }
                    
                    if result_tx.send(StreamData::collection(data)).is_err() {
                        // All downstream receivers dropped, stop processing
                        println!("DataSourceProcessor: All downstream receivers dropped, stopping");
                        break;
                    }
                }
            }
            
            // Send stream end signal
            if result_tx.send(StreamData::stream_end()).is_err() {
                println!("DataSourceProcessor: Failed to send end signal");
            }
            
            println!("DataSourceProcessor: Data source routine completed");
        }
    }
}