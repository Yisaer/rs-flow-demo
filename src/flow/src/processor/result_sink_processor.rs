//! Result Sink processor - pipeline exit point for collecting processed data
//! 
//! This processor acts as a pipeline exit point that collects final processed data
//! and forwards it to external consumers via a result channel.
//! 
//! Now uses StreamData::Control signals for shutdown, eliminating separate stop channels.

use tokio::sync::broadcast;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// ResultSink processor that serves as a pipeline exit point
/// 
/// This processor:
/// 1. Receives processed data from upstream processors
/// 2. Forwards results to external consumers via a result channel
/// 3. Handles pipeline completion and cleanup
pub struct ResultSinkProcessor {
    /// Input channels from upstream processors
    input_receivers: Vec<broadcast::Receiver<StreamData>>,
    /// Result channel for forwarding processed data to external consumers
    result_sender: broadcast::Sender<StreamData>,
    /// Number of downstream processors (typically 0 for sink)
    downstream_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl ResultSinkProcessor {
    /// Create a new ResultSinkProcessor
    pub fn new(
        input_receivers: Vec<broadcast::Receiver<StreamData>>,
        result_sender: broadcast::Sender<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            input_receivers,
            result_sender,
            downstream_count,
            processor_name: "ResultSinkProcessor".to_string(),
        }
    }
    
    /// Create a ResultSinkProcessor with single upstream input
    pub fn with_single_upstream(
        upstream_receiver: broadcast::Receiver<StreamData>,
        result_sender: broadcast::Sender<StreamData>,
    ) -> Self {
        Self::new(vec![upstream_receiver], result_sender, 0)
    }
    
    /// Create a ResultSinkProcessor with a name for debugging
    pub fn with_name(
        name: String,
        input_receivers: Vec<broadcast::Receiver<StreamData>>,
        result_sender: broadcast::Sender<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            input_receivers,
            result_sender,
            downstream_count,
            processor_name: name,
        }
    }
}

impl StreamProcessor for ResultSinkProcessor {
    fn start(&self) -> ProcessorView {
        // Create only result channel - no stop channel needed
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        
        // Clone the sender for the view (for external forwarding)
        let result_sender_clone = self.result_sender.clone();
        
        // Spawn the result sink routine
        let routine = self.create_result_sink_routine(result_tx);
        
        let join_handle = tokio::spawn(routine);
        
        // For ResultSinkProcessor, we use the result sender for external forwarding
        ProcessorView::new(
            Some(result_sender_clone),
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
impl ResultSinkProcessor {
    /// Create result sink routine that runs in tokio task
    /// Uses StreamData::Control signals for shutdown instead of separate stop channel
    fn create_result_sink_routine(
        &self,
        _result_tx: broadcast::Sender<StreamData>, // Internal result channel (not used for external forwarding)
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut input_receivers = self.input_receivers();
        let result_sender = self.result_sender.clone();
        let processor_name = self.processor_name.clone();
        
        async move {
            println!("{}: Starting result sink routine with {} upstream inputs", processor_name, input_receivers.len());
            
            // Main collection loop
            loop {
                // Receive processed data from upstream
                let upstream_result = async {
                    if let Some(receiver) = input_receivers.get_mut(0) {
                        receiver.recv().await
                    } else {
                        // No upstream input, wait indefinitely
                        futures::future::pending::<Result<StreamData, broadcast::error::RecvError>>().await
                    }
                };
                
                match upstream_result.await {
                    Ok(stream_data) => {
                        println!("{}: Received processed data: {:?}", processor_name, stream_data.description());
                        
                        // Check if this is a terminal signal
                        let is_terminal = stream_data.is_terminal();
                        
                        // Forward to external consumers (clone to avoid move)
                        if result_sender.send(stream_data.clone()).is_err() {
                            println!("{}: No external consumers remaining, continuing to collect", processor_name);
                            // Continue collecting even if no external consumers
                            // This prevents data loss in the pipeline
                        }
                        
                        if is_terminal {
                            println!("{}: Received terminal signal, completing collection", processor_name);
                            break;
                        }
                    }
                    Err(e) => {
                        // Handle broadcast errors from upstream
                        let error_data = utils::handle_receive_error(e);
                        println!("{}: Upstream error: {:?}", processor_name, error_data.description());
                        
                        // Forward error to external consumers
                        let _ = result_sender.send(error_data);
                    }
                }
            }
            
            println!("{}: Result sink routine completed", processor_name);
        }
    }
}