//! Control Source processor - pipeline entry point for manual control signal injection
//! 
//! This processor acts as a pipeline entry point that can receive external control signals
//! and inject them into the processing pipeline. It serves as the starting point for
//! pipelines that need external control capabilities.
//! 
//! Now uses StreamData::Control signals for shutdown, eliminating separate stop channels.

use tokio::sync::broadcast;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData, ControlSignal};

/// ControlSource processor that serves as a pipeline entry point
/// 
/// This processor:
/// 1. Receives external control signals via a control channel
/// 2. Injects them into the processing pipeline
/// 3. Serves as the starting point for controlled pipelines
pub struct ControlSourceProcessor {
    /// External control channel for receiving control signals
    control_receiver: broadcast::Receiver<StreamData>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor with control channel
    pub fn new(
        control_receiver: broadcast::Receiver<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            control_receiver,
            downstream_count,
            processor_name: "ControlSourceProcessor".to_string(),
        }
    }
    
    /// Create a ControlSourceProcessor with a custom name
    pub fn with_name(
        name: String,
        control_receiver: broadcast::Receiver<StreamData>,
        downstream_count: usize,
    ) -> Self {
        Self {
            control_receiver,
            downstream_count,
            processor_name: name,
        }
    }
}

impl StreamProcessor for ControlSourceProcessor {
    fn start(&self) -> ProcessorView {
        // Create only result channel - no stop channel needed
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        
        // Clone the sender for the view
        let result_tx_clone = result_tx.clone();
        
        // Spawn the control source routine
        let routine = self.create_control_source_routine(result_tx);
        
        let join_handle = tokio::spawn(routine);
        
        // For ControlSourceProcessor, we use the result sender for control injection
        ProcessorView::new(
            Some(result_tx_clone),
            result_rx,
            ProcessorHandle::new(join_handle),
        )
    }
    
    fn downstream_count(&self) -> usize {
        self.downstream_count
    }
    
    fn input_receivers(&self) -> Vec<broadcast::Receiver<StreamData>> {
        // ControlSourceProcessor has no upstream inputs - it's a pipeline entry point
        Vec::new()
    }
}

// Private helper methods
impl ControlSourceProcessor {
    /// Create control source routine that runs in tokio task
    /// Uses StreamData::Control signals for shutdown instead of separate stop channel
    fn create_control_source_routine(
        &self,
        result_tx: broadcast::Sender<StreamData>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let downstream_count = self.downstream_count;
        let mut control_rx = self.control_receiver.resubscribe();
        let processor_name = self.processor_name.clone();
        
        async move {
            println!("{}: Starting control source routine for {} downstream processors", 
                     processor_name, downstream_count);
            
            // Send initial stream start signal
            if result_tx.send(StreamData::stream_start()).is_err() {
                println!("{}: Failed to send start signal", processor_name);
                return;
            }
            
            // Main processing loop - receive and forward external control signals
            loop {
                // Use select! to also monitor for stop signals from downstream
                tokio::select! {
                    // Receive external control signals
                    control_result = control_rx.recv() => {
                        match control_result {
                            Ok(control_signal) => {
                                println!("{}: Received external control signal: {:?}", 
                                         processor_name, control_signal.description());
                                
                                // Check if this is a stop signal
                                if let StreamData::Control(ControlSignal::StreamEnd) = &control_signal {
                                    println!("{}: Received stop signal, shutting down gracefully", processor_name);
                                    // Forward the stop signal and exit
                                    let _ = result_tx.send(control_signal);
                                    break;
                                }
                                
                                // Forward control signal to downstream
                                if result_tx.send(control_signal).is_err() {
                                    println!("{}: All downstream receivers dropped, stopping", processor_name);
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("{}: Control channel error: {}", processor_name, e);
                                // Send channel closed signal
                                if result_tx.send(StreamData::control(ControlSignal::StreamEnd)).is_err() {
                                    break;
                                }
                                break;
                            }
                        }
                    }
                    
                    // Monitor result channel for stop signals from downstream
                    // This allows graceful shutdown when downstream closes
                    result_check = async {
                        // Use a temporary receiver to check for stop signals
                        let mut temp_rx = result_tx.subscribe();
                        // Use timeout to avoid blocking indefinitely
                        tokio::time::timeout(tokio::time::Duration::from_millis(100), temp_rx.recv()).await
                    } => {
                        match result_check {
                            Ok(Ok(stop_signal)) => {
                                if utils::is_stop_signal(&stop_signal) {
                                    println!("{}: Detected stop signal in result channel, shutting down", processor_name);
                                    break;
                                }
                            }
                            Ok(Err(_)) | Err(_) => {
                                // No stop signal detected or timeout, continue normal operation
                            }
                        }
                    }
                }
            }
            
            println!("{}: Control source routine completed", processor_name);
        }
    }
}