//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use tokio::sync::mpsc;
use crate::processor::{Processor, ProcessorError, StreamData};

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier
    source_name: String,
    /// Input channels for receiving control signals
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(
        source_name: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.source_name
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        
        tokio::spawn(async move {
            loop {
                let mut all_closed = true;
                let mut received_data = false;
                
                // Check all input channels
                for input in &mut inputs {
                    match input.try_recv() {
                        Ok(data) => {
                            all_closed = false;
                            received_data = true;
                            
                            // Handle control signals
                            if let Some(control) = data.as_control() {
                                match control {
                                    crate::processor::ControlSignal::StreamEnd => {
                                        // Forward StreamEnd to outputs
                                        for output in &outputs {
                                            let _ = output.send(data.clone()).await;
                                        }
                                        return Ok(());
                                    }
                                    _ => {
                                        // Forward other control signals
                                        for output in &outputs {
                                            if output.send(data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Forward non-control data (Collection or Error)
                                for output in &outputs {
                                    if output.send(data.clone()).await.is_err() {
                                        return Err(ProcessorError::ChannelClosed);
                                    }
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            all_closed = false;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Channel disconnected
                        }
                    }
                }
                // If all channels are closed and no data received, exit
                if all_closed && !received_data {
                    return Ok(());
                }
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}
