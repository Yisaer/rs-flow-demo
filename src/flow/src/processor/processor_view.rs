//! Processor view and handle for controlling stream processors
//! 
//! Provides control mechanisms for starting, stopping and monitoring stream processors.
//! 
//! Redesigned to use StreamData::Control signals for all control operations,
//! eliminating the need for separate stop channels.

use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use crate::processor::{StreamData, ControlSignal};

/// View for controlling and monitoring a stream processor
/// 
/// Now uses StreamData::Control signals for all control operations,
/// providing a unified communication channel for both data and control.
#[derive(Debug)]
pub struct ProcessorView {
    /// Sender for injecting control signals into the processor
    pub control_sender: Option<broadcast::Sender<StreamData>>,
    /// Channel for receiving processed results and control signals (unified)
    pub result_receiver: broadcast::Receiver<StreamData>,
    /// Handle to the processor task
    pub task_handle: ProcessorHandle,
}

/// Handle to a running processor task
#[derive(Debug)]
pub struct ProcessorHandle {
    /// The tokio task handle
    pub join_handle: JoinHandle<()>,
}

impl ProcessorHandle {
    /// Create a new processor handle
    pub fn new(join_handle: JoinHandle<()>) -> Self {
        Self { join_handle }
    }
    
    /// Wait for the processor task to complete
    pub async fn wait(self) -> Result<(), tokio::task::JoinError> {
        self.join_handle.await
    }
    
    /// Abort the processor task
    pub fn abort(&self) {
        self.join_handle.abort();
    }
}

impl ProcessorView {
    /// Create a new processor view
    pub fn new(
        control_sender: Option<broadcast::Sender<StreamData>>,
        result_receiver: broadcast::Receiver<StreamData>,
        task_handle: ProcessorHandle,
    ) -> Self {
        Self {
            control_sender,
            result_receiver,
            task_handle,
        }
    }
    
    /// Create a processor view with only result receiver (for backward compatibility)
    pub fn from_result_receiver(
        result_receiver: broadcast::Receiver<StreamData>,
        task_handle: ProcessorHandle,
    ) -> Self {
        Self {
            control_sender: None,
            result_receiver,
            task_handle,
        }
    }
    
    /// Send stop signal to the processor using StreamData::Control
    pub fn stop(&self) -> Result<usize, broadcast::error::SendError<StreamData>> {
        if let Some(ref sender) = self.control_sender {
            let stop_signal = StreamData::control(ControlSignal::StreamEnd);
            sender.send(stop_signal)
        } else {
            // For processors without control sender, we can't send stop signals
            // Return Ok(0) to indicate no action was taken
            Ok(0)
        }
    }
    
    /// Send a control signal to the processor
    pub fn send_control(&self, signal: ControlSignal) -> Result<usize, broadcast::error::SendError<StreamData>> {
        if let Some(ref sender) = self.control_sender {
            let control_data = StreamData::control(signal);
            sender.send(control_data)
        } else {
            // For processors without control sender, we can't send control signals
            Ok(0)
        }
    }
    
    /// Get a new receiver for the result channel
    pub fn result_resubscribe(&self) -> broadcast::Receiver<StreamData> {
        self.result_receiver.resubscribe()
    }
    
    /// Check if the processor is still running
    pub fn is_running(&self) -> bool {
        !self.task_handle.join_handle.is_finished()
    }
    
    /// Get the number of active downstream receivers
    pub fn active_receiver_count(&self) -> usize {
        self.result_receiver.len()
    }
    
    /// Send a flush signal to the processor
    pub fn flush(&self) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.send_control(ControlSignal::Flush)
    }
    
    /// Send a backpressure signal to the processor
    pub fn backpressure(&self) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.send_control(ControlSignal::Backpressure)
    }
    
    /// Send a resume signal to the processor
    pub fn resume(&self) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.send_control(ControlSignal::Resume)
    }
}