use crate::{DataBatch, Database, EventFilter, Handler, LogReference, Result};
use async_trait::async_trait;

#[derive(Clone)]
pub struct StdoutHandler {}

impl StdoutHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Database for StdoutHandler {
    type HandlerType = StdoutHandler;

    fn create_handler(&mut self, _event: EventFilter) -> Result<Box<Self::HandlerType>> {
        Ok(Box::new(StdoutHandler {}))
    }
}

#[async_trait]
impl Handler for StdoutHandler {
    async fn get_log_at_index(&self, _index: u64) -> Result<LogReference> {
        todo!()
    }

    async fn ingest(&mut self, data: DataBatch) -> Result<()> {
        for record in data.records {
            println!(
                "{}: {:?} {:?}",
                record.block_number, record.timestamp, record.values
            );
        }

        Ok(())
    }
}
