use alloy::rpc::types::eth::Filter;
use alloy_dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy_json_abi::Event;
use alloy_primitives::{Address, BlockHash};
use arbitrary::Unstructured;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Handler already exists: {0}")]
    HandlerAlreadyExists(String),
    #[error("Handler not found: {0}")]
    HandlerNotFound(String),
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}

// Field is the item that it is going to be stored in the database
#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub data_type: DynSolType,
    pub indexed: bool,
    pub index: u64,
}

#[derive(Clone, Debug)]
pub struct EventFilter {
    pub name: String,
    pub address: Option<Address>,
    pub event: Event,
    pub fields: Vec<Field>,
    pub dyn_event: DynSolEvent,
}

impl EventFilter {
    pub fn with_address(mut self, address: Address) -> Self {
        self.address = Some(address);

        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;

        self
    }

    pub fn filter(&self) -> Filter {
        Filter::new().event_signature(self.event.selector())
    }

    pub fn new_arbitrary_stream<'a>(&self, u: Unstructured<'a>) -> ArbitraryStream<'a> {
        ArbitraryStream {
            u,
            block_number: 0,
            fields: self.fields.clone(),
        }
    }
}

pub struct ArbitraryStream<'a> {
    u: Unstructured<'a>,
    block_number: u64,
    fields: Vec<Field>,
}

impl<'a> ArbitraryStream<'a> {
    pub fn next(&mut self) -> arbitrary::Result<DataRecord> {
        let mut values = Vec::new();

        for field in self.fields.iter() {
            let value = match field.data_type {
                DynSolType::Bool => DynSolValue::Bool(self.u.arbitrary()?),
                DynSolType::String => DynSolValue::String(self.u.arbitrary()?),
                DynSolType::Bytes => DynSolValue::Bytes(self.u.arbitrary()?),
                _ => {
                    todo!("{}", field.data_type)
                }
            };

            values.push(value);
        }

        self.block_number += 1;

        Ok(DataRecord {
            block_number: self.block_number,
            timestamp: Some(Utc::now()),
            values,
        })
    }
}

fn remove_newlines_and_tabs(s: &str) -> String {
    s.chars().filter(|&c| c != '\n' && c != '\t').collect()
}

impl From<&str> for EventFilter {
    fn from(item: &str) -> Self {
        let event = Event::parse(remove_newlines_and_tabs(item).as_str()).unwrap();
        let dyn_event = event.resolve().unwrap();

        let mut fields = Vec::new();
        for (idx, param) in event.inputs.clone().into_iter().enumerate() {
            let data_type = DynSolType::parse(param.ty.as_str()).unwrap();

            fields.push(Field {
                name: param.name,
                data_type: data_type,
                indexed: param.indexed,
                index: idx as u64,
            });
        }

        Self {
            event: event.clone(),
            name: event.name.into(),
            address: None,
            dyn_event,
            fields,
        }
    }
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

pub struct LogReference {
    pub block_hash: BlockHash,
}

/// Trait for handling data ingestion
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn get_log_at_index(&self, index: u64) -> Result<LogReference>;

    /// Ingest a single record
    async fn ingest(&mut self, batch: DataBatch) -> Result<()>;
}

/// Trait representing a database that can create handlers
#[async_trait]
pub trait Database: Send + Sync + Clone + 'static {
    /// The specific type of handler this database creates
    type HandlerType: Handler;

    /// Create a new handler with the given schema and batch configuration
    fn create_handler(&mut self, event: EventFilter) -> Result<Box<Self::HandlerType>>;
}

impl From<HandlerError> for DatabaseError {
    fn from(err: HandlerError) -> Self {
        match err {
            HandlerError::DatabaseError(e) => DatabaseError::DatabaseError(e.to_string()),
            HandlerError::HandlerAlreadyExists(s) => DatabaseError::HandlerAlreadyExists(s),
            HandlerError::HandlerNotFound(s) => DatabaseError::HandlerNotFound(s),
            HandlerError::InvalidSchema(s) => DatabaseError::InvalidSchema(s),
            HandlerError::JsonError(e) => DatabaseError::JsonError(e),
        }
    }
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Handler already exists: {0}")]
    HandlerAlreadyExists(String),
    #[error("Handler not found: {0}")]
    HandlerNotFound(String),
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}

// Define a record for the handler to process
#[derive(Debug)]
pub struct DataRecord {
    pub block_number: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub values: Vec<DynSolValue>,
}

#[derive(Debug)]
pub struct DataBatch {
    pub records: Vec<DataRecord>,
}

// Define a column in a table
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DynSolType,
    pub constraints: Option<Vec<String>>,
}

impl EventFilter {
    pub fn columns(&self) -> Vec<Column> {
        let mut columns = Vec::new();

        for param in self.event.inputs.clone() {
            let data_type = DynSolType::parse(param.ty.as_str()).unwrap();

            columns.push(Column {
                name: param.name,
                data_type: data_type,
                constraints: None,
            });
        }

        columns
    }
}
