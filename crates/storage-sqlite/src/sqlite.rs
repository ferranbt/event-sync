use alloy_dyn_abi::{DynSolType, DynSolValue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{
    SinkExt, StreamExt,
    channel::mpsc::{Receiver, Sender, channel},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, sync::Mutex};
use storage::{DataBatch, EventFilter};
use storage::{DataRecord, Database, Handler, HandlerError, LogReference};

pub type Result<T> = std::result::Result<T, HandlerError>;

// Handler metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerMetadata {
    pub table_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// The handler system
#[derive(Debug, Clone)]
pub struct Sqlite {
    conn: Arc<Mutex<Connection>>,
    handlers: HashMap<String, HandlerMetadata>,
    prepared_statements: Arc<Mutex<HashMap<String, String>>>, // Cache the INSERT statements
}

#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub db_path: String,
}

impl Sqlite {
    pub fn new(config: SqliteConfig) -> Result<Self> {
        let conn = Connection::open(config.db_path).unwrap();

        // Enable WAL mode for better concurrency
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();

        // Increase cache size for better performance
        conn.pragma_update(None, "cache_size", -10000).unwrap();

        // Create handler_metadata table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS handler_metadata (
                id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
            [],
        )
        .unwrap();

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            handlers: HashMap::new(),
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn create_handler_2(&mut self, event: EventFilter) -> Result<DataHandlerChannel> {
        let table_name = event.name.clone();

        let columns = event.columns();
        let now = Utc::now();

        // Check if handler already exists
        if self.handlers.contains_key(&table_name) {
            return Err(HandlerError::HandlerAlreadyExists(table_name.clone()));
        }

        // Create the table
        let mut create_table_sql = format!("CREATE TABLE IF NOT EXISTS {} (", table_name);

        // Add timestamp column
        create_table_sql.push_str("event_timestamp TEXT NOT NULL, ");

        println!("{}", columns.len());

        // Add the columns
        for (i, col) in columns.iter().enumerate() {
            println!("{}", col.name);
            // Convert PostgreSQL types to SQLite compatible types
            let sqlite_type = match col.data_type {
                DynSolType::Bool => "INTEGER",
                DynSolType::Int(_) => "INTEGER",
                DynSolType::Uint(_) => "INTEGER",
                DynSolType::FixedBytes(_) => "BLOB",
                DynSolType::Address => "TEXT",
                DynSolType::Function => "TEXT",
                DynSolType::Bytes => "BLOB",
                DynSolType::String => "TEXT",
                DynSolType::Array(_) => "TEXT",
                DynSolType::FixedArray(_, _) => "TEXT",
                _ => {
                    return Err(HandlerError::InvalidSchema(format!(
                        "Unsupported type: {:?}",
                        col.data_type
                    )));
                }
            };

            create_table_sql.push_str(&format!("{} {}", col.name, sqlite_type));

            // Add constraints if any
            if let Some(constraints) = &col.constraints {
                for constraint in constraints {
                    // Skip PostgreSQL-only constraints
                    if !constraint.to_uppercase().contains("USING") {
                        create_table_sql.push_str(&format!(" {}", constraint));
                    }
                }
            }

            if i < columns.len() - 1 {
                create_table_sql.push_str(", ");
            }
        }

        create_table_sql.push_str(")");

        // Execute the create table statement
        self.conn
            .lock()
            .expect("Failed to lock connection")
            .execute(&create_table_sql, [])
            .unwrap();

        // Insert the handler metadata
        let now_str = now.to_rfc3339();
        self.conn.lock().expect("Failed to lock connection").execute(
            "INSERT INTO handler_metadata (table_name, created_at, updated_at) VALUES (?1, ?2, ?3)",
            params![table_name, now_str, now_str],
        ).unwrap();

        // Add to the handlers map
        let metadata = HandlerMetadata {
            table_name: table_name.to_string(),
            created_at: now,
            updated_at: now,
        };

        self.handlers
            .insert(table_name.to_string(), metadata.clone());

        // Create and return the handler
        let handler = DataHandler::new(Arc::new(Mutex::new(self.clone())), event);

        let (tx, rx) = channel(16);
        let handler_channel = DataHandlerChannel { channel: tx };

        let _ = tokio::spawn(async move {
            handler.run(rx).await;
        });

        Ok(handler_channel)
    }

    fn get_or_create_insert_statement(
        &self,
        event: &EventFilter,
        table_name: &str,
    ) -> Result<String> {
        let mut statements = self
            .prepared_statements
            .lock()
            .expect("Failed to lock statements");

        if let Some(stmt) = statements.get(table_name) {
            return Ok(stmt.clone());
        }

        // Create the INSERT statement
        let column_names: Vec<String> = std::iter::once("event_timestamp".to_string())
            .chain(event.fields.iter().map(|f| f.name.clone()))
            .collect();

        let placeholders: Vec<String> = (1..=column_names.len())
            .map(|i| format!("?{}", i))
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            placeholders.join(", ")
        );

        statements.insert(table_name.to_string(), insert_sql.clone());
        Ok(insert_sql)
    }

    fn process_batch(&mut self, event: &EventFilter, batch: &[DataRecord]) -> Result<()> {
        let table_name = event.name.clone();

        if batch.is_empty() {
            return Ok(());
        }

        let handler = match self.handlers.get(&table_name) {
            Some(h) => h,
            None => return Err(HandlerError::HandlerNotFound(table_name.to_string())),
        };

        let mut conn = self.conn.lock().expect("Failed to lock connection");

        // Begin transaction
        let tx = conn.transaction().unwrap();

        // Get or create the prepared statement
        let insert_sql = self.get_or_create_insert_statement(&event, &handler.table_name)?;
        {
            let mut stmt = tx.prepare(&insert_sql).unwrap();

            // Process each record in the batch
            for record in batch {
                let timestamp = record.timestamp.unwrap().to_rfc3339();
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(timestamp)];

                // Add values for each column in order
                for value in record.values.iter() {
                    let param: Box<dyn rusqlite::ToSql> = match value {
                        DynSolValue::Bool(b) => Box::new(*b),
                        DynSolValue::Int(n, _) => Box::new(n.as_i64()),
                        DynSolValue::String(s) => Box::new(s.clone()),
                        DynSolValue::Bytes(b) => Box::new(b.clone()),
                        _ => {
                            todo!("{:?}", value)
                        }
                    };
                    params.push(param);
                }

                let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p).collect();
                stmt.execute(param_refs.as_slice()).unwrap();
            }
        } // stmt is dropped here

        // Commit the transaction
        tx.commit().unwrap();

        Ok(())
    }
}

#[async_trait]
impl Database for Sqlite {
    type HandlerType = DataHandlerChannel;

    fn create_handler(&mut self, event: EventFilter) -> storage::Result<Box<Self::HandlerType>> {
        self.create_handler_2(event)
            .map(Box::new)
            .map_err(From::from)
    }
}

#[derive(Debug, Clone)]
// A data handler that users will interact with
pub struct DataHandler {
    system: Arc<Mutex<Sqlite>>,
    event: EventFilter,
}

const BATCH_SIZE: usize = 1000;

impl DataHandler {
    pub fn new(system: Arc<Mutex<Sqlite>>, event: EventFilter) -> Self {
        Self { system, event }
    }

    pub async fn run(&self, mut rx: Receiver<DataBatch>) {
        let mut buffer = Vec::new();
        let mut last_flush = std::time::Instant::now();

        while let Some(values) = rx.next().await {
            for record in values.records {
                buffer.push(record);
            }

            // Check if we should flush based on size or time
            let should_flush_size = buffer.len() >= BATCH_SIZE;
            let should_flush_time = last_flush.elapsed() >= std::time::Duration::from_millis(100);

            if should_flush_size || should_flush_time {
                let records = std::mem::replace(&mut buffer, Vec::with_capacity(BATCH_SIZE));

                if !records.is_empty() {
                    let mut system = self.system.lock().expect("Failed to lock handler system");
                    system.process_batch(&self.event, &records).unwrap();
                }
                last_flush = std::time::Instant::now();
            }
        }
    }
}

pub struct DataHandlerChannel {
    pub channel: Sender<DataBatch>,
}

#[async_trait]
impl Handler for DataHandlerChannel {
    async fn get_log_at_index(&self, _number: u64) -> storage::Result<LogReference> {
        todo!()
    }

    /// Ingest a single record
    async fn ingest(&mut self, data: DataBatch) -> storage::Result<()> {
        self.channel.send(data).await.unwrap();
        Ok(())
    }
}
