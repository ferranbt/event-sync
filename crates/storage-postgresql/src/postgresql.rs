use alloy_dyn_abi::DynSolValue;
use async_trait::async_trait;
use chrono::Utc;
use deadpool_postgres::{Config, Pool, Runtime};
use std::{collections::HashMap, sync::Arc, sync::Mutex};
use storage::{
    Column, DataBatch, DataRecord, Database, DatabaseError, EventFilter, Handler, LogReference,
};
use thiserror::Error;
use tokio_postgres::Error as PgError;
use tokio_postgres::types::ToSql;
use url;

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("PostgreSQL error: {0}")]
    PostgresError(#[from] PgError),
    #[error("Config error: {0}")]
    ConfigError(#[from] deadpool_postgres::ConfigError),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Invalid connection string: {0}")]
    InvalidConnectionString(String),
    #[error("Handler already exists: {0}")]
    HandlerAlreadyExists(String),
    #[error("Handler not found: {0}")]
    HandlerNotFound(String),
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
}

impl From<HandlerError> for DatabaseError {
    fn from(err: HandlerError) -> Self {
        match err {
            HandlerError::PostgresError(e) => DatabaseError::DatabaseError(e.to_string()),
            HandlerError::ConfigError(e) => DatabaseError::DatabaseError(e.to_string()),
            HandlerError::JsonError(e) => DatabaseError::DatabaseError(e.to_string()),
            HandlerError::InvalidConnectionString(e) => DatabaseError::DatabaseError(e),
            HandlerError::HandlerAlreadyExists(s) => DatabaseError::HandlerAlreadyExists(s),
            HandlerError::HandlerNotFound(s) => DatabaseError::HandlerNotFound(s),
            HandlerError::InvalidSchema(s) => DatabaseError::InvalidSchema(s),
        }
    }
}

pub type Result<T> = std::result::Result<T, HandlerError>;

#[derive(Debug, Clone)]
pub struct Postgresql {
    pool: Pool,
    prepared_statements: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Debug, Clone)]
pub struct PostgresqlConfig {
    pub connection_string: String,
}

impl Postgresql {
    pub async fn new(config: PostgresqlConfig) -> Result<Self> {
        let mut cfg = Config::new();
        let url = url::Url::parse(&config.connection_string)
            .map_err(|e| HandlerError::InvalidConnectionString(e.to_string()))?;

        cfg.user = Some(url.username().to_string());
        if let Some(password) = url.password() {
            cfg.password = Some(password.to_string());
        }
        cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
        if let Some(host) = url.host_str() {
            cfg.host = Some(host.to_string());
        }
        if let Some(port) = url.port() {
            cfg.port = Some(port);
        }

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
            .unwrap();

        // Create handler_metadata table if it doesn't exist
        let client = pool.get().await.unwrap();
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS handler_metadata (
                id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
                &[],
            )
            .await?;

        Ok(Self {
            pool,
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn create_handler_internal(&self, event: EventFilter) -> Result<PostgresDataHandler> {
        let table_name = event.name.clone();
        let id = table_name.clone();

        let columns = event.columns();
        let now = Utc::now();
        let naive_now = now.naive_utc();

        let client = self.pool.get().await.unwrap();

        // Create the table
        let mut create_table_sql = format!("CREATE TABLE IF NOT EXISTS {} (", table_name);
        create_table_sql.push_str("event_timestamp TIMESTAMPTZ NOT NULL, ");

        for (i, col) in columns.iter().enumerate() {
            create_table_sql.push_str(&format!("{} {}", col.name, col.data_type));

            if let Some(constraints) = &col.constraints {
                for constraint in constraints {
                    create_table_sql.push_str(&format!(" {}", constraint));
                }
            }

            if i < columns.len() - 1 {
                create_table_sql.push_str(", ");
            }
        }
        create_table_sql.push_str(")");

        // Execute the create table statement
        client.execute(&create_table_sql, &[]).await?;

        // Insert the handler metadata
        client
            .execute(
                "INSERT INTO handler_metadata (id, table_name, created_at, updated_at) 
                 VALUES ($1, $2, $3, $4)",
                &[
                    &id,
                    &table_name,
                    &naive_now.to_string(),
                    &naive_now.to_string(),
                ],
            )
            .await?;

        Ok(PostgresDataHandler::new(
            id.to_string(),
            table_name.to_string(),
            columns,
            self.pool.clone(),
            Arc::clone(&self.prepared_statements),
        ))
    }
}

#[async_trait]
impl Database for Postgresql {
    type HandlerType = PostgresDataHandler;

    fn create_handler(&mut self, event: EventFilter) -> storage::Result<Box<Self::HandlerType>> {
        // Use the current_thread runtime for synchronous contexts
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.create_handler_internal(event)
                    .await
                    .map(|handler| Box::new(handler))
                    .map_err(From::from)
            })
        })
    }
}

#[derive(Debug, Clone)]
pub struct PostgresDataHandler {
    id: String,
    table_name: String,
    columns: Vec<Column>,
    pool: Pool,
    batch_buffer: Arc<Mutex<Vec<DataRecord>>>,
    last_flush: Arc<Mutex<std::time::Instant>>,
    prepared_statements: Arc<Mutex<HashMap<String, String>>>,
}

impl PostgresDataHandler {
    fn new(
        id: String,
        table_name: String,
        columns: Vec<Column>,
        pool: Pool,
        prepared_statements: Arc<Mutex<HashMap<String, String>>>,
    ) -> Self {
        Self {
            id,
            table_name,
            columns,
            pool,
            batch_buffer: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            last_flush: Arc::new(Mutex::new(std::time::Instant::now())),
            prepared_statements,
        }
    }

    async fn process_batch(&self, records: &[DataRecord]) -> Result<()> {
        let mut client = self.pool.get().await.unwrap();
        let tx = client.transaction().await?;

        let insert_sql = format!(
            "INSERT INTO {} (event_timestamp, {}) VALUES ($1, {})",
            self.table_name,
            self.columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            self.columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 2))
                .collect::<Vec<_>>()
                .join(", ")
        );

        for record in records {
            let mut params: Vec<Box<dyn ToSql + Sync>> =
                vec![Box::new(record.timestamp.unwrap().naive_utc().to_string())];

            for value in record.values.iter() {
                let param: Box<dyn ToSql + Sync> = match value {
                    DynSolValue::Bool(b) => Box::new(*b),
                    DynSolValue::Int(n, _) => Box::new(n.to_string()),
                    DynSolValue::String(s) => Box::new(s.clone()),
                    _ => {
                        todo!()
                    }
                };
                params.push(param);
            }

            let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| &**p).collect();
            tx.execute(&insert_sql, &param_refs[..]).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    fn get_or_create_insert_statement(&self, event: &EventFilter) -> Result<String> {
        let mut statements = self.prepared_statements.lock().unwrap();

        if let Some(stmt) = statements.get(&self.id) {
            return Ok(stmt.clone());
        }

        let column_names: Vec<String> = std::iter::once("event_timestamp".to_string())
            .chain(event.fields.iter().map(|f| f.name.clone()))
            .collect();

        let placeholders: Vec<String> = (1..=column_names.len())
            .map(|i| format!("${}", i))
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table_name,
            column_names.join(", "),
            placeholders.join(", ")
        );

        statements.insert(self.id.clone(), insert_sql.clone());
        Ok(insert_sql)
    }
}

const BATCH_SIZE: usize = 1000;

#[async_trait]
impl Handler for PostgresDataHandler {
    async fn get_log_at_index(&self, _number: u64) -> storage::Result<LogReference> {
        todo!()
    }

    async fn ingest(&mut self, data: DataBatch) -> storage::Result<()> {
        let mut buffer = self.batch_buffer.lock().unwrap();
        buffer.extend(data.records);

        let should_flush_size = buffer.len() >= BATCH_SIZE;
        let should_flush_time = {
            let last_flush = self.last_flush.lock().unwrap();
            last_flush.elapsed() >= std::time::Duration::from_millis(100)
        };

        if should_flush_size || should_flush_time {
            let records = std::mem::replace(&mut *buffer, Vec::with_capacity(BATCH_SIZE));
            drop(buffer);

            // Create a new runtime for this specific operation
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                self.process_batch(&records)
                    .await
                    .map_err(|e| DatabaseError::DatabaseError(e.to_string()))
            })?;

            *self.last_flush.lock().unwrap() = std::time::Instant::now();
        }

        Ok(())
    }
}
