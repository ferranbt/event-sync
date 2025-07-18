use arbitrary::Unstructured;
use storage::{DataBatch, Database, EventFilter, Handler};
use storage_sqlite::{Sqlite, SqliteConfig};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let handler_system = Sqlite::new(SqliteConfig {
        db_path: "./db.sqlite".to_string(),
    })
    .unwrap();

    let event = EventFilter::from(
        "event DepositEvent(
        bytes pubkey,
        string name,
        )",
    );

    let num_handlers = 1;
    let records_to_insert = 10000;

    let mut handlers = Vec::new();

    for i in 0..num_handlers {
        let event = event.clone().with_name(format!("test_{}", i));
        let handler = handler_system.clone().create_handler(event).unwrap();

        handlers.push(handler);
    }

    let start = std::time::Instant::now();

    // Spawn concurrent tasks for each handler
    let handles: Vec<_> = handlers
        .into_iter()
        .map(|mut handler| {
            let event = event.clone();

            tokio::spawn(async move {
                let mut data = vec![0u8; 1000000000];
                getrandom::getrandom(&mut data).unwrap();

                let u = Unstructured::new(&data);
                let mut stream = event.new_arbitrary_stream(u);

                for _ in 0..records_to_insert {
                    let record = stream.next().unwrap();

                    handler
                        .ingest(DataBatch {
                            records: vec![record],
                        })
                        .await
                        .unwrap();
                }
            })
        })
        .collect();

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let total_records = records_to_insert * num_handlers;
    let records_per_second = total_records as f64 / duration.as_secs_f64();
    println!(
        "Handlers: {}, Inserted {} records in {:.2} seconds ({:.2} records/second)",
        num_handlers,
        total_records,
        duration.as_secs_f64(),
        records_per_second
    );

    Ok(())
}
