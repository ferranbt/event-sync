use alloy_primitives::Address;
use clap::{Parser, command};
use indexer::{EthProviderImpl, Scheduler};
use storage::StdoutHandler;
use storage::{Database, EventFilter};

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
pub struct Cli {
    pub rpc_url: String,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let rpc_url = &cli.rpc_url;
    let provider = EthProviderImpl::from(rpc_url);

    let system = Scheduler::new(provider);
    system.spawn();

    let mut event: EventFilter = "event DepositEvent(
        bytes pubkey,
        bytes withdrawal_credentials,
        bytes amount,
        bytes signature,
        bytes index0,
    )"
    .into();
    event = event.with_name("deposit".to_string());
    event = event.with_address(
        "0xAc4b3DacB91461209Ae9d41EC517c2B9Cb1B7DAF"
            .parse::<Address>()
            .unwrap(),
    );

    let mut db = StdoutHandler::new();
    let handler = db.create_handler(event.clone())?;

    system.track(event, handler)?;

    // sleep for 20 seconds
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;

    Ok(())
}
