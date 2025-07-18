use alloy::providers::{Provider, RootProvider};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::eth::BlockNumberOrTag;
use alloy::rpc::types::eth::Filter;
use alloy::rpc::types::{
    Block, BlockNumHash, BlockTransactions, BlockTransactionsKind, Header, Log,
};
use alloy::transports::layers::{RetryBackoffLayer, RetryPolicy};
use alloy::transports::TransportResult;

use alloy::consensus::Header as ConsensusHeader;
use alloy_primitives::{Address, BlockHash, BlockNumber, Bloom, Bytes, B256, B64, U256};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashSet;
use storage::EventFilter;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

pub struct Scheduler<Provider> {
    provider: Provider,
    notification_sender: broadcast::Sender<Notification>,
}

impl<Provider> Scheduler<Provider> {
    pub fn new(provider: Provider) -> Self {
        let (notification_sender, _receiver) = tokio::sync::broadcast::channel(50);

        Scheduler {
            provider,
            notification_sender,
        }
    }
}

impl<Provider> Scheduler<Provider>
where
    Provider: EthProvider + Clone + Send + Sync + 'static,
{
    pub fn track(&self, event: EventFilter, _sink: Box<dyn storage::Handler>) -> eyre::Result<()> {
        let filter = event.filter();
        let provider = self.provider.clone();

        let mut rx = self.notification_sender.subscribe();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(_notification) => {
                        println!("Received notification");
                    }
                    Err(RecvError::Lagged(_)) => {
                        println!("Lagged");
                    }
                    Err(RecvError::Closed) => {
                        return;
                    }
                };
            }
        });

        let _handle = tokio::spawn(async move {
            loop {
                sync_batch(provider.clone(), "", filter.clone(), 0, 1000000)
                    .await
                    .unwrap();
            }
        });

        Ok(())
    }

    pub fn spawn(&self) {
        let mut blockchain = Blockchain::new(self.provider.clone());
        let provider = self.provider.clone();
        let sender = self.notification_sender.clone();

        // spawn a thread to keep track of reorgs on the chain
        tokio::spawn(async move {
            loop {
                let block = match provider
                    .get_block_by_number(BlockNumberOrTag::Latest, false)
                    .await
                {
                    Ok(block) => block,
                    Err(e) => {
                        tracing::error!("Failed to get latest block: {:?}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

                if let Some(block) = block {
                    let notification = blockchain.handle_new_block(block).await.unwrap();
                    match notification {
                        InsertPayloadOk::Inserted(notification) => {
                            tracing::info!(
                                "Inserted block: {:?}",
                                notification.head_block().header.number
                            );

                            match sender.send(notification) {
                                Ok(_) => {}
                                Err(_) => {}
                            }
                        }
                        InsertPayloadOk::AlreadyTip => {
                            tracing::debug!("Already at tip");
                        }
                        _ => {}
                    };
                }

                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
struct FilterStatus {
    last_block: u64,
}

pub trait EthProvider {
    fn get_block_by_number(
        &self,
        number: BlockNumberOrTag,
        full: bool,
    ) -> impl std::future::Future<Output = TransportResult<Option<Block>>> + Send;

    fn get_block_by_hash(
        &self,
        hash: BlockHash,
        full: bool,
    ) -> impl std::future::Future<Output = TransportResult<Option<Block>>> + Send;

    fn get_logs(
        &self,
        filter: &Filter,
    ) -> impl std::future::Future<Output = TransportResult<Vec<Log>>> + Send;
}

#[derive(Debug, Clone)]
pub struct CustomRetryPolicy;

impl RetryPolicy for CustomRetryPolicy {
    fn should_retry(&self, _err: &alloy::transports::TransportError) -> bool {
        true
    }

    fn backoff_hint(
        &self,
        _error: &alloy::transports::TransportError,
    ) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_millis(100))
    }
}

#[derive(Clone)]
pub struct EthProviderImpl {
    provider: RootProvider,
}

impl EthProviderImpl {
    pub fn from(url: &str) -> Self {
        let retry_layer = RetryBackoffLayer::new_with_policy(10, 100, 10000, CustomRetryPolicy);

        let client = RpcClient::builder()
            .layer(retry_layer)
            .http(url.parse().unwrap());

        let provider = RootProvider::new(client);
        Self { provider }
    }
}

impl EthProvider for EthProviderImpl {
    async fn get_block_by_number(
        &self,
        number: BlockNumberOrTag,
        full: bool,
    ) -> TransportResult<Option<Block>> {
        self.provider
            .get_block_by_number(number)
            .kind(BlockTransactionsKind::from(full))
            .await
    }

    async fn get_block_by_hash(
        &self,
        hash: BlockHash,
        full: bool,
    ) -> TransportResult<Option<Block>> {
        self.provider
            .get_block_by_hash(hash)
            .kind(BlockTransactionsKind::from(full))
            .await
    }

    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        self.provider.get_logs(filter).await
    }
}

struct Blockchain<Provider> {
    // blocks contains a list with all the blocks by its hash
    blocks: BTreeMap<BlockHash, Block>,

    // blocks_by_number maps the block number to a set of block hashes
    blocks_by_number: BTreeMap<u64, HashSet<BlockHash>>,

    // provider is a reference to the jsonrpc provider
    provider: Provider,

    // latest_header is the latest recorded block header
    latest_header: Option<BlockNumHash>,
}

#[derive(Debug)]
enum InsertPayloadOk {
    Inserted(Notification),
    AlreadyTip,
}

#[derive(Debug)]
enum InsertError {
    TooLow,
    TooFarBack,
}

impl<Provider> Blockchain<Provider> {
    fn new(provider: Provider) -> Self {
        Blockchain {
            blocks: BTreeMap::new(),
            blocks_by_number: BTreeMap::new(),
            provider: provider,
            latest_header: None,
        }
    }
}

impl<Provider> Blockchain<Provider>
where
    Provider: EthProvider,
{
    async fn handle_new_block(&mut self, block: Block) -> Result<InsertPayloadOk, InsertError> {
        let latest_header = if let Some(latest_header) = self.latest_header {
            latest_header
        } else {
            self.latest_header = Some(BlockNumHash::new(block.header.number, block.header.hash));
            self.blocks.insert(block.header.hash, block.clone());

            return Ok(InsertPayloadOk::Inserted(Notification::ChainCommitted {
                new: Chain::from_block(block),
            }));
        };

        // check if the new block is the same as the latest one.
        if block.header.hash == latest_header.hash {
            return Ok(InsertPayloadOk::AlreadyTip);
        }

        // check if the new block goes on top of the current chain
        if block.header.parent_hash == latest_header.hash {
            let new_header = BlockNumHash::new(block.header.number, block.header.hash);

            self.latest_header = Some(new_header);
            self.blocks.insert(block.header.hash, block.clone());

            return Ok(InsertPayloadOk::Inserted(Notification::ChainCommitted {
                new: Chain::from_block(block),
            }));
        };

        if block.header.number < latest_header.number {
            // we do not alloy updates of a lower block that currently exists
            return Err(InsertError::TooLow);
        }

        let mut old_chain_head = self.blocks.get(&latest_header.hash).unwrap().clone();
        let mut new_chain_head = block;
        let mut new_chain = Chain::from_block(new_chain_head.clone());

        // Try to fill the gaps between remote chain (advanced) and local chain
        loop {
            new_chain_head = self
                .get_block_by_hash(new_chain_head.header.parent_hash)
                .await
                .unwrap();

            if new_chain_head.header.number <= old_chain_head.header.number {
                break;
            }

            // extend the chain
            new_chain.extend(new_chain_head.clone());
        }

        if new_chain_head.header.hash == old_chain_head.header.hash {
            // we filled the gap between the local head we had and the update
            return Ok(InsertPayloadOk::Inserted(Notification::ChainCommitted {
                new: new_chain,
            }));
        }

        // Since it is a reorg of the chain add to the new chain this block
        new_chain.extend(new_chain_head.clone());

        // At this point, there is a reorg hapenning
        let mut old_chain = Chain::from_block(old_chain_head.clone());

        loop {
            new_chain_head = self
                .get_block_by_hash(new_chain_head.header.parent_hash)
                .await
                .unwrap();

            old_chain_head = self
                .get_block_by_hash(old_chain_head.header.parent_hash)
                .await
                .unwrap();

            if new_chain_head.header.hash == old_chain_head.header.hash {
                break;
            }

            old_chain.extend(old_chain_head.clone());
            new_chain.extend(new_chain_head.clone());
        }

        Ok(InsertPayloadOk::Inserted(Notification::ChainReorged {
            old: old_chain,
            new: new_chain,
        }))
    }

    async fn get_block_by_hash(&mut self, block_hash: BlockHash) -> Option<Block> {
        // Check if the block is in the local cache
        if let Some(block) = self.blocks.get(&block_hash) {
            return Some(block.clone());
        }

        // If not in cache, fetch it from the provider
        match self.provider.get_block_by_hash(block_hash, false).await {
            Ok(Some(block)) => {
                // Store the block in the local cache
                self.blocks.insert(block_hash, block.clone());

                // Also store it in blocks_by_number to be removed later
                let block_number = block.header.number;
                self.blocks_by_number
                    .entry(block_number)
                    .or_default()
                    .insert(block_hash);

                Some(block.clone())
            }
            Ok(None) => None,
            Err(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
struct Chain {
    blocks: BTreeMap<BlockNumber, Block>,
}

impl Chain {
    fn from_block(block: Block) -> Self {
        let mut blocks = BTreeMap::new();
        blocks.insert(block.header.number, block);

        Chain { blocks }
    }

    // Convert blocks to a Vec
    fn blocks_as_vec(&self) -> Vec<Block> {
        self.blocks.values().cloned().collect()
    }

    fn extend(&mut self, block: Block) {
        self.blocks.insert(block.header.number, block);
    }
}

#[derive(Debug, Clone)]
enum Notification {
    /// Chain got committed without a reorg, and only the new chain is returned.
    ChainCommitted {
        /// The new chain after commit.
        new: Chain,
    },
    /// Chain got reorged, and both the old and the new chains are returned.
    ChainReorged {
        /// The old chain before reorg.
        old: Chain,
        /// The new chain after reorg.
        new: Chain,
    },
}

impl Notification {
    pub fn reverted(&self) -> Option<Chain> {
        match self {
            Self::ChainCommitted { .. } => None,
            Self::ChainReorged { old, .. } => Some(old.clone()),
        }
    }

    pub fn committed(&self) -> Chain {
        match self {
            Self::ChainCommitted { new } => new.clone(),
            Self::ChainReorged { new, .. } => new.clone(),
        }
    }

    pub fn last(&self) -> Block {
        match self {
            Self::ChainCommitted { new } => new.blocks.values().last().unwrap().clone(),
            Self::ChainReorged { new, .. } => new.blocks.values().last().unwrap().clone(),
        }
    }

    pub fn head_block(&self) -> Block {
        match self {
            Self::ChainCommitted { new } => new.blocks.values().last().unwrap().clone(),
            Self::ChainReorged { new, .. } => new.blocks.values().last().unwrap().clone(),
        }
    }
}

enum StartFilter {
    LatestBlock,
    BlockNumber(u64),
}

const BATCH_SIZE: u64 = 1000;

async fn sync_batch<T>(provider: T, name: &str, filter: Filter, from: u64, to: u64) -> Result<()>
where
    T: EthProvider,
{
    println!("Syncing batch from {} to {}", from, to);

    let additive_factor: u64 = ((BATCH_SIZE as f64) * 0.10) as u64;
    let mut size: u64 = BATCH_SIZE;
    let mut indx = from;

    loop {
        let target = if indx + size > to { to } else { indx + size };
        let batch_filter: Filter = filter.clone().from_block(indx).to_block(target);

        println!("Fetching logs from {} to {}", indx, target);

        match provider.get_logs(&batch_filter).await {
            Ok(logs) => {
                // additive increasing
                size = size + additive_factor as u64;
                if size > BATCH_SIZE {
                    size = BATCH_SIZE;
                }

                let new_status = FilterStatus { last_block: target };

                println!(
                    "Upserting status for filter: {:?} {:?} {:?}",
                    name,
                    new_status.last_block,
                    logs.len(),
                );

                if target == to {
                    return Ok(());
                }
                indx = indx + size;

                if logs.len() == 0 {
                    continue;
                }
            }
            Err(e) => {
                tracing::error!("Failed to get logs {:?}", e);

                // multiplicative decreasing
                size = size / 2;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::sync::broadcast;

    struct MockChain {
        blocks: BTreeMap<BlockHash, Block>,
        next_increment: u64,
    }

    impl MockChain {
        fn new() -> Self {
            Self {
                blocks: BTreeMap::new(),
                next_increment: 0,
            }
        }

        fn genesis(&mut self) -> Block {
            let block = MockChain::mock_block(B256::default(), B256::default(), 0);

            // add block to cache
            self.blocks.insert(block.header.hash, block.clone());

            block
        }

        fn new_block(&mut self, parent: Block) -> Block {
            // get the number of the parent to derive the number of this block
            let number = self.blocks.get(&parent.header.hash).unwrap().header.number;

            self.next_increment += 1;

            let hash = B256::with_last_byte(self.next_increment as u8);
            let number = number + 1;
            let block = MockChain::mock_block(parent.header.hash, hash, number);

            // add block to cache
            self.blocks.insert(block.header.hash, block.clone());

            block
        }

        fn mock_block(parent_hash: B256, hash: B256, number: u64) -> Block {
            Block {
                header: Header {
                    hash: hash,
                    inner: ConsensusHeader {
                        parent_hash: parent_hash,
                        ommers_hash: B256::with_last_byte(3),
                        beneficiary: Address::with_last_byte(4),
                        state_root: B256::with_last_byte(5),
                        transactions_root: B256::with_last_byte(6),
                        receipts_root: B256::with_last_byte(7),
                        withdrawals_root: Some(B256::with_last_byte(8)),
                        number: number,
                        gas_used: 0,
                        gas_limit: 0,
                        extra_data: Bytes::from(vec![1, 2, 3]),
                        logs_bloom: Bloom::default(),
                        timestamp: 0,
                        difficulty: U256::from(0),
                        mix_hash: B256::with_last_byte(14),
                        nonce: B64::with_last_byte(15),
                        base_fee_per_gas: Some(20),
                        blob_gas_used: None,
                        excess_blob_gas: None,
                        parent_beacon_block_root: None,
                        requests_hash: None,
                    },
                    size: Some(U256::from(19)),
                    total_difficulty: Some(U256::from(0)),
                },
                uncles: vec![],
                transactions: BlockTransactions::Hashes(vec![]),
                withdrawals: None,
            }
        }
    }

    impl EthProvider for MockChain {
        async fn get_block_by_number(
            &self,
            _number: BlockNumberOrTag,
            _full: bool,
        ) -> TransportResult<Option<Block>> {
            todo!()
        }

        async fn get_block_by_hash(
            &self,
            hash: BlockHash,
            _full: bool,
        ) -> TransportResult<Option<Block>> {
            Ok(self.blocks.get(&hash).cloned())
        }

        async fn get_logs(&self, _filter: &Filter) -> TransportResult<Vec<Log>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_update_latest_header() {
        let mut mock_chain = MockChain::new();
        let gen = mock_chain.genesis();

        let mut blockchain = Blockchain::new(mock_chain);
        let notification = match blockchain.handle_new_block(gen.clone()).await.unwrap() {
            InsertPayloadOk::Inserted(notification) => notification,
            _ => panic!("Expected InsertPayloadOk::Inserted"),
        };

        let _committed = notification.committed();
        //assert_eq!(committed.blocks.len(), 1);
        //assert_eq!(committed.blocks_as_vec(), vec![gen.clone()]);

        match blockchain.handle_new_block(gen.clone()).await.unwrap() {
            InsertPayloadOk::AlreadyTip => {}
            _ => panic!("Expected InsertPayloadOk::AlreadyTip"),
        };
    }

    #[tokio::test]
    async fn test_fill_gap() {
        // test that we can fill the gap between the tip of the chain and the new block
        let mut mock_chain = MockChain::new();
        let gen = mock_chain.genesis();
        let block_a = mock_chain.new_block(gen.clone());
        let block_b = mock_chain.new_block(block_a.clone());

        let mut blockchain = Blockchain::new(mock_chain);
        let _ = blockchain.handle_new_block(gen.clone()).await;

        let notification = blockchain.handle_new_block(block_b.clone()).await;
        let notification = match notification.unwrap() {
            InsertPayloadOk::Inserted(notification) => notification,
            _ => panic!("Expected InsertPayloadOk::Inserted"),
        };

        let committed = notification.committed();
        assert_eq!(committed.blocks.len(), 2);

        assert_eq!(
            committed.blocks_as_vec(),
            vec![block_a.clone(), block_b.clone()]
        );
    }

    #[tokio::test]
    async fn test_sync_batch() {
        let mut mock_chain = MockChain::new();
        let gen = mock_chain.genesis();
        let block_a = mock_chain.new_block(gen.clone());
        let block_b = mock_chain.new_block(block_a.clone());

        // include a fork on block_a
        let block_2_b = mock_chain.new_block(block_a.clone());
        let block_2_c = mock_chain.new_block(block_2_b.clone());

        let mut blockchain = Blockchain::new(mock_chain);
        let _ = blockchain.handle_new_block(block_b.clone()).await;

        let notification = blockchain.handle_new_block(block_2_c.clone()).await;
        let notification = match notification.unwrap() {
            InsertPayloadOk::Inserted(notification) => notification,
            _ => panic!("Expected InsertPayloadOk::Inserted"),
        };

        let reverted = notification.reverted();
        let committed = notification.committed();

        assert_eq!(reverted.clone().unwrap().blocks.len(), 1);
        assert_eq!(reverted.unwrap().blocks_as_vec(), vec![block_b.clone()]);
        assert_eq!(committed.blocks.len(), 2);
        assert_eq!(
            committed.blocks_as_vec(),
            vec![block_2_b.clone(), block_2_c.clone()]
        );
    }

    #[tokio::test]
    async fn test_something() {
        let size = 5;
        let (tx, _) = broadcast::channel(size);
        let mut rx1 = tx.subscribe();
        //let mut rx2 = tx.subscribe();

        for i in 0..100 {
            println!("tx: {:?}", i);
            tx.send(i).unwrap();
        }

        // sleep

        println!("rx1: {:?}", rx1.recv().await); // lagged 2 ??
        println!("rx1: {:?}", rx1.recv().await); // 2
        println!("rx1: {:?}", rx1.recv().await); // 3
        println!("rx1: {:?}", rx1.recv().await); // 3
        println!("rx1: {:?}", rx1.recv().await); // 3
        println!("rx1: {:?}", rx1.recv().await); // 3
        println!("rx1: {:?}", rx1.recv().await); // 3
        println!("rx1: {:?}", rx1.recv().await); // 3
        println!("rx1: {:?}", rx1.recv().await); // 3
                                                 // why?

        //println!("rx2: {:?}", rx1.recv().await);

        //assert_eq!(rx1.recv().await.unwrap(), 10);
        //assert_eq!(rx1.recv().await.unwrap(), 20);
        //});

        //tokio::spawn(async move {
        //assert_eq!(rx2.recv().await.unwrap(), 10);
        //assert_eq!(rx2.recv().await.unwrap(), 20);
        //});
    }

    #[test]
    fn test_filter_storage() {
        /*
        let db = FilterStorage::new("./storage").unwrap();

        let event: EventFilter = "event DepositEvent(
            bytes pubkey,
            bytes withdrawal_credentials,
            bytes amount,
            bytes signature,
            bytes index
        )"
        .into();

        db.create_entry(event, FilterStatus::default()).unwrap();

        let filters = db.get_all_filters().unwrap();
        assert_eq!(filters.len(), 1);

        let status = db.get_status("DepositEvent").unwrap();
        assert_eq!(status.last_block, 0);

        let new_status = FilterStatus { last_block: 1 };
        db.upsert_status("DepositEvent", new_status).unwrap();

        let status = db.get_status("DepositEvent").unwrap();
        assert_eq!(status.last_block, 1);
        */
    }
}
