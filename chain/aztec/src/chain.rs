use async_trait::async_trait;
use graph::{
    anyhow::Result,
    blockchain::{
        Block as _, BlockHash, BlockIngestor, BlockPtr, Blockchain, BlockchainKind,
        EmptyNodeCapabilities, IngestorError, NoopDecoderHook, NoopRuntimeAdapter,
        RuntimeAdapter as RuntimeAdapterTrait, TriggerFilterWrapper,
        block_stream::{
            BlockStream, BlockStreamBuilder, BlockStreamError, BlockStreamEvent, BlockStreamMapper,
            BlockWithTriggers, FirehoseCursor, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        client::ChainClient,
        firehose_block_ingestor::FirehoseBlockIngestor,
        firehose_block_stream::FirehoseBlockStream,
    },
    cheap_clone::CheapClone,
    components::{
        network_provider::ChainName,
        store::{ChainHeadStore, DeploymentCursorTracker, DeploymentLocator, SourceableStore},
    },
    data::subgraph::UnifiedMappingApiVersion,
    firehose::{self, FirehoseEndpoint, ForkStep},
    futures03::Stream,
    prelude::{BlockNumber, Error, Logger, LoggerFactory, MetricsRegistry, o},
};
use prost::Message;
use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    adapter::TriggerFilter,
    codec,
    data_source::{
        DataSource, DataSourceTemplate, UnresolvedDataSource, UnresolvedDataSourceTemplate,
    },
    rpc::AztecRpcProvider,
    trigger::{AztecTrigger, PublicLogTrigger},
};

pub struct Chain {
    logger_factory: LoggerFactory,
    name: ChainName,
    client: Arc<ChainClient<Self>>,
    chain_head_store: Arc<dyn ChainHeadStore>,
    metrics_registry: Arc<MetricsRegistry>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: aztec")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: ChainName,
        chain_head_store: Arc<dyn ChainHeadStore>,
        client: ChainClient<Self>,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            logger_factory,
            name,
            chain_head_store,
            client: Arc::new(client),
            metrics_registry,
            block_stream_builder: Arc::new(AztecStreamBuilder {}),
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Aztec;

    type Client = AztecRpcProvider;
    type Block = codec::Block;
    type DataSource = DataSource;
    type UnresolvedDataSource = UnresolvedDataSource;
    type DataSourceTemplate = DataSourceTemplate;
    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;
    type TriggerData = AztecTrigger;
    type MappingTrigger = AztecTrigger;
    type TriggerFilter = TriggerFilter;
    type NodeCapabilities = EmptyNodeCapabilities<Chain>;
    type DecoderHook = NoopDecoderHook;

    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapterTrait<Self>>, Error> {
        Ok(Arc::new(TriggersAdapter {}))
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: impl DeploymentCursorTracker,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        filter: Arc<TriggerFilterWrapper<Self>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        if self.client.is_firehose() {
            self.block_stream_builder
                .build_firehose(
                    self,
                    deployment,
                    store.firehose_cursor(),
                    start_blocks,
                    store.block_ptr(),
                    filter.chain_filter.clone(),
                    unified_api_version,
                )
                .await
        } else {
            self.block_stream_builder
                .build_polling(
                    self,
                    deployment,
                    start_blocks,
                    source_subgraph_stores,
                    store.block_ptr(),
                    filter,
                    unified_api_version,
                )
                .await
        }
    }

    async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        self.chain_head_store.cheap_clone().chain_head_ptr().await
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        if self.client.is_firehose() {
            let firehose_endpoint = self.client.firehose_endpoint().await?;

            firehose_endpoint
                .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
                .await
                .map_err(Into::into)
        } else {
            self.client
                .rpc()?
                .block_ptr_for_number(number)
                .await
                .map_err(Into::into)
        }
    }

    async fn refetch_firehose_block(
        &self,
        _logger: &Logger,
        _cursor: FirehoseCursor,
    ) -> Result<Self::Block, Error> {
        unimplemented!(
            "Aztec dynamic data sources are not supported in this POC, so refetch is disabled"
        )
    }

    fn is_refetch_block_required(&self) -> bool {
        false
    }

    async fn runtime(
        &self,
    ) -> Result<(Arc<dyn RuntimeAdapterTrait<Self>>, Self::DecoderHook), Error> {
        Ok((Arc::new(NoopRuntimeAdapter::default()), NoopDecoderHook))
    }

    fn chain_client(&self) -> Arc<ChainClient<Self>> {
        self.client.clone()
    }

    async fn block_ingestor(&self) -> Result<Box<dyn BlockIngestor>, Error> {
        if self.client.is_firehose() {
            let ingestor = FirehoseBlockIngestor::<codec::HeaderOnlyBlock, Self>::new(
                self.chain_head_store.cheap_clone(),
                self.chain_client(),
                self.logger_factory
                    .component_logger("AztecFirehoseBlockIngestor", None),
                self.name.clone(),
            );
            Ok(Box::new(ingestor))
        } else {
            Ok(Box::new(RpcBlockIngestor {
                logger: self
                    .logger_factory
                    .component_logger("AztecRpcBlockIngestor", None),
                name: self.name.clone(),
                chain_head_store: self.chain_head_store.cheap_clone(),
                rpc: self.client.rpc()?.clone(),
                polling_interval: Duration::from_secs(2),
            }))
        }
    }
}

pub struct AztecStreamBuilder {}

#[async_trait]
impl BlockStreamBuilder<Chain> for AztecStreamBuilder {
    async fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let adapter = chain.triggers_adapter(
            &deployment,
            &EmptyNodeCapabilities::default(),
            unified_api_version,
        )?;

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "AztecFirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper { adapter, filter });

        Ok(Box::new(FirehoseBlockStream::new(
            deployment.hash,
            chain.chain_client(),
            subgraph_current_block,
            block_cursor,
            firehose_mapper,
            start_blocks,
            logger,
            chain.metrics_registry.clone(),
        )))
    }

    async fn build_polling(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        _source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilterWrapper<Chain>>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let adapter = chain.triggers_adapter(
            &deployment,
            &EmptyNodeCapabilities::default(),
            unified_api_version,
        )?;

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "AztecRpcPollingBlockStream"));

        let start_block = subgraph_current_block
            .as_ref()
            .map(|ptr| ptr.number.saturating_add(1))
            .or_else(|| start_blocks.into_iter().min())
            .unwrap_or(0);

        Ok(Box::new(RpcPollingBlockStream::new(
            chain.client.rpc()?.clone(),
            adapter,
            filter.chain_filter.clone(),
            logger,
            start_block,
            subgraph_current_block,
            Duration::from_secs(2),
        )))
    }
}

pub struct TriggersAdapter {}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<Chain>>, BlockNumber), Error> {
        panic!("Aztec POC uses FirehoseBlockStream, not trigger scanning")
    }

    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: codec::Block,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        let shared_block = Arc::new(block.clone());
        let mut trigger_data = Vec::new();

        for log in &block.public_logs {
            let contract = log.contract_address_hex();
            let tag = log.tag_hex();
            if !filter.matches_public_log(&contract, &tag) {
                continue;
            }

            trigger_data.push(AztecTrigger::PublicLog(Arc::new(PublicLogTrigger {
                log: log.clone(),
                block: shared_block.cheap_clone(),
            })));
        }

        if filter.trigger_every_block {
            trigger_data.push(AztecTrigger::Block(shared_block));
        }

        Ok(BlockWithTriggers::new(block, trigger_data, logger))
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        panic!("Aztec POC Firehose stream handles fork steps directly")
    }

    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        Ok(Some(BlockPtr {
            hash: BlockHash::zero(),
            number: block.number.saturating_sub(1),
        }))
    }

    async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        unimplemented!("chain head comes from the chain store for Aztec")
    }

    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
        _root: Option<BlockHash>,
    ) -> Result<Option<codec::Block>, Error> {
        panic!("Aztec POC FirehoseBlockStream cannot resolve ancestor blocks")
    }

    async fn load_block_ptrs_by_numbers(
        &self,
        _logger: Logger,
        _block_numbers: BTreeSet<BlockNumber>,
    ) -> Result<Vec<codec::Block>> {
        unimplemented!("Aztec POC does not support subgraph data source trigger scans")
    }
}

pub struct RpcPollingBlockStream {
    inner: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<Chain>, BlockStreamError>> + Send>>,
}

impl RpcPollingBlockStream {
    fn new(
        rpc: AztecRpcProvider,
        adapter: Arc<dyn TriggersAdapterTrait<Chain>>,
        filter: Arc<TriggerFilter>,
        logger: Logger,
        next_block: BlockNumber,
        current_block: Option<BlockPtr>,
        polling_interval: Duration,
    ) -> Self {
        let state = RpcPollingState {
            rpc,
            adapter,
            filter,
            logger,
            next_block,
            current_block,
            polling_interval,
        };

        Self {
            inner: Box::pin(graph::futures03::stream::unfold(
                state,
                |mut state| async move {
                    loop {
                        match state.next_event().await {
                            Ok(Some(event)) => return Some((Ok(event), state)),
                            Ok(None) => tokio::time::sleep(state.polling_interval).await,
                            Err(err) => return Some((Err(BlockStreamError::from(err)), state)),
                        }
                    }
                },
            )),
        }
    }
}

impl BlockStream<Chain> for RpcPollingBlockStream {
    fn buffer_size_hint(&self) -> usize {
        1
    }
}

impl Stream for RpcPollingBlockStream {
    type Item = Result<BlockStreamEvent<Chain>, BlockStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

struct RpcPollingState {
    rpc: AztecRpcProvider,
    adapter: Arc<dyn TriggersAdapterTrait<Chain>>,
    filter: Arc<TriggerFilter>,
    logger: Logger,
    next_block: BlockNumber,
    current_block: Option<BlockPtr>,
    polling_interval: Duration,
}

impl RpcPollingState {
    async fn next_event(&mut self) -> Result<Option<BlockStreamEvent<Chain>>, Error> {
        let latest = self.rpc.latest_block_number().await?;
        if self.next_block > latest {
            return Ok(None);
        }

        let block = self.rpc.block_by_number(self.next_block).await?;
        let parent_ptr = block.parent_ptr();

        if let (Some(current), Some(parent)) = (&self.current_block, parent_ptr.as_ref()) {
            if current != parent {
                self.current_block = Some(parent.clone());
                self.next_block = parent.number.saturating_add(1);
                return Ok(Some(BlockStreamEvent::Revert(
                    parent.clone(),
                    FirehoseCursor::None,
                )));
            }
        }

        self.next_block = block.number().saturating_add(1);
        self.current_block = Some(block.ptr());

        let block_with_triggers = self
            .adapter
            .triggers_in_block(&self.logger, block, self.filter.as_ref())
            .await?;

        Ok(Some(BlockStreamEvent::ProcessBlock(
            block_with_triggers,
            FirehoseCursor::None,
        )))
    }
}

pub struct RpcBlockIngestor {
    logger: Logger,
    name: ChainName,
    chain_head_store: Arc<dyn ChainHeadStore>,
    rpc: AztecRpcProvider,
    polling_interval: Duration,
}

#[async_trait]
impl BlockIngestor for RpcBlockIngestor {
    async fn run(self: Box<Self>) {
        loop {
            match self.ingest_latest_block().await {
                Ok(()) => {}
                Err(err) => {
                    graph::slog::warn!(
                        self.logger,
                        "Aztec RPC block ingestor failed";
                        "network" => self.name.to_string(),
                        "error" => err.to_string(),
                    );
                }
            }

            tokio::time::sleep(self.polling_interval).await;
        }
    }

    fn network_name(&self) -> ChainName {
        self.name.clone()
    }

    fn kind(&self) -> BlockchainKind {
        BlockchainKind::Aztec
    }
}

impl RpcBlockIngestor {
    async fn ingest_latest_block(&self) -> Result<(), Error> {
        let latest = self.rpc.latest_block_number().await?;
        let block = Arc::new(self.rpc.block_by_number(latest).await?);

        self.chain_head_store
            .cheap_clone()
            .set_chain_head(block, latest.to_string())
            .await
    }
}

pub struct FirehoseMapper {
    adapter: Arc<dyn TriggersAdapterTrait<Chain>>,
    filter: Arc<TriggerFilter>,
}

#[async_trait]
impl BlockStreamMapper<Chain> for FirehoseMapper {
    fn decode_block(
        &self,
        output: Option<&[u8]>,
    ) -> Result<Option<codec::Block>, BlockStreamError> {
        let block = match output {
            Some(block) => codec::Block::decode(block)?,
            None => {
                return Err(anyhow::anyhow!(
                    "Aztec mapper expected every Firehose response to include a block"
                )
                .into());
            }
        };

        Ok(Some(block))
    }

    async fn block_with_triggers(
        &self,
        logger: &Logger,
        block: codec::Block,
    ) -> Result<BlockWithTriggers<Chain>, BlockStreamError> {
        self.adapter
            .triggers_in_block(logger, block, self.filter.as_ref())
            .await
            .map_err(BlockStreamError::from)
    }
}

#[async_trait]
impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    fn trigger_filter(&self) -> &TriggerFilter {
        self.filter.as_ref()
    }

    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
    ) -> Result<BlockStreamEvent<Chain>, FirehoseError> {
        let step = ForkStep::try_from(response.step).unwrap_or_else(|_| {
            panic!(
                "unknown step i32 value {}, maybe the Firehose protobuf definitions are stale",
                response.step
            )
        });

        let any_block = response
            .block
            .as_ref()
            .expect("Aztec Firehose response should always include a block payload");
        let block = self.decode_block(Some(any_block.value.as_ref()))?.unwrap();

        match step {
            ForkStep::StepNew => Ok(BlockStreamEvent::ProcessBlock(
                self.block_with_triggers(logger, block).await?,
                FirehoseCursor::from(response.cursor.clone()),
            )),
            ForkStep::StepUndo => {
                let parent_ptr = block
                    .parent_ptr()
                    .expect("Aztec genesis block should never be reverted");

                Ok(BlockStreamEvent::Revert(
                    parent_ptr,
                    FirehoseCursor::from(response.cursor.clone()),
                ))
            }
            ForkStep::StepFinal => {
                panic!("irreversible step is not handled and should not be requested")
            }
            ForkStep::StepUnset => panic!("unknown Firehose fork step"),
        }
    }

    async fn block_ptr_for_number(
        &self,
        logger: &Logger,
        endpoint: &Arc<FirehoseEndpoint>,
        number: BlockNumber,
    ) -> Result<BlockPtr, Error> {
        endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .await
    }

    async fn final_block_ptr_for(
        &self,
        logger: &Logger,
        endpoint: &Arc<FirehoseEndpoint>,
        block: &codec::Block,
    ) -> Result<BlockPtr, Error> {
        match block.final_block_number() {
            Some(number) => self.block_ptr_for_number(logger, endpoint, number).await,
            None => Ok(block.ptr()),
        }
    }
}
