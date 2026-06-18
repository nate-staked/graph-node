use async_trait::async_trait;
use graph::{
    anyhow::{Context, Result, anyhow, bail},
    blockchain::{BlockHash, BlockPtr, ChainIdentifier},
    components::network_provider::{NetworkDetails, ProviderName},
    prelude::{BlockNumber, alloy::primitives::B256, hex},
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value, json};
use std::{
    convert::TryFrom,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::codec::{self, Block, BlockHeader, PublicLog};

#[derive(Debug)]
pub struct AztecRpcClient {
    label: String,
    url: String,
    client: reqwest::Client,
    request_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct AztecRpcProvider {
    inner: Arc<AztecRpcClient>,
}

impl AztecRpcProvider {
    pub fn new(
        label: String,
        url: String,
        headers: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(AztecRpcClient::new(label, url, headers, timeout)?),
        })
    }

    pub async fn latest_block_number(&self) -> Result<BlockNumber> {
        self.inner.latest_block_number().await
    }

    pub async fn block_ptr_for_number(&self, number: BlockNumber) -> Result<BlockPtr> {
        self.inner.block_ptr_for_number(number).await
    }

    pub async fn block_by_number(&self, number: BlockNumber) -> Result<Block> {
        self.inner.block_by_number(number).await
    }
}

impl AztecRpcClient {
    pub fn new(
        label: String,
        url: String,
        headers: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Self> {
        let mut header_map = HeaderMap::new();
        for (name, value) in headers {
            header_map.insert(
                HeaderName::try_from(name.as_str())
                    .with_context(|| format!("invalid Aztec RPC header name `{name}`"))?,
                HeaderValue::try_from(value.as_str())
                    .with_context(|| format!("invalid Aztec RPC header value for `{name}`"))?,
            );
        }

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(header_map)
            .build()
            .context("failed to build Aztec RPC HTTP client")?;

        Ok(Self {
            label,
            url,
            client,
            request_id: AtomicU64::new(1),
        })
    }

    pub async fn latest_block_number(&self) -> Result<BlockNumber> {
        let value = self.call("node_getBlockNumber", json!([])).await?;
        value_to_block_number(&value).context("Aztec RPC returned invalid block number")
    }

    pub async fn block_ptr_for_number(&self, number: BlockNumber) -> Result<BlockPtr> {
        Ok(self.block_header(number).await?.ptr())
    }

    pub async fn block_by_number(&self, number: BlockNumber) -> Result<Block> {
        let header = self.block_header(number).await?;
        let public_logs = self.public_logs_for_block(number).await?;

        Ok(Block {
            header: Some(header),
            public_logs,
        })
    }

    async fn block_header(&self, number: BlockNumber) -> Result<BlockHeader> {
        let block = self.call("node_getBlock", json!([number])).await?;
        parse_block_header(&block)
            .with_context(|| format!("Aztec RPC returned an unsupported block shape for #{number}"))
    }

    async fn public_logs_for_block(&self, number: BlockNumber) -> Result<Vec<PublicLog>> {
        let mut logs = Vec::new();
        let mut after_log = None;

        loop {
            let mut filter = json!({
                "fromBlock": number,
                "toBlock": number + 1,
            });

            if let Some(after_log) = after_log.take() {
                filter["afterLog"] = after_log;
            }

            let response = self.call("node_getPublicLogs", json!([filter])).await?;
            let page_logs = response
                .get("logs")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("Aztec RPC getPublicLogs response is missing `logs`"))?;

            for log in page_logs {
                logs.push(parse_public_log(log).context("failed to parse Aztec public log")?);
            }

            let max_logs_hit = response
                .get("maxLogsHit")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            if !max_logs_hit {
                break;
            }

            after_log = page_logs
                .last()
                .and_then(|log| log.get("id").cloned())
                .or_else(|| page_logs.last().and_then(|log| log.get("logId").cloned()));

            if after_log.is_none() {
                bail!("Aztec RPC requested pagination but the last log had no cursor");
            }
        }

        Ok(logs)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let response: Value = self
            .client
            .post(&self.url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .with_context(|| format!("Aztec RPC `{method}` request failed"))?
            .error_for_status()
            .with_context(|| format!("Aztec RPC `{method}` returned HTTP error"))?
            .json()
            .await
            .with_context(|| format!("Aztec RPC `{method}` returned invalid JSON"))?;

        if let Some(error) = response.get("error") {
            bail!("Aztec RPC `{method}` returned error: {error}");
        }

        response
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("Aztec RPC `{method}` response is missing `result`"))
    }
}

#[async_trait]
impl NetworkDetails for AztecRpcProvider {
    fn provider_name(&self) -> ProviderName {
        self.inner.label.clone().into()
    }

    async fn chain_identifier(&self) -> Result<ChainIdentifier> {
        let genesis = self
            .block_ptr_for_number(0)
            .await
            .unwrap_or_else(|_| BlockPtr::new(BlockHash::from(B256::ZERO), 0));

        Ok(ChainIdentifier {
            net_version: "aztec".to_string(),
            genesis_block_hash: genesis.hash,
        })
    }

    async fn provides_extended_blocks(&self) -> Result<bool> {
        Ok(true)
    }
}

fn parse_block_header(value: &Value) -> Result<BlockHeader> {
    let number = first_i64(
        value,
        &[
            &["number"],
            &["blockNumber"],
            &["header", "number"],
            &["header", "globalVariables", "blockNumber"],
            &["header", "globalVariables", "block_number"],
        ],
    )
    .ok_or_else(|| anyhow!("missing block number"))?;

    let hash = first_bytes(
        value,
        &[
            &["hash"],
            &["blockHash"],
            &["header", "hash"],
            &["header", "blockHash"],
        ],
    )
    .ok_or_else(|| anyhow!("missing block hash"))?;

    let parent_hash = first_bytes(
        value,
        &[
            &["parentHash"],
            &["previousBlockHash"],
            &["header", "parentHash"],
            &["header", "previousBlockHash"],
        ],
    )
    .unwrap_or_default();

    let timestamp = first_i64(
        value,
        &[
            &["timestamp"],
            &["header", "timestamp"],
            &["header", "globalVariables", "timestamp"],
        ],
    )
    .unwrap_or(0);

    let final_block_number = first_i64(
        value,
        &[
            &["finalBlockNumber"],
            &["finalizedBlockNumber"],
            &["header", "lastFinalBlockHeight"],
            &["header", "finalBlockNumber"],
        ],
    )
    .unwrap_or(0);

    Ok(BlockHeader {
        number,
        hash,
        parent_hash,
        timestamp,
        final_block_number,
    })
}

fn parse_public_log(value: &Value) -> Result<PublicLog> {
    let log = value.get("log").unwrap_or(value);
    let id = value.get("id").unwrap_or(value);

    let fields = first_array(log, &[&["fields"], &["log", "fields"]])
        .ok_or_else(|| anyhow!("missing public log fields"))?
        .iter()
        .map(value_to_bytes)
        .collect::<Result<Vec<_>>>()?;

    let tag = first_bytes(log, &[&["tag"]])
        .or_else(|| fields.first().cloned())
        .unwrap_or_default();

    let contract_address = first_bytes(
        log,
        &[
            &["contractAddress"],
            &["contract_address"],
            &["address"],
            &["log", "contractAddress"],
        ],
    )
    .ok_or_else(|| anyhow!("missing public log contract address"))?;

    let tx_hash = first_bytes(
        id,
        &[
            &["txHash"],
            &["transactionHash"],
            &["id", "txHash"],
            &["logId", "txHash"],
        ],
    )
    .unwrap_or_default();

    let log_index = first_i64(id, &[&["logIndex"], &["index"], &["id", "logIndex"]])
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0);

    Ok(PublicLog {
        contract_address,
        tag,
        fields,
        tx_hash,
        log_index,
    })
}

fn value_to_block_number(value: &Value) -> Result<BlockNumber> {
    let number = value_to_i64(value).ok_or_else(|| anyhow!("invalid block number: {value}"))?;
    Ok(BlockNumber::try_from(number)?)
}

fn first_i64(value: &Value, paths: &[&[&str]]) -> Option<i64> {
    paths
        .iter()
        .filter_map(|path| value_at(value, path))
        .find_map(value_to_i64)
}

fn first_bytes(value: &Value, paths: &[&[&str]]) -> Option<Vec<u8>> {
    paths
        .iter()
        .filter_map(|path| value_at(value, path))
        .find_map(|value| value_to_bytes(value).ok())
}

fn first_array<'a>(value: &'a Value, paths: &[&[&str]]) -> Option<&'a Vec<Value>> {
    paths
        .iter()
        .filter_map(|path| value_at(value, path))
        .find_map(Value::as_array)
}

fn value_at<'a>(mut value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    for segment in path {
        value = value.get(*segment)?;
    }
    Some(value)
}

fn value_to_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value.as_str().and_then(|s| {
            s.strip_prefix("0x")
                .and_then(|hex| i64::from_str_radix(hex, 16).ok())
                .or_else(|| s.parse::<i64>().ok())
        })
    })
}

fn value_to_bytes(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::String(s) => hex_to_bytes(s),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| anyhow!("invalid byte array element: {value}"))
            })
            .collect(),
        _ => bail!("cannot convert JSON value to bytes: {value}"),
    }
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    let hex = codec::normalize_hex(value);
    if hex.is_empty() {
        return Ok(Vec::new());
    }

    let padded = if hex.len() % 2 == 0 {
        hex
    } else {
        format!("0{hex}")
    };

    hex::decode(padded).context("invalid hex string")
}
