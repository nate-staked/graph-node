use graph::{
    blockchain::{Block as BlockchainBlock, BlockPtr, BlockTime},
    prelude::{BlockNumber, alloy::primitives::B256, hex},
};
use prost::Message;
use std::convert::TryFrom;

#[derive(Clone, PartialEq, Message)]
pub struct Block {
    #[prost(message, optional, tag = "1")]
    pub header: Option<BlockHeader>,
    #[prost(message, repeated, tag = "2")]
    pub public_logs: Vec<PublicLog>,
}

#[derive(Clone, PartialEq, Message)]
pub struct HeaderOnlyBlock {
    #[prost(message, optional, tag = "1")]
    pub header: Option<BlockHeader>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockHeader {
    #[prost(int64, tag = "1")]
    pub number: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub parent_hash: Vec<u8>,
    #[prost(int64, tag = "4")]
    pub timestamp: i64,
    #[prost(int64, tag = "5")]
    pub final_block_number: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct PublicLog {
    #[prost(bytes = "vec", tag = "1")]
    pub contract_address: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub tag: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub fields: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "4")]
    pub tx_hash: Vec<u8>,
    #[prost(uint32, tag = "5")]
    pub log_index: u32,
}

impl Block {
    pub fn header(&self) -> &BlockHeader {
        self.header
            .as_ref()
            .expect("Aztec Firehose block is missing its header")
    }

    pub fn final_block_number(&self) -> Option<BlockNumber> {
        let number = self.header().final_block_number;
        if number <= 0 {
            None
        } else {
            BlockNumber::try_from(number).ok()
        }
    }
}

impl HeaderOnlyBlock {
    pub fn header(&self) -> &BlockHeader {
        self.header
            .as_ref()
            .expect("Aztec Firehose header-only block is missing its header")
    }
}

impl BlockHeader {
    pub fn ptr(&self) -> BlockPtr {
        BlockPtr::from((hash_to_b256(&self.hash), self.number))
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        if self.number <= 0 || self.parent_hash.is_empty() {
            None
        } else {
            Some(BlockPtr::from((
                hash_to_b256(&self.parent_hash),
                self.number.saturating_sub(1),
            )))
        }
    }
}

impl PublicLog {
    pub fn contract_address_hex(&self) -> String {
        normalize_hex_bytes(&self.contract_address)
    }

    pub fn tag_hex(&self) -> String {
        normalize_hex_bytes(&self.tag)
    }
}

impl BlockchainBlock for Block {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().number).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.header().ptr()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        BlockTime::since_epoch(self.header().timestamp, 0)
    }
}

impl BlockchainBlock for HeaderOnlyBlock {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().number).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.header().ptr()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        BlockTime::since_epoch(self.header().timestamp, 0)
    }
}

pub(crate) fn normalize_hex(input: &str) -> String {
    input
        .strip_prefix("0x")
        .or_else(|| input.strip_prefix("0X"))
        .unwrap_or(input)
        .to_ascii_lowercase()
}

fn normalize_hex_bytes(bytes: &[u8]) -> String {
    normalize_hex(&hex::encode(bytes))
}

fn hash_to_b256(bytes: &[u8]) -> B256 {
    let mut hash = [0u8; 32];
    if bytes.len() >= 32 {
        hash.copy_from_slice(&bytes[bytes.len() - 32..]);
    } else {
        hash[32 - bytes.len()..].copy_from_slice(bytes);
    }
    B256::from(hash)
}
