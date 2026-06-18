use async_trait::async_trait;
use graph::{
    blockchain::{Block as _, MappingTriggerTrait, TriggerData},
    derive::CheapClone,
    prelude::{BlockNumber, hex},
    runtime::{AscHeap, AscPtr, HostExportError, asc_new, gas::GasCounter},
};
use graph_runtime_wasm::module::ToAscPtr;
use std::{cmp::Ordering, sync::Arc};

use crate::codec;

#[derive(Clone, CheapClone)]
pub enum AztecTrigger {
    Block(Arc<codec::Block>),
    PublicLog(Arc<PublicLogTrigger>),
}

pub struct PublicLogTrigger {
    pub log: codec::PublicLog,
    pub block: Arc<codec::Block>,
}

impl std::fmt::Debug for AztecTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AztecTrigger::Block(block) => f
                .debug_struct("Block")
                .field("number", &block.number())
                .field("hash", &block.hash().hash_hex())
                .finish(),
            AztecTrigger::PublicLog(trigger) => f
                .debug_struct("PublicLog")
                .field("block", &trigger.block.number())
                .field("contract", &trigger.log.contract_address_hex())
                .field("tag", &trigger.log.tag_hex())
                .field("log_index", &trigger.log.log_index)
                .finish(),
        }
    }
}

impl AztecTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            AztecTrigger::Block(block) => block.number(),
            AztecTrigger::PublicLog(trigger) => trigger.block.number(),
        }
    }

    fn error_context(&self) -> String {
        match self {
            AztecTrigger::Block(block) => {
                format!("Aztec block #{} ({})", block.number(), block.hash())
            }
            AztecTrigger::PublicLog(trigger) => format!(
                "Aztec public log contract 0x{}, tag 0x{}, tx 0x{}, block #{} ({})",
                trigger.log.contract_address_hex(),
                trigger.log.tag_hex(),
                hex::encode(&trigger.log.tx_hash),
                trigger.block.number(),
                trigger.block.hash()
            ),
        }
    }
}

impl PartialEq for AztecTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a), Self::Block(b)) => a.ptr() == b.ptr(),
            (Self::PublicLog(a), Self::PublicLog(b)) => {
                a.block.ptr() == b.block.ptr()
                    && a.log.tx_hash == b.log.tx_hash
                    && a.log.log_index == b.log.log_index
            }
            _ => false,
        }
    }
}

impl Eq for AztecTrigger {}

impl Ord for AztecTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Public logs run before block handlers, matching the Ethereum convention
            // that block handlers observe state after log handlers have run.
            (Self::Block(_), Self::PublicLog(_)) => Ordering::Greater,
            (Self::PublicLog(_), Self::Block(_)) => Ordering::Less,
            (Self::Block(_), Self::Block(_)) => Ordering::Equal,
            (Self::PublicLog(a), Self::PublicLog(b)) => a
                .log
                .log_index
                .cmp(&b.log.log_index)
                .then_with(|| a.log.tx_hash.cmp(&b.log.tx_hash)),
        }
    }
}

impl PartialOrd for AztecTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for AztecTrigger {
    fn error_context(&self) -> String {
        self.error_context()
    }

    fn address_match(&self) -> Option<&[u8]> {
        None
    }
}

impl MappingTriggerTrait for AztecTrigger {
    fn error_context(&self) -> String {
        self.error_context()
    }
}

#[async_trait]
impl ToAscPtr for AztecTrigger {
    async fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        let bytes = match self {
            AztecTrigger::Block(block) => block.hash().as_slice().to_vec(),
            AztecTrigger::PublicLog(trigger) => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&trigger.log.contract_address);
                bytes.extend_from_slice(&trigger.log.tag);
                for field in &trigger.log.fields {
                    bytes.extend_from_slice(field);
                }
                bytes
            }
        };

        asc_new(heap, bytes.as_slice(), gas)
            .await
            .map(|ptr| ptr.erase())
    }
}
