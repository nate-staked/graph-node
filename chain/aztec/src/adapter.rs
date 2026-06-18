use std::collections::HashSet;

use graph::blockchain as bc;
use graph::prelude::*;
use prost_types::Any;

use crate::{Chain, codec::normalize_hex, data_source::DataSource};

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) trigger_every_block: bool,
    pub(crate) contract_addresses: HashSet<String>,
    pub(crate) tags: HashSet<String>,
}

impl TriggerFilter {
    pub(crate) fn matches_public_log(&self, contract_address: &str, tag: &str) -> bool {
        let contract_matches = self.contract_addresses.is_empty()
            || self.contract_addresses.contains(contract_address);
        let tag_matches = self.tags.is_empty() || self.tags.contains(tag);

        contract_matches && tag_matches
    }
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        for data_source in data_sources {
            self.trigger_every_block |= data_source.has_block_handler();

            if !data_source.has_public_log_handler() {
                continue;
            }

            if let Some(address) = data_source.source.address.as_ref() {
                self.contract_addresses.insert(normalize_hex(address));
            }

            for handler in &data_source.mapping.event_handlers {
                if let Some(tag) = handler.tag.as_ref().or(handler.event.as_ref()) {
                    self.tags.insert(normalize_hex(tag));
                }
            }
        }
    }

    fn node_capabilities(&self) -> bc::EmptyNodeCapabilities<Chain> {
        bc::EmptyNodeCapabilities::default()
    }

    fn extend_with_template(
        &mut self,
        _data_source: impl Iterator<Item = <Chain as bc::Blockchain>::DataSourceTemplate>,
    ) {
    }

    fn to_firehose_filter(self) -> Vec<Any> {
        // The Aztec Firehose filter protobuf is intentionally left undefined in this
        // POC. The graph-node side still collects the right filter inputs so the
        // Firehose producer can add pushdown later without changing manifest shape.
        vec![]
    }
}
