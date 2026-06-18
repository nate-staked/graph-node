use async_trait::async_trait;
use graph::{
    anyhow::{Context, Error, anyhow},
    blockchain::{self, Block as _, Blockchain, TriggerWithHandler},
    cheap_clone::CheapClone,
    components::{
        link_resolver::LinkResolverContext, store::StoredDynamicDataSource,
        subgraph::InstanceDSTemplateInfo,
    },
    data::subgraph::{DataSourceContext, DeploymentHash},
    data_source::DataSourceTemplateInfo,
    prelude::{BlockNumber, Deserialize, Link, LinkResolver, Logger},
    semver,
};
use std::{collections::HashSet, sync::Arc};

use crate::{
    Chain,
    codec::normalize_hex,
    trigger::{AztecTrigger, PublicLogTrigger},
};

pub const AZTEC_KIND: &str = "aztec";
pub const AZTEC_CONTRACT_KIND: &str = "aztec/contract";
const BLOCK_HANDLER_KIND: &str = "block";
const PUBLIC_LOG_HANDLER_KIND: &str = "event";

#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
}

impl DataSource {
    fn from_manifest(
        kind: String,
        network: Option<String>,
        name: String,
        source: Source,
        mapping: Mapping,
        context: Option<DataSourceContext>,
    ) -> Result<Self, Error> {
        Ok(Self {
            kind,
            network,
            name,
            source,
            mapping,
            context: Arc::new(context),
            creation_block: None,
        })
    }

    pub(crate) fn has_block_handler(&self) -> bool {
        !self.mapping.block_handlers.is_empty()
    }

    pub(crate) fn has_public_log_handler(&self) -> bool {
        !self.mapping.event_handlers.is_empty()
    }

    fn block_handler(&self) -> Option<&MappingBlockHandler> {
        self.mapping.block_handlers.first()
    }

    fn matching_public_log_handler(
        &self,
        trigger: &PublicLogTrigger,
    ) -> Option<&MappingEventHandler> {
        if let Some(address) = &self.source.address {
            if normalize_hex(address) != trigger.log.contract_address_hex() {
                return None;
            }
        }

        self.mapping.event_handlers.iter().find(|handler| {
            handler
                .tag
                .as_ref()
                .or(handler.event.as_ref())
                .map(|tag| normalize_hex(tag) == trigger.log.tag_hex())
                .unwrap_or(true)
        })
    }
}

impl blockchain::DataSource<Chain> for DataSource {
    fn from_template_info(
        _info: InstanceDSTemplateInfo,
        _template: &graph::data_source::DataSourceTemplate<Chain>,
    ) -> Result<Self, Error> {
        Err(anyhow!(
            "Aztec subgraphs do not support dynamic data sources in this POC"
        ))
    }

    fn from_stored_dynamic_data_source(
        _template: &DataSourceTemplate,
        _stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        Err(anyhow!(
            "Aztec subgraphs do not support dynamic data sources in this POC"
        ))
    }

    fn address(&self) -> Option<&[u8]> {
        self.source.address.as_ref().map(String::as_bytes)
    }

    fn start_block(&self) -> BlockNumber {
        self.source.start_block
    }

    fn end_block(&self) -> Option<BlockNumber> {
        self.source.end_block
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_deref()
    }

    fn context(&self) -> Arc<Option<DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        self.creation_block
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        Some(self.mapping.runtime.cheap_clone())
    }

    fn handler_kinds(&self) -> HashSet<&str> {
        let mut kinds = HashSet::new();
        if self.has_block_handler() {
            kinds.insert(BLOCK_HANDLER_KIND);
        }
        if self.has_public_log_handler() {
            kinds.insert(PUBLIC_LOG_HANDLER_KIND);
        }
        kinds
    }

    fn match_and_decode(
        &self,
        trigger: &<Chain as Blockchain>::TriggerData,
        block: &Arc<<Chain as Blockchain>::Block>,
        _logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<Chain>>, Error> {
        if self.source.start_block > block.number() {
            return Ok(None);
        }

        let handler = match trigger {
            AztecTrigger::Block(_) => match self.block_handler() {
                Some(handler) => &handler.handler,
                None => return Ok(None),
            },
            AztecTrigger::PublicLog(public_log) => {
                match self.matching_public_log_handler(public_log) {
                    Some(handler) => &handler.handler,
                    None => return Ok(None),
                }
            }
        };

        Ok(Some(TriggerWithHandler::<Chain>::new(
            trigger.cheap_clone(),
            handler.clone(),
            block.ptr(),
            block.timestamp(),
        )))
    }

    fn is_duplicate_of(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.network == other.network
            && self.name == other.name
            && self.source == other.source
            && self.mapping.block_handlers == other.mapping.block_handlers
            && self.mapping.event_handlers == other.mapping.event_handlers
            && self.context == other.context
    }

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        unreachable!("Aztec subgraphs do not support dynamic data sources in this POC")
    }

    fn validate(&self, _: &semver::Version) -> Vec<Error> {
        let mut errors = Vec::new();

        if self.kind != AZTEC_KIND && self.kind != AZTEC_CONTRACT_KIND {
            errors.push(anyhow!(
                "data source has invalid `kind`, expected {} or {} but found {}",
                AZTEC_KIND,
                AZTEC_CONTRACT_KIND,
                self.kind
            ));
        }

        if self.has_public_log_handler() && self.source.address.is_none() {
            errors.push(anyhow!(
                "Aztec public event/log handlers require `source.address` for this POC"
            ));
        }

        errors
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    #[serde(default)]
    pub(crate) source: Source,
    pub mapping: UnresolvedMapping,
    pub context: Option<DataSourceContext>,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        deployment_hash: &DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        _manifest_idx: u32,
        _spec_version: &semver::Version,
    ) -> Result<DataSource, Error> {
        let mapping = self
            .mapping
            .resolve(deployment_hash, resolver, logger)
            .await
            .with_context(|| format!("failed to resolve Aztec data source {}", self.name))?;

        DataSource::from_manifest(
            self.kind,
            self.network,
            self.name,
            self.source,
            mapping,
            self.context,
        )
    }
}

#[derive(Clone, Debug)]
pub struct DataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: Mapping,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: UnresolvedMapping,
}

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for UnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        deployment_hash: &DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        _manifest_idx: u32,
        _spec_version: &semver::Version,
    ) -> Result<DataSourceTemplate, Error> {
        let mapping = self
            .mapping
            .resolve(deployment_hash, resolver, logger)
            .await
            .with_context(|| {
                format!("failed to resolve Aztec data source template {}", self.name)
            })?;

        Ok(DataSourceTemplate {
            kind: self.kind,
            network: self.network,
            name: self.name,
            mapping,
        })
    }
}

impl blockchain::DataSourceTemplate<Chain> for DataSourceTemplate {
    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        Some(self.mapping.runtime.cheap_clone())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn manifest_idx(&self) -> u32 {
        unreachable!("Aztec subgraphs do not support dynamic data sources in this POC")
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn info(&self) -> DataSourceTemplateInfo {
        DataSourceTemplateInfo {
            api_version: self.api_version(),
            runtime: self.runtime(),
            name: self.name.clone(),
            manifest_idx: None,
            kind: self.kind.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    #[serde(default)]
    pub block_handlers: Vec<MappingBlockHandler>,
    #[serde(default, alias = "publicLogHandlers")]
    pub event_handlers: Vec<MappingEventHandler>,
    pub file: Link,
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        deployment_hash: &DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, Error> {
        let module_bytes = resolver
            .cat(
                &LinkResolverContext::new(deployment_hash, logger),
                &self.file,
            )
            .await
            .with_context(|| format!("failed to resolve mapping {}", self.file.link))?;

        Ok(Mapping {
            api_version: semver::Version::parse(&self.api_version)?,
            language: self.language,
            entities: self.entities,
            block_handlers: self.block_handlers,
            event_handlers: self.event_handlers,
            runtime: Arc::new(module_bytes),
            link: self.file,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub language: String,
    pub entities: Vec<String>,
    pub block_handlers: Vec<MappingBlockHandler>,
    pub event_handlers: Vec<MappingEventHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct MappingBlockHandler {
    pub handler: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub handler: String,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Source {
    pub(crate) address: Option<String>,
    #[serde(default)]
    pub(crate) start_block: BlockNumber,
    pub(crate) end_block: Option<BlockNumber>,
}
