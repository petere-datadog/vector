use futures::FutureExt;
use vector_lib::configurable::configurable_component;
use vrl::value::Kind;

use vector_lib::codecs::{JsonSerializerConfig, NewlineDelimitedEncoderConfig, encoding::Framer};

use super::{
    auth::AzureDataExplorerAuth,
    request_builder::AzureDataExplorerRequestBuilder,
    service::AdxServiceRequestBuilder,
    sink::{AdxPartitionKey, AdxPartitioner, AzureDataExplorerSink},
};
use crate::{
    config::{SinkContext, ValidatedSink},
    http::client_v1::{HttpClient, full_body},
    sinks::{
        azure_common::config::AzureAuthentication,
        prelude::*,
        util::{
            BatchConfig,
            http::RetryStrategy,
            http_v1::{AsyncHttpService, http_response_retry_logic},
        },
    },
    template::{ConfinedTemplate, ConfinementConfig, Template},
    tls::MaybeTlsSettings,
};

/// Default Batch settings for the azure_data_explorer sink.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct AzureDataExplorerDefaultBatchSettings;

impl SinkBatchSettings for AzureDataExplorerDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1000);
    // Defaulting to 2MB to allow for data created via update policies during the ingestion.
    // see here: https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Ccsharp
    const MAX_BYTES: Option<usize> = Some(2_000_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Configuration for the `azure_data_explorer` sink.
#[configurable_component(sink(
    "azure_data_explorer",
    "Deliver log events to Azure Data Explorer (Kusto) via streaming ingestion."
))]
#[derive(Clone, Debug)]
pub struct AzureDataExplorerConfig {
    /// The Kusto cluster endpoint URL.
    ///
    /// This must be the plain cluster URL, without the `ingest-` prefix, because
    /// streaming ingestion POSTs directly to the cluster.
    #[configurable(metadata(docs::examples = "https://mycluster.eastus.kusto.windows.net"))]
    #[configurable(validation(format = "uri"))]
    pub(super) ingestion_endpoint: String,

    /// The target database.
    ///
    /// Supports [template syntax][template] for dynamic routing.
    ///
    /// [template]: https://vector.dev/docs/reference/configuration/template-syntax/
    #[configurable(metadata(
        docs::examples = "my_database",
        docs::examples = "logs_{{ tenant }}",
    ))]
    pub(super) database: Template,

    /// The target table.
    ///
    /// Supports [template syntax][template] for dynamic routing.
    ///
    /// [template]: https://vector.dev/docs/reference/configuration/template-syntax/
    #[configurable(metadata(
        docs::examples = "my_table",
        docs::examples = "logs_{{ kubernetes.namespace }}",
    ))]
    pub(super) table: Template,

    /// Azure authentication configuration.
    ///
    /// Supports `client_secret_credential`, `managed_identity`, `workload_identity`,
    /// `azure_cli`, `client_certificate_credential`, and `managed_identity_client_assertion`.
    pub(super) auth: AzureAuthentication,

    /// Optional ingestion mapping reference name, sent as the `mappingName` query parameter.
    ///
    /// The named [JSON mapping] must already exist on the table in ADX. Supports
    /// [template syntax][template].
    ///
    /// [JSON mapping]: https://learn.microsoft.com/en-us/kusto/management/mappings?view=azure-data-explorer
    /// [template]: https://vector.dev/docs/reference/configuration/template-syntax/
    #[serde(default)]
    #[configurable(metadata(docs::examples = "my_mapping"))]
    pub(super) mapping_reference: Option<Template>,

    #[serde(default)]
    pub(super) batch: BatchConfig<AzureDataExplorerDefaultBatchSettings>,

    #[serde(default)]
    pub(super) request: TowerRequestConfig,

    #[serde(default)]
    pub(super) retry_strategy: RetryStrategy,

    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub(super) encoding: Transformer,

    /// Compression algorithm.
    ///
    /// When gzip is used, the payload is compressed and `Content-Encoding: gzip` is set.
    #[serde(default = "Compression::gzip_default")]
    pub(super) compression: Compression,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub(super) acknowledgements: AcknowledgementsConfig,

    #[serde(flatten)]
    pub(super) confinement: ConfinementConfig,
}

impl GenerateConfig for AzureDataExplorerConfig {
    fn generate_config() -> serde_json::Value {
        serde_yaml::from_str(indoc::indoc! {r#"
            ingestion_endpoint: https://mycluster.eastus.kusto.windows.net
            database: my_database
            table: my_table
            auth:
              azure_credential_kind: client_secret_credential
              azure_tenant_id: ${AZURE_TENANT_ID}
              azure_client_id: ${AZURE_CLIENT_ID}
              azure_client_secret: ${AZURE_CLIENT_SECRET}
        "#})
        .unwrap()
    }
}

/// Validated state for the `azure_data_explorer` sink.
pub struct ValidatedAzureDataExplorer {
    batch_settings: BatcherSettings,
    confined_database: ConfinedTemplate,
    confined_table: ConfinedTemplate,
    confined_mapping_reference: Option<ConfinedTemplate>,
    compression: Compression,
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_data_explorer")]
impl SinkConfig for AzureDataExplorerConfig {
    fn confinement_config(&self) -> Option<&crate::template::ConfinementConfig> {
        Some(&self.confinement)
    }

    fn input(&self) -> Input {
        let requirement = Requirement::empty().optional_meaning("timestamp", Kind::timestamp());
        Input::log().with_schema_requirement(requirement)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[async_trait::async_trait]
impl ValidatedSink for AzureDataExplorerConfig {
    type Validated = ValidatedAzureDataExplorer;

    fn validate(&self) -> crate::Result<Self::Validated> {
        let batch_settings = self.batch.validate()?.into_batcher_settings()?;

        // Surface a malformed endpoint at config load rather than per request.
        url::Url::parse(self.ingestion_endpoint.trim_end_matches('/'))
            .map_err(|e| format!("Invalid `ingestion_endpoint` URL: {e}"))?;

        let confined_database = self.database.clone().confine(
            &self.confinement,
            AzureDataExplorerConfig::NAME,
            "database",
        )?;
        let confined_table = self.table.clone().confine(
            &self.confinement,
            AzureDataExplorerConfig::NAME,
            "table",
        )?;
        let confined_mapping_reference = self
            .mapping_reference
            .clone()
            .map(|template| {
                template.confine(
                    &self.confinement,
                    AzureDataExplorerConfig::NAME,
                    "mapping_reference",
                )
            })
            .transpose()?;

        let compression = match self.compression {
            Compression::None | Compression::Gzip(_) => self.compression,
            _ => return Err(format!("Invalid compression: {}", self.compression).into()),
        };

        Ok(ValidatedAzureDataExplorer {
            batch_settings,
            confined_database,
            confined_table,
            confined_mapping_reference,
            compression,
        })
    }

    async fn build(
        &self,
        validated: &Self::Validated,
        cx: SinkContext,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let ValidatedAzureDataExplorer {
            batch_settings,
            confined_database,
            confined_table,
            confined_mapping_reference,
            compression,
        } = validated;

        let client = HttpClient::new(MaybeTlsSettings::from_config(None, false)?, cx.proxy())?;
        let auth = AzureDataExplorerAuth::new(&self.auth)?;

        let partitioner = AdxPartitioner {
            database: confined_database.clone(),
            table: confined_table.clone(),
            mapping_reference: confined_mapping_reference.clone(),
        };

        // `streamFormat=MultiJSON` requires newline-delimited JSON objects, so the
        // framer and serializer are fixed rather than user-selectable.
        let encoder = Encoder::<Framer>::new(
            NewlineDelimitedEncoderConfig.build().into(),
            JsonSerializerConfig::default().build().into(),
        );

        let request_builder = AzureDataExplorerRequestBuilder {
            encoder: (self.encoding.clone(), encoder),
            compression: *compression,
        };

        let service_request_builder = AdxServiceRequestBuilder {
            ingestion_endpoint: self.ingestion_endpoint.clone(),
            compression: *compression,
            auth: auth.clone(),
        };

        let service: AsyncHttpService<AdxServiceRequestBuilder, AdxPartitionKey> =
            AsyncHttpService::new(client.clone(), service_request_builder);

        let service = ServiceBuilder::new()
            .settings(
                self.request.into_settings(),
                http_response_retry_logic(self.retry_strategy.clone()),
            )
            .service(service);

        let sink =
            AzureDataExplorerSink::new(service, *batch_settings, request_builder, partitioner);

        let healthcheck = healthcheck(client, self.ingestion_endpoint.clone(), auth).boxed();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }
}

/// Acquires a token and runs `.show version` against the cluster.
async fn healthcheck(
    client: HttpClient,
    ingestion_endpoint: String,
    auth: AzureDataExplorerAuth,
) -> crate::Result<()> {
    let token = auth.get_token().await?;
    let mgmt_uri = format!("{}/v1/rest/mgmt", ingestion_endpoint.trim_end_matches('/'));

    let body = serde_json::json!({
        "csl": ".show version",
    });
    let body = bytes::Bytes::from(serde_json::to_vec(&body)?);

    let request = http_1::Request::post(&mgmt_uri)
        .header(http_1::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(http_1::header::CONTENT_TYPE, "application/json")
        .header("Accept", "application/json")
        .body(full_body(body))?;

    let response = client.send(request).await?;
    let status = response.status();

    if status.is_success() {
        Ok(())
    } else if status == http_1::StatusCode::UNAUTHORIZED || status == http_1::StatusCode::FORBIDDEN
    {
        Err(format!(
            "Azure Data Explorer authentication failed (HTTP {status}). \
             Verify your `auth` configuration."
        )
        .into())
    } else {
        Err(format!("Azure Data Explorer healthcheck failed: HTTP {status}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AzureDataExplorerConfig>();
    }
}
