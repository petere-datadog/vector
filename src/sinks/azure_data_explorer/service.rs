//! Service implementation for the `azure_data_explorer` sink.
//!
//! Performs streaming ingestion:
//! `POST /v1/rest/ingest/{database}/{table}?streamFormat=MultiJSON`, which
//! requires the plain cluster URL (no `ingest-` prefix).
//! See: <https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/rest/streaming-ingest>

use bytes::Bytes;
use http_1::{
    Request,
    header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE},
};
use url::Url;
use uuid::Uuid;

use super::{auth::AzureDataExplorerAuth, sink::AdxPartitionKey};
use crate::sinks::util::{
    buffer::compression::Compression, http::HttpRequest, http_v1::AsyncHttpServiceRequestBuilder,
};

#[derive(Clone)]
pub(super) struct AdxServiceRequestBuilder {
    pub(super) ingestion_endpoint: String,
    pub(super) compression: Compression,
    pub(super) auth: AzureDataExplorerAuth,
}

#[async_trait::async_trait]
impl AsyncHttpServiceRequestBuilder<AdxPartitionKey> for AdxServiceRequestBuilder {
    async fn build(
        &self,
        mut request: HttpRequest<AdxPartitionKey>,
    ) -> Result<Request<Bytes>, crate::Error> {
        let uri = build_ingest_uri(&self.ingestion_endpoint, request.get_additional_metadata())?;
        let payload = request.take_payload();

        // Served from the Azure SDK's token cache; only reaches Entra ID when the
        // cached token is within five minutes of expiring.
        let token = self.auth.get_token().await?;

        let mut builder = Request::post(uri.as_str())
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, payload.len())
            .header("Accept", "application/json")
            .header("x-ms-app", "Kusto.Vector")
            .header("x-ms-user", "Kusto.Vector")
            .header(
                "x-ms-client-request-id",
                format!("Vector.Ingest;{}", Uuid::new_v4()),
            );

        if let Some(content_encoding) = self.compression.content_encoding() {
            builder = builder.header(CONTENT_ENCODING, content_encoding);
        }

        builder.body(payload).map_err(Into::into)
    }
}

/// Builds `POST {ingestion_endpoint}/v1/rest/ingest/{database}/{table}?streamFormat=MultiJSON...`
fn build_ingest_uri(ingestion_endpoint: &str, key: &AdxPartitionKey) -> crate::Result<Url> {
    let base = ingestion_endpoint.trim_end_matches('/');
    let mut url = Url::parse(base).map_err(|e| format!("Invalid ingestion_endpoint URL: {e}"))?;

    url.path_segments_mut()
        .map_err(|()| {
            "ingestion_endpoint must be a hierarchical HTTP(S) URL (e.g. cannot-be-a-base URLs are not supported)"
        })?
        .push("v1")
        .push("rest")
        .push("ingest")
        .push(key.database.as_str())
        .push(key.table.as_str());

    {
        let mut query = url.query_pairs_mut();
        query.append_pair("streamFormat", "MultiJSON");
        if let Some(mapping) = key.mapping_reference.as_deref()
            && !mapping.is_empty()
        {
            query.append_pair("mappingName", mapping);
        }
    }

    Ok(url)
}
