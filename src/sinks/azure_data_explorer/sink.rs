//! Sink and partitioner implementation for the `azure_data_explorer` sink.

use crate::sinks::{
    prelude::*,
    util::http::{HttpJsonBatchSizer, HttpRequest},
};

use super::request_builder::AzureDataExplorerRequestBuilder;

/// The resolved ingestion target for a batch of events.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(super) struct AdxPartitionKey {
    pub database: String,
    pub table: String,
    pub mapping_reference: Option<String>,
}

/// Resolves the ADX ingestion target for a single event.
pub(super) struct AdxPartitioner {
    pub database: ConfinedTemplate,
    pub table: ConfinedTemplate,
    pub mapping_reference: Option<ConfinedTemplate>,
}

impl AdxPartitioner {
    fn render(template: &ConfinedTemplate, item: &Event, field: &'static str) -> Option<String> {
        template
            .render_string(item)
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some(field),
                    drop_event: true,
                });
            })
            .ok()
    }
}

impl Partitioner for AdxPartitioner {
    type Item = Event;
    /// `None` means the event should be dropped.
    type Key = Option<AdxPartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let database = Self::render(&self.database, item, "database")?;
        let table = Self::render(&self.table, item, "table")?;
        let mapping_reference = match self.mapping_reference.as_ref() {
            Some(template) => Some(Self::render(template, item, "mapping_reference")?),
            None => None,
        };

        Some(AdxPartitionKey {
            database,
            table,
            mapping_reference,
        })
    }
}

pub(super) struct AzureDataExplorerSink<S> {
    service: S,
    batch_settings: BatcherSettings,
    request_builder: AzureDataExplorerRequestBuilder,
    partitioner: AdxPartitioner,
}

impl<S> AzureDataExplorerSink<S>
where
    S: Service<HttpRequest<AdxPartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    pub(super) const fn new(
        service: S,
        batch_settings: BatcherSettings,
        request_builder: AzureDataExplorerRequestBuilder,
        partitioner: AdxPartitioner,
    ) -> Self {
        Self {
            service,
            batch_settings,
            request_builder,
            partitioner,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;

        input
            .batched_partitioned(self.partitioner, batch_settings.timeout, |_| {
                batch_settings.as_item_size_config(HttpJsonBatchSizer)
            })
            .filter_map(|(key, batch)| async move { key.map(move |k| (k, batch)) })
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for AzureDataExplorerSink<S>
where
    S: Service<HttpRequest<AdxPartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
