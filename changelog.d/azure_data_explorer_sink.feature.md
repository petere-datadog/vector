A new `azure_data_explorer` sink has been added to deliver log events to Azure Data Explorer (Kusto) via streaming ingestion. Events are encoded as MultiJSON and sent to `POST {ingestion_endpoint}/v1/rest/ingest/{database}/{table}`.

The sink supports Azure Entra ID authentication through all of the credential kinds shared by Vector's other Azure sinks, gzip compression, and configurable batching. The `database`, `table`, and `mapping_reference` options each accept template syntax, so events can be routed to different ingestion targets based on their contents.

Streaming ingestion must be enabled on the cluster, with a [streaming ingestion policy](https://learn.microsoft.com/en-us/kusto/management/streaming-ingestion-policy) defined on the target database or table. Note that `ingestion_endpoint` is the plain cluster URL, without the `ingest-` prefix.

authors: petere-datadog, benmali
