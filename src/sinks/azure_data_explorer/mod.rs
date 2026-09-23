//! The Azure Data Explorer (ADX / Kusto) [`vector_lib::sink::VectorSink`].
//!
//! Delivers log events via [streaming ingestion]: `POST /v1/rest/ingest/{db}/{table}`
//! with `streamFormat=MultiJSON`.
//!
//! The target database, table, and ingestion mapping are each resolved per-event
//! from templates, so events can be routed dynamically (see `AdxPartitioner` in
//! `sink.rs`).
//!
//! [streaming ingestion]: https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/rest/streaming-ingest

mod auth;
mod config;
mod request_builder;
mod service;
mod sink;
