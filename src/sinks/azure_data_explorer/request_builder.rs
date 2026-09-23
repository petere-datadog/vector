use std::io;

use bytes::Bytes;

use vector_lib::codecs::encoding::Framer;

use super::sink::AdxPartitionKey;
use crate::sinks::{prelude::*, util::http::HttpRequest};

pub(super) struct AzureDataExplorerRequestBuilder {
    pub(super) encoder: (Transformer, Encoder<Framer>),
    pub(super) compression: Compression,
}

impl RequestBuilder<(AdxPartitionKey, Vec<Event>)> for AzureDataExplorerRequestBuilder {
    type Metadata = (AdxPartitionKey, EventFinalizers);
    type Events = Vec<Event>;
    type Encoder = (Transformer, Encoder<Framer>);
    type Payload = Bytes;
    type Request = HttpRequest<AdxPartitionKey>;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (AdxPartitionKey, Vec<Event>),
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let (key, mut events) = input;
        let finalizers = events.take_finalizers();
        let builder = RequestMetadataBuilder::from_events(&events);
        ((key, finalizers), builder, events)
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let (key, finalizers) = metadata;
        HttpRequest::new(payload.into_payload(), finalizers, request_metadata, key)
    }
}
