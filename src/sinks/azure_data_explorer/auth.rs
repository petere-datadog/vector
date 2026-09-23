//! Azure authentication for Azure Data Explorer.

use std::sync::Arc;

use azure_core::credentials::TokenCredential;

use crate::sinks::azure_common::config::AzureAuthentication;

/// Scope for Azure Data Explorer / Kusto API access.
const KUSTO_SCOPE: &str = "https://kusto.kusto.windows.net/.default";

/// Azure token provider for Azure Data Explorer.
#[derive(Clone)]
pub(super) struct AzureDataExplorerAuth {
    credential: Arc<dyn TokenCredential>,
}

impl AzureDataExplorerAuth {
    /// Creates a new auth provider from an [`AzureAuthentication`] config value.
    pub(super) fn new(auth: &AzureAuthentication) -> crate::Result<Self> {
        let credential = auth
            .credential()
            .map_err(|e| format!("Failed to create Azure credential for Kusto: {e}"))?;

        Ok(Self { credential })
    }

    /// Returns a valid Bearer access token string.
    pub(super) async fn get_token(&self) -> crate::Result<String> {
        let access_token = self
            .credential
            .get_token(&[KUSTO_SCOPE], None)
            .await
            .map_err(|e| format!("Failed to acquire Azure token for Kusto: {e}"))?;

        Ok(access_token.token.secret().to_string())
    }
}
