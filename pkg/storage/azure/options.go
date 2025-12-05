package azure

import (
"net/http"

"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
)

// TableProviderOptions holds configuration options for Azure Storage Tables.
type TableProviderOptions struct {
Prefix                    string
Table                     string
Endpoint                  string
UseDefaultAzureCredential bool
SharedKeyCredential       *aztables.SharedKeyCredential
HTTPClient                *http.Client // Custom HTTP client for connection pooling
}

// StoreOption configures a TableProviderOptions.
type StoreOption func(*TableProviderOptions)

// WithPrefix sets the prefix used in the table name.
func WithPrefix(prefix string) StoreOption {
return func(o *TableProviderOptions) {
o.Prefix = prefix
}
}

// WithTable sets the logical table name (without prefix).
func WithTable(name string) StoreOption {
return func(o *TableProviderOptions) {
o.Table = name
}
}

// WithEndpoint sets the Azure Table endpoint URL.
func WithEndpoint(endpoint string) StoreOption {
return func(o *TableProviderOptions) {
o.Endpoint = endpoint
}
}

// WithDefaultCredential configures the store to use Azure Default Credential.
func WithDefaultCredential() StoreOption {
return func(o *TableProviderOptions) {
o.UseDefaultAzureCredential = true
}
}

// WithSharedKey configures the store to use a shared key credential.
func WithSharedKey(cred *aztables.SharedKeyCredential) StoreOption {
return func(o *TableProviderOptions) {
o.SharedKeyCredential = cred
}
}

// WithHTTPClient sets a custom HTTP client for connection pooling and keep-alive.
// This is important for performance in containerized environments where the default
// HTTP client may create new connections for each request.
// If not set, a default HTTP client with optimized connection pooling will be used.
func WithHTTPClient(client *http.Client) StoreOption {
return func(o *TableProviderOptions) {
o.HTTPClient = client
}
}
