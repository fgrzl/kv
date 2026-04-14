// Package azure provides an Azure Table Storage backend for kv.KV using a
// lightweight HTTP client and optional credentials (shared key or managed identity).
package azure

import (
	"net/http"

	"github.com/fgrzl/azkit/credentials"
	"github.com/fgrzl/kv/pkg/valuecodec"
)

// TableProviderOptions holds configuration options for Azure Storage Tables.
type TableProviderOptions struct {
	Prefix                    string
	Table                     string
	Endpoint                  string
	SharedKeyCredential       *credentials.SharedKeyCredential
	ManagedIdentityCredential *credentials.ManagedIdentityCredential
	HTTPClient                *http.Client
	ValueCodec                *valuecodec.Codec
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

// WithSharedKey configures the store to use a shared key credential.
func WithSharedKey(cred *credentials.SharedKeyCredential) StoreOption {
	return func(o *TableProviderOptions) {
		o.SharedKeyCredential = cred
	}
}

// WithManagedIdentity configures the store to use Azure Managed Identity.
func WithManagedIdentity(cred *credentials.ManagedIdentityCredential) StoreOption {
	return func(o *TableProviderOptions) {
		o.ManagedIdentityCredential = cred
	}
}

// WithHTTPClient sets a custom HTTP client for connection pooling and keep-alive.
// If not set, a default HTTP client with optimized connection pooling will be used.
func WithHTTPClient(client *http.Client) StoreOption {
	return func(o *TableProviderOptions) {
		o.HTTPClient = client
	}
}

func WithDefaultValueCompression() StoreOption {
	return WithValueCompression(valuecodec.DefaultConfig())
}

func WithValueCompression(config valuecodec.Config) StoreOption {
	return func(o *TableProviderOptions) {
		o.ValueCodec = valuecodec.New(config)
	}
}

func WithoutValueCompression() StoreOption {
	return func(o *TableProviderOptions) {
		o.ValueCodec = valuecodec.New(valuecodec.DisabledConfig())
	}
}
