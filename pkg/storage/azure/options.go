package azure

import "github.com/Azure/azure-sdk-for-go/sdk/data/aztables"

// TableProviderOptions holds configuration options for Azure Storage Tables.
type TableProviderOptions struct {
	Prefix                    string
	Table                     string
	Endpoint                  string
	UseDefaultAzureCredential bool
	SharedKeyCredential       *aztables.SharedKeyCredential
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
