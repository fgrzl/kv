package azure

import (
	"fmt"
	"net/url"
	"strings"
)

// getClient builds an HTTPTableClient from store options.
// If TableProviderOptions.HTTPClient is set, it is used instead of the default transport.
func getClient(options *TableProviderOptions) (*HTTPTableClient, error) {
	if options == nil {
		return nil, fmt.Errorf("options required")
	}
	if options.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}

	name := sanitizeTableName(fmt.Sprintf("%s-%s", options.Prefix, options.Table))
	allowInsecure := strings.HasPrefix(options.Endpoint, "http://")

	var client *HTTPTableClient
	var err error

	switch {
	case options.SharedKeyCredential != nil:
		client, err = NewHTTPTableClient(
			options.SharedKeyCredential.AccountName,
			options.SharedKeyCredential.AccountKey,
			name,
			allowInsecure,
			options.Endpoint,
		)

	case options.ManagedIdentityCredential != nil:
		accountName, parseErr := parseAccountName(options.Endpoint)
		if parseErr != nil {
			return nil, fmt.Errorf("cannot derive account name from endpoint: %w", parseErr)
		}
		client, err = NewHTTPTableClientWithManagedIdentity(
			accountName,
			options.ManagedIdentityCredential,
			name,
			allowInsecure,
			options.Endpoint,
		)

	default:
		return nil, fmt.Errorf("missing credential: provide SharedKeyCredential or ManagedIdentityCredential")
	}

	if err != nil {
		return nil, err
	}
	if options.HTTPClient != nil {
		client.httpClient = options.HTTPClient
	}
	return client, nil
}

// parseAccountName extracts the storage account name from an endpoint URL.
// Supports both "https://<account>.table.core.windows.net" and custom endpoints
// like "http://127.0.0.1:10002/<account>".
func parseAccountName(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	// Standard Azure endpoint: account is the first subdomain
	host := u.Hostname()
	if parts := strings.SplitN(host, ".", 2); len(parts) == 2 && parts[1] != "" {
		return parts[0], nil
	}
	// Azurite / custom endpoint: account is the first path segment
	segments := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(segments) > 0 && segments[0] != "" {
		return segments[0], nil
	}
	return "", fmt.Errorf("cannot determine account name from %q", endpoint)
}

// sanitizeTableName normalizes a table name to Azure Table Storage rules:
// must start with a letter, contain only alphanumerics, length 3–63.
func sanitizeTableName(name string) string {
	if len(name) == 0 {
		return "T00"
	}
	out := []byte{}
	first := name[0]
	if isLetter(first) {
		out = append(out, first)
	} else {
		out = append(out, 'T')
	}
	for i := 1; i < len(name); i++ {
		if isAlphanumeric(name[i]) {
			out = append(out, name[i])
		}
	}
	for len(out) < 3 {
		out = append(out, '0')
	}
	if len(out) > 63 {
		out = out[:63]
	}
	return string(out)
}

func isLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isAlphanumeric(c byte) bool {
	return isLetter(c) || (c >= '0' && c <= '9')
}
