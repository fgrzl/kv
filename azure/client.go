package azure

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
)

func NewSharedKeyCredential(accountName, accountKey string) (*aztables.SharedKeyCredential, error) {
	return aztables.NewSharedKeyCredential(accountName, accountKey)
}

func getClient(options *TableProviderOptions) (*aztables.Client, error) {
	name := sanitizeTableName(fmt.Sprintf("%s-%s", options.Prefix, options.Table))
	url := fmt.Sprintf("%s/%s", options.Endpoint, name)
	clientOptions := aztables.ClientOptions{}

	switch {
	case options.UseDefaultAzureCredential:
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
		return aztables.NewClient(url, cred, &clientOptions)

	case options.SharedKeyCredential != nil:
		if options.SharedKeyCredential.AccountName() == "devstoreaccount1" {
			clientOptions.InsecureAllowCredentialWithHTTP = true
		}
		return aztables.NewClientWithSharedKey(url, options.SharedKeyCredential, &clientOptions)

	default:
		return nil, fmt.Errorf("missing credential")
	}
}

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
