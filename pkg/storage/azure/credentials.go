package azure

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// TokenRefreshBuffer is the time before expiry when we refresh the token
	TokenRefreshBuffer = 5 * time.Minute
	// IMDSRequestTimeout is the timeout for IMDS requests
	IMDSRequestTimeout = 10 * time.Second
	// IMDSAPIVersion is the API version for Azure IMDS
	IMDSAPIVersion = "2018-02-01"
	// AppServiceAPIVersion is the API version for App Service / Container Apps token endpoint
	AppServiceAPIVersion = "2019-08-01"
)

// SharedKeyCredential holds Azure Table Storage account credentials
// This is a lightweight replacement for the Azure SDK type
type SharedKeyCredential struct {
	AccountName string
	AccountKey  string
}

// NewSharedKeyCredential creates a new shared key credential
func NewSharedKeyCredential(accountName, accountKey string) (*SharedKeyCredential, error) {
	return &SharedKeyCredential{
		AccountName: accountName,
		AccountKey:  accountKey,
	}, nil
}

// ManagedIdentityCredential acquires tokens from Azure IMDS or App Service/Container Apps endpoint
// This provides DefaultCredential-like behavior for Azure Container Apps and VMs
type ManagedIdentityCredential struct {
	clientID         string
	token            string
	tokenExpiry      time.Time
	mu               sync.RWMutex
	httpClient       *http.Client
	imdsEndpoint     string
	identityEndpoint string // from IDENTITY_ENDPOINT env var (App Service / ACA)
	identityHeader   string // from IDENTITY_HEADER env var
	useAppService    bool   // true when running in ACA / App Service
}

// imdsTokenResponse represents the response from Azure IMDS
type imdsTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   string `json:"expires_in"`
	ExpiresOn   string `json:"expires_on"`
	Resource    string `json:"resource"`
	TokenType   string `json:"token_type"`
	ClientID    string `json:"client_id"`
}

// NewManagedIdentityCredential creates a credential that uses Azure Managed Identity.
// When IDENTITY_ENDPOINT and IDENTITY_HEADER are set (App Service / Container Apps), uses that endpoint;
// otherwise falls back to IMDS (VMs / VMSS).
func NewManagedIdentityCredential(clientID string) *ManagedIdentityCredential {
	cred := &ManagedIdentityCredential{
		clientID:   clientID,
		httpClient: &http.Client{Timeout: IMDSRequestTimeout},
	}

	identityEndpoint := os.Getenv("IDENTITY_ENDPOINT")
	identityHeader := os.Getenv("IDENTITY_HEADER")

	if identityEndpoint != "" && identityHeader != "" {
		cred.identityEndpoint = identityEndpoint
		cred.identityHeader = identityHeader
		cred.useAppService = true
		slog.Info("managed identity: using App Service/Container Apps endpoint",
			"endpoint", identityEndpoint)
	} else {
		cred.imdsEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token"
		slog.Info("managed identity: using IMDS endpoint")
	}

	return cred
}

// GetToken retrieves a valid access token, refreshing if necessary
func (c *ManagedIdentityCredential) GetToken(ctx context.Context) (string, error) {
	// Check if we have a cached token that's still valid (fast path with read lock)
	c.mu.RLock()
	cachedToken := c.token
	cachedExpiry := c.tokenExpiry
	c.mu.RUnlock()

	// Check validity outside the lock to avoid race condition
	if cachedToken != "" && time.Now().Before(cachedExpiry.Add(-TokenRefreshBuffer)) {
		return cachedToken, nil
	}

	// Need to acquire or refresh token
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have refreshed)
	if c.token != "" && time.Now().Before(c.tokenExpiry.Add(-TokenRefreshBuffer)) {
		return c.token, nil
	}

	// Build token request: App Service/ACA vs IMDS
	query := url.Values{}
	query.Set("resource", "https://storage.azure.com/")
	if c.clientID != "" {
		query.Set("client_id", c.clientID)
	}

	var reqURL string
	endpointType := "IMDS"
	if c.useAppService {
		query.Set("api-version", AppServiceAPIVersion)
		reqURL = fmt.Sprintf("%s?%s", c.identityEndpoint, query.Encode())
		endpointType = "AppService"
	} else {
		query.Set("api-version", IMDSAPIVersion)
		reqURL = fmt.Sprintf("%s?%s", c.imdsEndpoint, query.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create %s request: %w", endpointType, err)
	}

	if c.useAppService {
		req.Header.Set("X-IDENTITY-HEADER", c.identityHeader)
	} else {
		req.Header.Set("Metadata", "true")
	}

	slog.Debug("requesting managed identity token", "client_id", c.clientID, "endpoint_type", endpointType)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Warn("failed to call managed identity endpoint", "error", err, "endpoint_type", endpointType)
		return "", fmt.Errorf("failed to call %s endpoint: %w", endpointType, err)
	}
	defer resp.Body.Close()

	// Extract request ID for debugging
	requestID := resp.Header.Get("x-ms-request-id")

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Error("managed identity endpoint returned error",
			"status", resp.StatusCode,
			"request_id", requestID,
			"endpoint_type", endpointType,
			"body", string(body))
		return "", fmt.Errorf("%s returned status %d (request_id=%s): %s", endpointType, resp.StatusCode, requestID, string(body))
	}

	var tokenResp imdsTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		slog.Error("failed to decode token response", "error", err, "request_id", requestID, "endpoint_type", endpointType)
		return "", fmt.Errorf("failed to decode token response: %w", err)
	}

	// Parse expiry time (Unix timestamp as string) - use strconv for better performance
	expiresOn, err := strconv.ParseInt(tokenResp.ExpiresOn, 10, 64)
	if err != nil {
		slog.Error("failed to parse token expiry", "error", err, "expires_on", tokenResp.ExpiresOn, "endpoint_type", endpointType)
		return "", fmt.Errorf("failed to parse token expiry: %w", err)
	}

	c.tokenExpiry = time.Unix(expiresOn, 0)
	c.token = tokenResp.AccessToken

	// Log JWT claims for diagnostic purposes (audience, issuer, tenant, identity)
	logJWTClaims(c.token, endpointType)

	slog.Debug("successfully refreshed managed identity token",
		"expires_at", c.tokenExpiry,
		"request_id", requestID,
		"endpoint_type", endpointType)

	return c.token, nil
}

// logJWTClaims decodes and logs key claims from a JWT access token for diagnostic purposes.
// This helps identify misconfigurations such as wrong audience, tenant, or identity.
func logJWTClaims(token, endpointType string) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 2 {
		slog.Warn("managed identity: token is not a valid JWT (cannot inspect claims)",
			"endpoint_type", endpointType)
		return
	}

	// JWT payload is base64url-encoded (no padding)
	payload := parts[1]
	// Add padding if necessary
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		slog.Warn("managed identity: failed to decode JWT payload",
			"error", err,
			"endpoint_type", endpointType)
		return
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		slog.Warn("managed identity: failed to parse JWT claims",
			"error", err,
			"endpoint_type", endpointType)
		return
	}

	// Extract diagnostic fields — these reveal the most common misconfigurations
	aud, _ := claims["aud"].(string)
	iss, _ := claims["iss"].(string)
	oid, _ := claims["oid"].(string)
	sub, _ := claims["sub"].(string)
	tid, _ := claims["tid"].(string)
	appid, _ := claims["appid"].(string)

	slog.Info("managed identity: JWT claims (diagnostic)",
		"aud", aud,
		"iss", iss,
		"oid", oid,
		"sub", sub,
		"tid", tid,
		"appid", appid,
		"endpoint_type", endpointType,
	)

	// Warn about common misconfigurations
	expectedAud := "https://storage.azure.com"
	if aud != "" && aud != expectedAud && aud != expectedAud+"/" {
		slog.Error("managed identity: TOKEN AUDIENCE MISMATCH — this will cause 403 AuthenticationFailed",
			"got_aud", aud,
			"expected_aud", expectedAud,
			"endpoint_type", endpointType,
		)
	}
}
