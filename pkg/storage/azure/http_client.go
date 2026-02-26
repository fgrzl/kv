package azure

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// HTTP client configuration
	HTTPRequestTimeout      = 30 * time.Second
	HTTPConnectTimeout      = 5 * time.Second
	HTTPKeepAlive           = 30 * time.Second
	HTTPIdleConnTimeout     = 90 * time.Second
	HTTPTLSHandshakeTimeout = 10 * time.Second
	HTTPMaxIdleConns        = 100
	HTTPMaxIdleConnsPerHost = 100

	// Azure Table Storage limits
	AzureBatchMaxEntities = 100
	AzureDefaultPageSize  = 1000
	AzureAPIVersion       = "2021-06-08"

	// Retry configuration
	MaxRetryAttempts  = 3
	InitialRetryDelay = 100 * time.Millisecond
	MaxRetryDelay     = 10 * time.Second

	// Buffer pool size hint
	BufferPoolDefaultSize = 32 * 1024 // 32KB
)

// AzureError represents a structured error from Azure Table Storage
type AzureError struct {
	StatusCode int
	RequestID  string
	Message    string
	Code       string
}

func (e *AzureError) Error() string {
	return fmt.Sprintf("azure table error: status=%d code=%s request_id=%s message=%s",
		e.StatusCode, e.Code, e.RequestID, e.Message)
}

// IsTransient returns true if the error is likely transient and retryable
func (e *AzureError) IsTransient() bool {
	return e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode == http.StatusServiceUnavailable ||
		e.StatusCode == http.StatusRequestTimeout
}

// ErrorResponse represents Azure error response body
type ErrorResponse struct {
	ODataError struct {
		Code    string `json:"code"`
		Message struct {
			Lang  string `json:"lang"`
			Value string `json:"value"`
		} `json:"message"`
	} `json:"odata.error"`
}

// parseAzureError extracts structured error information from Azure response.
// Supports both JSON (OData) and XML error formats — Azure Table Storage can
// return either depending on the request headers and error type.
func parseAzureError(resp *http.Response, body []byte) *AzureError {
	azErr := &AzureError{
		StatusCode: resp.StatusCode,
		RequestID:  resp.Header.Get("x-ms-request-id"),
		Message:    string(body),
	}

	// Try JSON (OData) format first
	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil {
		if errResp.ODataError.Code != "" {
			azErr.Code = errResp.ODataError.Code
			azErr.Message = errResp.ODataError.Message.Value
			return azErr
		}
	}

	// Fallback: Try XML format (Azure Table Storage returns XML for auth errors)
	bodyStr := string(body)
	if strings.Contains(bodyStr, "<m:code>") {
		if start := strings.Index(bodyStr, "<m:code>"); start != -1 {
			start += len("<m:code>")
			if end := strings.Index(bodyStr[start:], "</m:code>"); end != -1 {
				azErr.Code = bodyStr[start : start+end]
			}
		}
		if start := strings.Index(bodyStr, "<m:message"); start != -1 {
			if tagEnd := strings.Index(bodyStr[start:], ">"); tagEnd != -1 {
				msgStart := start + tagEnd + 1
				if end := strings.Index(bodyStr[msgStart:], "</m:message>"); end != -1 {
					azErr.Message = bodyStr[msgStart : msgStart+end]
				}
			}
		}
	}

	// Log additional response headers on auth failures for diagnostics
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		diagHeaders := make(map[string]string)
		for _, key := range []string{
			"x-ms-error-code",
			"x-ms-request-id",
			"WWW-Authenticate",
			"x-ms-version",
			"Server",
			"Date",
		} {
			if v := resp.Header.Get(key); v != "" {
				diagHeaders[key] = v
			}
		}
		slog.Error("azure auth failure diagnostic",
			"status", resp.StatusCode,
			"code", azErr.Code,
			"request_id", azErr.RequestID,
			"response_headers", diagHeaders,
			"message", azErr.Message,
		)
	}

	return azErr
}

// parseBatchResponse parses the multipart/mixed body of an Azure Table Storage
// batch response to detect per-entity failures. Azure returns 202 Accepted for
// the batch envelope even when individual entities within a changeset fail
// (e.g., 409 Conflict, 413 Entity Too Large, quota errors).
func parseBatchResponse(respBody []byte) error {
	lines := strings.Split(string(respBody), "\n")
	var entityErrors []string
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "HTTP/1.1 ") {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			continue
		}
		statusCode, err := strconv.Atoi(parts[1])
		if err != nil || statusCode < 400 {
			continue
		}
		// Found a failure — look for JSON error details in following lines
		detail := ""
		for j := i + 1; j < len(lines); j++ {
			trimmed := strings.TrimSpace(lines[j])
			if strings.HasPrefix(trimmed, "--") {
				break
			}
			if strings.HasPrefix(trimmed, "{") {
				detail = trimmed
				break
			}
		}
		if detail != "" {
			entityErrors = append(entityErrors, fmt.Sprintf("status %d: %s", statusCode, detail))
		} else {
			entityErrors = append(entityErrors, line)
		}
	}
	if len(entityErrors) > 0 {
		return fmt.Errorf("batch entity failures: %s", strings.Join(entityErrors, "; "))
	}
	return nil
}

// getRetryDelay calculates exponential backoff delay with optional Retry-After header
func getRetryDelay(attempt int, resp *http.Response) time.Duration {
	// Check Retry-After header for 429 responses
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				return time.Duration(seconds) * time.Second
			}
		}
	}

	// Exponential backoff
	delay := InitialRetryDelay * (1 << uint(attempt))
	if delay > MaxRetryDelay {
		delay = MaxRetryDelay
	}
	return delay
}

// retryableRequest executes a request with automatic retry on transient failures
// Returns the response and any non-retryable error
func (c *HTTPTableClient) retryableRequest(ctx context.Context, req *http.Request, body []byte) (*http.Response, error) {
	var lastErr error
	var resp *http.Response

	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		// Check context cancellation before each attempt
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context cancelled: %w", err)
		}

		// Clone request for retry (body needs to be re-readable)
		var reqBody io.Reader
		if len(body) > 0 {
			reqBody = bytes.NewReader(body)
		}
		retryReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL.String(), reqBody)
		if err != nil {
			return nil, fmt.Errorf("create retry request: %w", err)
		}

		// Copy headers
		retryReq.Header = req.Header.Clone()

		resp, err = c.httpClient.Do(retryReq)
		if err != nil {
			// Network error - check if retryable
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() && attempt < MaxRetryAttempts-1 {
				delay := getRetryDelay(attempt, nil)
				slog.Warn("request timeout, retrying",
					"attempt", attempt+1,
					"delay", delay,
					"error", err)
				// Issue #16: Use context-aware sleep instead of blocking time.Sleep
				select {
				case <-time.After(delay):
					// Sleep completed
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				lastErr = err
				continue
			}
			return nil, fmt.Errorf("request failed: %w", err)
		}

		// Check for transient HTTP errors
		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusRequestTimeout {

			if attempt < MaxRetryAttempts-1 {
				delay := getRetryDelay(attempt, resp)
				respBody, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				slog.Warn("transient error, retrying",
					"status", resp.StatusCode,
					"attempt", attempt+1,
					"delay", delay,
					"request_id", resp.Header.Get("x-ms-request-id"))

				// Issue #16: Use context-aware sleep
				select {
				case <-time.After(delay):
					// Sleep completed
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				lastErr = parseAzureError(resp, respBody)
				continue
			} else {
				// Final attempt failed
				respBody, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				return nil, parseAzureError(resp, respBody)
			}
		}

		// Success or non-retryable error
		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
	}
	return nil, errors.New("max retries exceeded")
}

var (
	// bufferPool for reusing byte buffers with pre-allocated capacity
	bufferPool = sync.Pool{
		New: func() interface{} {
			buf := bytes.NewBuffer(make([]byte, 0, BufferPoolDefaultSize))
			return buf
		},
	}

	// builderPool for reusing strings.Builder with pre-allocated capacity
	builderPool = sync.Pool{
		New: func() interface{} {
			b := &strings.Builder{}
			b.Grow(512) // Pre-allocate reasonable URL size
			return b
		},
	}
)

// HTTPTableClient provides a lightweight native Go HTTP client for Azure Table Storage REST API
type HTTPTableClient struct {
	endpoint       string
	tableName      string
	accountName    string
	accountKey     string
	sasToken       string                     // SAS token for auth (alternative to SharedKey)
	managedCred    *ManagedIdentityCredential // For Bearer token auth
	httpClient     *http.Client
	allowInsecure  bool
	useBearerToken bool // true if using Managed Identity
	useSAS         bool // true if using SAS token
}

// NewHTTPTableClient creates a new HTTP-based Table Storage client with SharedKey authentication
func NewHTTPTableClient(accountName, accountKey, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account name and key are required")
	}

	// Validate that the account key is valid base64 (required for HMAC-SHA256 signing)
	if _, err := base64.StdEncoding.DecodeString(accountKey); err != nil {
		return nil, fmt.Errorf("account key must be valid base64: %w", err)
	}

	var endpoint string
	if customEndpoint != "" {
		endpoint = customEndpoint
	} else {
		endpoint = fmt.Sprintf("https://%s.table.core.windows.net", accountName)
		if allowInsecure {
			endpoint = fmt.Sprintf("http://%s.table.core.windows.net", accountName)
		}
	}

	return &HTTPTableClient{
		endpoint:       endpoint,
		tableName:      tableName,
		accountName:    accountName,
		accountKey:     accountKey,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: false,
		useSAS:         false,
	}, nil
}

// NewHTTPTableClientWithSAS creates a client with SAS token authentication
func NewHTTPTableClientWithSAS(accountName, sasToken, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" || sasToken == "" {
		return nil, fmt.Errorf("account name and SAS token are required")
	}

	var endpoint string
	if customEndpoint != "" {
		endpoint = customEndpoint
	} else {
		endpoint = fmt.Sprintf("https://%s.table.core.windows.net", accountName)
		if allowInsecure {
			endpoint = fmt.Sprintf("http://%s.table.core.windows.net", accountName)
		}
	}

	return &HTTPTableClient{
		endpoint:       endpoint,
		tableName:      tableName,
		accountName:    accountName,
		sasToken:       sasToken,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: false,
		useSAS:         true,
	}, nil
}

// NewHTTPTableClientWithManagedIdentity creates a client with Managed Identity (Bearer token) authentication
func NewHTTPTableClientWithManagedIdentity(accountName string, managedCred *ManagedIdentityCredential, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" {
		return nil, fmt.Errorf("account name is required")
	}
	if managedCred == nil {
		return nil, fmt.Errorf("managed identity credential is required")
	}

	var endpoint string
	if customEndpoint != "" {
		endpoint = customEndpoint
	} else {
		endpoint = fmt.Sprintf("https://%s.table.core.windows.net", accountName)
		if allowInsecure {
			endpoint = fmt.Sprintf("http://%s.table.core.windows.net", accountName)
		}
	}

	return &HTTPTableClient{
		endpoint:       endpoint,
		tableName:      tableName,
		accountName:    accountName,
		managedCred:    managedCred,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: true,
		useSAS:         false,
	}, nil
}

// newOptimizedHTTPClient creates an HTTP client optimized for high throughput
// Disables compression since stored data is already compressed (zstd events)
func newOptimizedHTTPClient() *http.Client {
	return &http.Client{
		Timeout: HTTPRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        HTTPMaxIdleConns,
			MaxIdleConnsPerHost: HTTPMaxIdleConnsPerHost,
			IdleConnTimeout:     HTTPIdleConnTimeout,
			DisableCompression:  true, // Data is already compressed
			DialContext: (&net.Dialer{
				Timeout:   HTTPConnectTimeout,
				KeepAlive: HTTPKeepAlive,
			}).DialContext,
			TLSHandshakeTimeout:   HTTPTLSHandshakeTimeout,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}
}

// Entity represents a table storage entity with metadata
type entity struct {
	PartitionKey string `json:"PartitionKey"`
	RowKey       string `json:"RowKey"`
	Value        []byte `json:"Value,omitempty"`
	Timestamp    string `json:"Timestamp,omitempty"`
}

// CreateTable ensures the table exists
func (c *HTTPTableClient) CreateTable(ctx context.Context) error {
	body := map[string]string{"TableName": c.tableName}
	data, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint+"/Tables", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", data, "/Tables")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, data)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Table already exists is acceptable (409)
	if resp.StatusCode == http.StatusConflict {
		slog.Debug("table already exists", "table", c.tableName)
		return nil
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	slog.Info("table created", "table", c.tableName, "request_id", resp.Header.Get("x-ms-request-id"))
	return nil
}

// AddEntity inserts a new entity (insert-only semantics)
func (c *HTTPTableClient) AddEntity(ctx context.Context, data []byte) error {
	var e entity
	if err := json.Unmarshal(data, &e); err != nil {
		return fmt.Errorf("unmarshal entity: %w", err)
	}

	reqBody := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
	}
	if len(e.Value) > 0 {
		reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	enc := json.NewEncoder(buf)
	if err := enc.Encode(reqBody); err != nil {
		return fmt.Errorf("encode entity: %w", err)
	}

	// Use odata=nometadata for minimal response
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	// Pre-calculate and grow to exact size
	expectedLen := len(c.endpoint) + 1 + len(c.tableName)
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", buf.Bytes(), fmt.Sprintf("/%s", c.tableName))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, buf.Bytes())
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	return nil
}

// AddEntityBatch inserts multiple entities in a single batch request (up to 100)
// Uses multipart/mixed format for efficient bulk inserts
// Issue #1: Changed from POST (insert) to PUT (upsert) to make WAL recovery idempotent.
// Without this, WAL recovery fails on already-inserted entries with 409 Conflict,
// making the store permanently unopenable after a crash at the wrong moment.
func (c *HTTPTableClient) AddEntityBatch(ctx context.Context, entities [][]byte) error {
	if len(entities) == 0 {
		return nil
	}
	if len(entities) > AzureBatchMaxEntities {
		return fmt.Errorf("batch size %d exceeds maximum of %d entities", len(entities), AzureBatchMaxEntities)
	}

	// Generate batch and changeset boundaries
	batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	changesetID := fmt.Sprintf("changeset_%d", time.Now().UnixNano())

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Pre-allocate buffer with reasonable estimate (reduce allocations)
	// Rough estimate: 500 bytes per entity + batch overhead
	estimatedSize := len(entities)*500 + 1024
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	// Build multipart/mixed request
	fmt.Fprintf(buf, "--%s\r\n", batchID)
	fmt.Fprintf(buf, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", changesetID)

	for i, entData := range entities {
		var e entity
		if err := json.Unmarshal(entData, &e); err != nil {
			return fmt.Errorf("failed to unmarshal entity %d: %w", i, err)
		}

		reqBody := map[string]interface{}{
			"PartitionKey": e.PartitionKey,
			"RowKey":       e.RowKey,
		}
		if len(e.Value) > 0 {
			reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
		}

		entJSON, _ := json.Marshal(reqBody)

		fmt.Fprintf(buf, "--%s\r\n", changesetID)
		fmt.Fprintf(buf, "Content-Type: application/http\r\n")
		fmt.Fprintf(buf, "Content-Transfer-Encoding: binary\r\n\r\n")
		// Use PUT instead of POST to enable upsert (create or replace) semantics
		// This makes WAL recovery idempotent and prevents 409 Conflict errors
		fmt.Fprintf(buf, "PUT %s/%s(PartitionKey='%s',RowKey='%s') HTTP/1.1\r\n", c.endpoint, c.tableName,
			url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey))
		fmt.Fprintf(buf, "Content-Type: application/json\r\n")
		fmt.Fprintf(buf, "Accept: application/json;odata=nometadata\r\n")
		fmt.Fprintf(buf, "Content-Length: %d\r\n\r\n", len(entJSON))
		buf.Write(entJSON)
		buf.WriteString("\r\n")
	}

	fmt.Fprintf(buf, "--%s--\r\n", changesetID)
	fmt.Fprintf(buf, "--%s--\r\n", batchID)

	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 7 // "/$batch"
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteString("/$batch")
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", buf.Bytes(), "/$batch")
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchID))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, buf.Bytes())
	if err != nil {
		return fmt.Errorf("execute batch request: %w", err)
	}
	defer resp.Body.Close()

	// Always read the body — needed both for error responses and for
	// Issue #14: checking per-entity failures inside a 202 Accepted envelope.
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusAccepted {
		return parseAzureError(resp, respBody)
	}

	// Issue #14: Parse the multipart/mixed response to detect per-entity failures.
	// Azure returns 202 for the batch envelope even when individual entities fail.
	if err := parseBatchResponse(respBody); err != nil {
		return err
	}

	slog.Debug("batch entities added",
		"count", len(entities),
		"request_id", resp.Header.Get("x-ms-request-id"))

	return nil
}

// BatchOpType distinguishes the operation kind inside a batch changeset.
type BatchOpType int

const (
	BatchInsertReplace BatchOpType = iota
	BatchDelete
)

// BatchOp represents a single operation within a mixed batch transaction.
type BatchOp struct {
	Type         BatchOpType
	Entity       []byte // JSON entity payload (InsertReplace only)
	PartitionKey string // used by Delete
	RowKey       string // used by Delete
}

// SubmitBatch executes a mixed batch of InsertReplace (PUT) and Delete operations
// in a single Azure Table Storage $batch request with one changeset.
// All operations must target the same partition. For delete-only batches, individual
// DeleteEntity calls are used to avoid Azurite/emulator issues with batch DELETEs.
func (c *HTTPTableClient) SubmitBatch(ctx context.Context, ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	if len(ops) > AzureBatchMaxEntities {
		return fmt.Errorf("batch size %d exceeds maximum of %d entities", len(ops), AzureBatchMaxEntities)
	}

	// Azurite returns CommandsInBatchActOnDifferentPartitions for delete-only batches
	// even when all keys share the same partition. Use individual deletes for delete-only.
	allDelete := true
	for _, op := range ops {
		if op.Type != BatchDelete {
			allDelete = false
			break
		}
	}
	if allDelete {
		for _, op := range ops {
			if err := c.DeleteEntity(ctx, op.PartitionKey, op.RowKey); err != nil {
				return err
			}
		}
		return nil
	}

	batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	changesetID := fmt.Sprintf("changeset_%d", time.Now().UnixNano())

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	estimatedSize := len(ops)*500 + 1024
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	fmt.Fprintf(buf, "--%s\r\n", batchID)
	fmt.Fprintf(buf, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", changesetID)

	for i, op := range ops {
		fmt.Fprintf(buf, "--%s\r\n", changesetID)
		fmt.Fprintf(buf, "Content-Type: application/http\r\n")
		fmt.Fprintf(buf, "Content-Transfer-Encoding: binary\r\n\r\n")

		switch op.Type {
		case BatchInsertReplace:
			var e entity
			if err := json.Unmarshal(op.Entity, &e); err != nil {
				return fmt.Errorf("failed to unmarshal entity %d: %w", i, err)
			}
			reqBody := map[string]interface{}{
				"PartitionKey": e.PartitionKey,
				"RowKey":       e.RowKey,
			}
			if len(e.Value) > 0 {
				reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
			}
			entJSON, _ := json.Marshal(reqBody)
			fmt.Fprintf(buf, "PUT %s/%s(PartitionKey='%s',RowKey='%s') HTTP/1.1\r\n",
				c.endpoint, c.tableName,
				url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey))
			fmt.Fprintf(buf, "Content-Type: application/json\r\n")
			fmt.Fprintf(buf, "Accept: application/json;odata=nometadata\r\n")
			fmt.Fprintf(buf, "Content-Length: %d\r\n\r\n", len(entJSON))
			buf.Write(entJSON)
			buf.WriteString("\r\n")

		case BatchDelete:
			fmt.Fprintf(buf, "DELETE %s/%s(PartitionKey='%s',RowKey='%s') HTTP/1.1\r\n",
				c.endpoint, c.tableName,
				url.QueryEscape(op.PartitionKey), url.QueryEscape(op.RowKey))
			fmt.Fprintf(buf, "If-Match: *\r\n")
			fmt.Fprintf(buf, "Accept: application/json;odata=nometadata\r\n\r\n")
		}
	}

	fmt.Fprintf(buf, "--%s--\r\n", changesetID)
	fmt.Fprintf(buf, "--%s--\r\n", batchID)

	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 7
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteString("/$batch")
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", buf.Bytes(), "/$batch")
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchID))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, buf.Bytes())
	if err != nil {
		return fmt.Errorf("execute batch request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusAccepted {
		return parseAzureError(resp, respBody)
	}

	if err := parseBatchResponse(respBody); err != nil {
		return err
	}

	slog.Debug("batch submitted",
		"count", len(ops),
		"request_id", resp.Header.Get("x-ms-request-id"))

	return nil
}

// UpsertEntity updates or inserts an entity (merge semantics)
func (c *HTTPTableClient) UpsertEntity(ctx context.Context, data []byte, mode string) error {
	var e entity
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}

	reqBody := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
	}
	if len(e.Value) > 0 {
		reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
	}

	// Use buffer pool for JSON encoding
	bodyBuf := bufferPool.Get().(*bytes.Buffer)
	bodyBuf.Reset()
	defer bufferPool.Put(bodyBuf)

	if err := json.NewEncoder(bodyBuf).Encode(reqBody); err != nil {
		return err
	}
	body := bodyBuf.Bytes()

	// Build URL efficiently
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	// Pre-calculate URL length for efficiency
	expectedLen := len(c.endpoint) + 1 + len(c.tableName) + 16 + // "(PartitionKey=''"
		len(e.PartitionKey)*3 + 11 + len(e.RowKey)*3 + 2 // URL escape worst case is 3x
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(e.PartitionKey))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(e.RowKey))
	b.WriteString("')")

	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	method := "PATCH" // Merge (update if exists, insert if not)
	if mode == "Replace" {
		method = "PUT" // Replace (overwrite if exists, insert if not)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}

	c.signRequest(req, method, body, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, body)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	return nil
}

// DeleteEntity removes an entity
func (c *HTTPTableClient) DeleteEntity(ctx context.Context, pk, rk string) error {
	// Build URL efficiently
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(pk))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(rk))
	b.WriteString("')")

	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}

	c.signRequest(req, "DELETE", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)))
	req.Header.Set("If-Match", "*") // Delete regardless of etag

	resp, err := c.retryableRequest(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// 404 is acceptable (entity doesn't exist)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	return nil
}

// GetEntity retrieves a single entity
func (c *HTTPTableClient) GetEntity(ctx context.Context, pk, rk string) ([]byte, error) {
	// Build URL efficiently
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 1 + len(c.tableName) + 16 +
		len(pk)*3 + 11 + len(rk)*3 + 2 + 13 // "?$format=json"
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(pk))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(rk))
	b.WriteString("')")

	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	} else {
		b.WriteString("?$format=json")
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	c.signRequest(req, "GET", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("ResourceNotFound")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, parseAzureError(resp, body)
	}

	var result entity
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Re-encode as JSON byte slice
	return json.Marshal(result)
}

// ListEntitiesPager provides pagination for list entities queries
type ListEntitiesPager struct {
	client            *HTTPTableClient
	filter            string
	selectCols        string
	top               int32
	continuationToken string
	ctx               context.Context
	currentPage       []entity
	pageIndex         int
	done              bool
}

// NewListEntitiesPager creates a new pager for listing entities
func (c *HTTPTableClient) NewListEntitiesPager(filter, selectCols string, top int32) *ListEntitiesPager {
	if top == 0 || top > AzureDefaultPageSize {
		top = AzureDefaultPageSize
	}
	return &ListEntitiesPager{
		client:     c,
		filter:     filter,
		selectCols: selectCols,
		top:        top,
		pageIndex:  -1,
	}
}

// PageResponse represents a page response
type PageResponse struct {
	Value     []entity `json:"value"`
	NextToken string   `json:"odata.nextLink,omitempty"`
}

// FetchPage fetches the next page of results with efficient continuation token handling
func (p *ListEntitiesPager) FetchPage(ctx context.Context) ([]entity, error) {
	if p.done {
		return nil, nil
	}

	p.ctx = ctx

	// Build query URL efficiently with strings.Builder
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	// Pre-allocate with reasonable estimate
	estimatedLen := len(p.client.endpoint) + 1 + len(p.client.tableName) + 256
	if p.client.useSAS {
		estimatedLen += len(p.client.sasToken)
	}
	b.Grow(estimatedLen)

	b.WriteString(p.client.endpoint)
	b.WriteByte('/')
	b.WriteString(p.client.tableName)
	b.WriteByte('?')
	b.WriteString("$top=")
	b.WriteString(strconv.Itoa(int(p.top)))

	if p.filter != "" {
		b.WriteString("&$filter=")
		b.WriteString(url.QueryEscape(p.filter))
	}
	if p.selectCols != "" {
		b.WriteString("&$select=")
		b.WriteString(url.QueryEscape(p.selectCols))
	}
	if p.continuationToken != "" {
		b.WriteByte('&')
		b.WriteString(p.continuationToken)
	}

	// Add SAS token if configured
	if p.client.useSAS {
		b.WriteByte('&')
		b.WriteString(p.client.sasToken)
	}

	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	p.client.signRequest(req, "GET", nil, fmt.Sprintf("/%s", p.client.tableName))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := p.client.retryableRequest(ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, parseAzureError(resp, body)
	}

	// Extract continuation tokens from response headers
	nextPK := resp.Header.Get("x-ms-continuation-NextPartitionKey")
	nextRK := resp.Header.Get("x-ms-continuation-NextRowKey")

	if nextPK != "" || nextRK != "" {
		// Build continuation token query string
		cb := builderPool.Get().(*strings.Builder)
		cb.Reset()
		if nextPK != "" {
			cb.WriteString("NextPartitionKey=")
			cb.WriteString(url.QueryEscape(nextPK))
		}
		if nextRK != "" {
			if cb.Len() > 0 {
				cb.WriteByte('&')
			}
			cb.WriteString("NextRowKey=")
			cb.WriteString(url.QueryEscape(nextRK))
		}
		p.continuationToken = cb.String()
		builderPool.Put(cb)
	} else {
		p.done = true
	}

	var pageResp PageResponse
	if err := json.NewDecoder(resp.Body).Decode(&pageResp); err != nil {
		return nil, err
	}

	p.pageIndex++
	return pageResp.Value, nil
}

// IsDone returns whether there are more pages
func (p *ListEntitiesPager) IsDone() bool {
	return p.done
}

// Close closes the pager
func (p *ListEntitiesPager) Close() error {
	return nil
}

// signRequest signs an HTTP request with either Bearer token or SharedKeyLite authentication.
// For Table service, this uses SharedKeyLite per the Azure SDK and REST API spec:
// https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
//
// SharedKeyLite StringToSign for Table service:
//
//	Date + "\n" + CanonicalizedResource
//
// CanonicalizedResource format (Table service / SharedKeyLite):
//
//	/<accountName>/<path>[?comp=<value>]
func (c *HTTPTableClient) signRequest(req *http.Request, method string, _ []byte, resourcePath string) {
	// Set standard headers
	req.Header.Set("x-ms-version", AzureAPIVersion)
	date := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("x-ms-date", date)

	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
		req.Header.Set("Content-Type", contentType)
	}

	// Use Bearer token authentication if Managed Identity is configured
	if c.useBearerToken && c.managedCred != nil {
		token, err := c.managedCred.GetToken(req.Context())
		if err != nil {
			// Issue #15: Log error clearly and set a sentinel header so failures are visible.
			// This prevents silent auth failures where a request proceeds without credentials.
			slog.Error("failed to acquire managed identity token",
				"error", err,
				"method", method,
				"path", resourcePath)
			// Set a sentinel header so any monitoring/debugging can detect auth failure
			req.Header.Set("X-Auth-Failure", "token_acquisition_failed")
			// Continue anyway - Azure will return 401, which is better than silently failing
			return
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		// Diagnostic logging for Bearer token requests
		tokenPrefix := token
		if len(tokenPrefix) > 20 {
			tokenPrefix = tokenPrefix[:20] + "..."
		}
		slog.Debug("azure bearer auth: request signed",
			"method", method,
			"url", req.URL.String(),
			"resource_path", resourcePath,
			"x-ms-version", req.Header.Get("x-ms-version"),
			"x-ms-date", req.Header.Get("x-ms-date"),
			"token_prefix", tokenPrefix,
			"account", c.accountName,
		)
		return
	}

	// SAS tokens do not require signing; the token is appended to the URL as a query parameter
	if c.useSAS {
		return
	}

	// Use SharedKeyLite authentication for Table service
	// Per the Azure REST API spec and the official Azure SDK for Go (aztables),
	// Table service uses SharedKeyLite with a simplified StringToSign.
	//
	// StringToSign = Date + "\n" + CanonicalizedResource
	// Authorization = "SharedKeyLite " + accountName + ":" + Base64(HMAC-SHA256(key, StringToSign))

	// Build CanonicalizedResource: /<accountName>/<path>[?comp=<value>]
	canonicalResource := fmt.Sprintf("/%s%s", c.accountName, resourcePath)

	// Include ?comp= query parameter if present (per spec)
	if req.URL != nil && req.URL.RawQuery != "" {
		params, err := url.ParseQuery(req.URL.RawQuery)
		if err == nil {
			if compVal, ok := params["comp"]; ok && len(compVal) > 0 {
				canonicalResource += "?comp=" + compVal[0]
			}
		}
	}

	// Build StringToSign per SharedKeyLite for Table service
	stringToSign := date + "\n" + canonicalResource

	// Compute HMAC-SHA256
	// Note: Account key validity is verified in NewHTTPTableClient,
	// so this should never fail in normal operation.
	decodedKey, err := base64.StdEncoding.DecodeString(c.accountKey)
	if err != nil {
		// This is a programming error (invalid key made it past factory validation)
		slog.Error("FATAL: account key failed to decode despite factory validation",
			"error", err,
			"method", method,
			"path", resourcePath,
			"account", c.accountName)
		req.Header.Set("X-Auth-Failure", "key_decode_error")
		return
	}

	h := hmac.New(sha256.New, decodedKey)
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Set Authorization header using SharedKeyLite scheme
	authHeader := fmt.Sprintf("SharedKeyLite %s:%s", c.accountName, signature)
	req.Header.Set("Authorization", authHeader)
}

// AuthDiagnostic contains the results of an auth diagnostic check
type AuthDiagnostic struct {
	AuthMode        string            `json:"auth_mode"`
	Endpoint        string            `json:"endpoint"`
	AccountName     string            `json:"account_name"`
	TableName       string            `json:"table_name"`
	TokenAcquired   bool              `json:"token_acquired,omitempty"`
	TokenError      string            `json:"token_error,omitempty"`
	TokenAudience   string            `json:"token_audience,omitempty"`
	TokenIssuer     string            `json:"token_issuer,omitempty"`
	TokenTenantID   string            `json:"token_tenant_id,omitempty"`
	TokenObjectID   string            `json:"token_object_id,omitempty"`
	RequestURL      string            `json:"request_url"`
	ResponseStatus  int               `json:"response_status"`
	ResponseHeaders map[string]string `json:"response_headers,omitempty"`
	ResponseBody    string            `json:"response_body,omitempty"`
	ErrorCode       string            `json:"error_code,omitempty"`
	ErrorMessage    string            `json:"error_message,omitempty"`
	Suggestion      string            `json:"suggestion,omitempty"`
}

// DiagnoseAuth performs a lightweight diagnostic request to help identify authentication
// and authorization issues. It attempts to list tables (GET /Tables?$top=1) and returns
// detailed information about the request, token, response, and suggested fixes.
func (c *HTTPTableClient) DiagnoseAuth(ctx context.Context) *AuthDiagnostic {
	diag := &AuthDiagnostic{
		Endpoint:    c.endpoint,
		AccountName: c.accountName,
		TableName:   c.tableName,
	}

	// Determine auth mode
	switch {
	case c.useBearerToken:
		diag.AuthMode = "ManagedIdentity (Bearer)"
	case c.useSAS:
		diag.AuthMode = "SAS"
	default:
		diag.AuthMode = "SharedKey"
	}

	// If using Bearer, inspect the token
	if c.useBearerToken && c.managedCred != nil {
		token, err := c.managedCred.GetToken(ctx)
		if err != nil {
			diag.TokenAcquired = false
			diag.TokenError = err.Error()
			diag.Suggestion = "Token acquisition failed. Check: (1) IDENTITY_ENDPOINT and IDENTITY_HEADER env vars are set, " +
				"(2) Managed identity is assigned to the Container App, " +
				"(3) If using user-assigned identity, ManagedIdentityID matches the client ID"
			return diag
		}
		diag.TokenAcquired = true

		// Decode JWT claims for diagnostics
		claims := decodeJWTClaims(token)
		if claims != nil {
			diag.TokenAudience, _ = claims["aud"].(string)
			diag.TokenIssuer, _ = claims["iss"].(string)
			diag.TokenTenantID, _ = claims["tid"].(string)
			diag.TokenObjectID, _ = claims["oid"].(string)
		}
	}

	// Make a lightweight diagnostic request: list tables with $top=1
	reqURL := c.endpoint + "/Tables?$top=1"
	if c.useSAS {
		reqURL += "&" + c.sasToken
	}
	diag.RequestURL = reqURL

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		diag.ErrorMessage = fmt.Sprintf("failed to create request: %v", err)
		return diag
	}

	c.signRequest(req, "GET", nil, "/Tables")
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		diag.ErrorMessage = fmt.Sprintf("request failed (network): %v", err)
		diag.Suggestion = "Network error. Check: (1) Storage account firewall allows Container App outbound IPs or VNet, " +
			"(2) Private endpoint DNS is configured correctly, " +
			"(3) Storage account endpoint is reachable"
		return diag
	}
	defer resp.Body.Close()

	diag.ResponseStatus = resp.StatusCode

	// Capture response headers
	diag.ResponseHeaders = make(map[string]string)
	for _, key := range []string{
		"x-ms-error-code", "x-ms-request-id", "WWW-Authenticate",
		"x-ms-version", "Server", "Date", "Content-Type",
	} {
		if v := resp.Header.Get(key); v != "" {
			diag.ResponseHeaders[key] = v
		}
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) > 2048 {
		diag.ResponseBody = string(body[:2048]) + "...(truncated)"
	} else {
		diag.ResponseBody = string(body)
	}

	if resp.StatusCode == http.StatusOK {
		diag.Suggestion = "Authentication is working correctly."
		return diag
	}

	// Parse error
	azErr := parseAzureError(resp, body)
	diag.ErrorCode = azErr.Code
	diag.ErrorMessage = azErr.Message

	// Provide targeted suggestions based on the error
	switch {
	case resp.StatusCode == http.StatusForbidden && (diag.ErrorCode == "AuthenticationFailed" || strings.Contains(diag.ErrorMessage, "AuthenticationFailed")):
		diag.Suggestion = "Azure rejected the authentication token. Check: " +
			"(1) Token audience should be 'https://storage.azure.com' (got: '" + diag.TokenAudience + "'), " +
			"(2) Token tenant ID should match the storage account's tenant, " +
			"(3) The storage account exists and the endpoint URL is correct"

	case resp.StatusCode == http.StatusForbidden && (diag.ErrorCode == "AuthorizationPermissionMismatch" || strings.Contains(diag.ErrorMessage, "AuthorizationPermissionMismatch")):
		diag.Suggestion = "Token is valid but identity lacks permissions. Assign 'Storage Table Data Contributor' role to the managed identity " +
			"(Object ID: " + diag.TokenObjectID + ") on the storage account. " +
			"Note: 'Contributor' and 'Owner' roles do NOT grant data-plane access."

	case resp.StatusCode == http.StatusForbidden && (diag.ErrorCode == "AuthorizationFailure" || strings.Contains(diag.ErrorMessage, "AuthorizationFailure")):
		diag.Suggestion = "Authorization failed. This usually means no RBAC role is assigned at all. " +
			"Assign 'Storage Table Data Contributor' to the managed identity on the storage account."

	case resp.StatusCode == http.StatusUnauthorized:
		diag.Suggestion = "Request was not authenticated. Check: (1) Bearer token is present in Authorization header, " +
			"(2) Token has not expired, (3) Storage account allows AAD authentication"

	default:
		diag.Suggestion = fmt.Sprintf("Unexpected status %d. Check Azure Storage account health and network connectivity.", resp.StatusCode)
	}

	return diag
}

// decodeJWTClaims extracts the claims from a JWT token without validation.
// Returns nil if the token cannot be decoded.
func decodeJWTClaims(token string) map[string]interface{} {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 2 {
		return nil
	}

	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil
	}
	return claims
}
