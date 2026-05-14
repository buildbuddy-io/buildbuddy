// Package jsonrpc contains utilities for the subset of the JSON-RPC 2.0
// specification needed by MCP.
//
// https://www.jsonrpc.org/specification
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"net/http"
)

const (
	// Version is the JSON-RPC protocol version used by MCP.
	Version = "2.0"

	// ParseErrorCode reports malformed JSON input.
	ParseErrorCode = -32700
	// InvalidRequestCode reports an invalid JSON-RPC request object.
	InvalidRequestCode = -32600
	// MethodNotFoundCode reports an unknown method name.
	MethodNotFoundCode = -32601
	// InvalidParamsCode reports invalid method params.
	InvalidParamsCode = -32602
	// InternalErrorCode reports an unexpected server-side failure.
	InternalErrorCode = -32603
)

// Request is one incoming JSON-RPC request object.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is one JSON-RPC response object.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is the structured JSON-RPC error payload.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ParsedBody is the normalized result of parsing one JSON-RPC HTTP body.
type ParsedBody struct {
	Requests []json.RawMessage
	IsBatch  bool
}

// Success wraps a successful result in a JSON-RPC response object.
func Success(id json.RawMessage, result any) *Response {
	return &Response{
		JSONRPC: Version,
		ID:      id,
		Result:  result,
	}
}

// Failure wraps an error in a JSON-RPC response object.
func Failure(id json.RawMessage, code int, message string) *Response {
	return &Response{
		JSONRPC: Version,
		ID:      id,
		Error: &Error{
			Code:    code,
			Message: message,
		},
	}
}

// IsNotification reports whether the request ID is absent or explicitly null.
func IsNotification(id json.RawMessage) bool {
	trimmed := bytes.TrimSpace(id)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

// ParseBody parses a single JSON-RPC HTTP body into either one request or a
// batch of requests.
func ParseBody(body []byte) (*ParsedBody, *Response, int) {
	if len(body) == 0 {
		return nil, Failure(nil, ParseErrorCode, "empty request body"), http.StatusBadRequest
	}
	if body[0] != '[' {
		return &ParsedBody{
			Requests: []json.RawMessage{body},
		}, nil, http.StatusOK
	}
	var requests []json.RawMessage
	if err := json.Unmarshal(body, &requests); err != nil {
		return nil, Failure(nil, ParseErrorCode, "parse request"), http.StatusBadRequest
	}
	if len(requests) == 0 {
		return nil, Failure(nil, InvalidRequestCode, "batch requests must not be empty"), http.StatusOK
	}
	return &ParsedBody{
		Requests: requests,
		IsBatch:  true,
	}, nil, http.StatusOK
}

// ParseRequest parses and validates one JSON-RPC request object.
func ParseRequest(raw []byte) (*Request, *Response, int) {
	var request Request
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, Failure(nil, ParseErrorCode, "parse request"), http.StatusBadRequest
	}
	if request.JSONRPC != Version || request.Method == "" {
		return nil, Failure(request.ID, InvalidRequestCode, "invalid JSON-RPC request"), http.StatusOK
	}
	return &request, nil, http.StatusOK
}
