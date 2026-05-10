// Package event defines the Event struct that mirrors the SQLite messages
// table schema. JSONL emit is plain json.Marshal; storage layer maps empty
// strings to SQL NULL for nullable columns (src, trace_id, etc.).
package event

// Event is one captured rpcx or http message exchange (paired req+resp).
type Event struct {
	Ts          int64  `json:"ts"`           // unix nanos, request start
	Channel     string `json:"channel"`      // "rpcx" | "http"
	Src         string `json:"src,omitempty"`
	Dst         string `json:"dst"`
	Method      string `json:"method"`     // rpcx: "service.path:method" or http: "POST /path"
	Status      string `json:"status"`     // "ok"|"error"|"timeout"|"dial_error"|"shutdown"|"upstream_closed" or http status
	DurationMs  int64  `json:"duration_ms"`
	TraceID     string `json:"trace_id,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	SagaID      string `json:"saga_id,omitempty"`
	ReqHeaders  string `json:"req_headers,omitempty"`  // JSON-encoded header map
	ReqPayload  string `json:"req_payload,omitempty"`  // decoded JSON for rpcx, body for http
	RespHeaders string `json:"resp_headers,omitempty"`
	RespPayload string `json:"resp_payload,omitempty"`
	Error       string `json:"error,omitempty"`
}
