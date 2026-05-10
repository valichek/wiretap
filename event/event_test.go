package event

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvent_JSONRoundtrip(t *testing.T) {
	e := Event{
		Ts:          1_700_000_000_000_000_000,
		Channel:     "rpcx",
		Src:         "client-a",
		Dst:         "service-x",
		Method:      "service.x:do-thing",
		Status:      "ok",
		DurationMs:  12,
		TraceID:     "00-abc-def-01",
		RequestID:   "req-1",
		SagaID:      "saga-1",
		ReqHeaders:  `{"x":"1"}`,
		ReqPayload:  `{"amount":100}`,
		RespHeaders: `{"y":"2"}`,
		RespPayload: `{"ok":true}`,
	}
	b, err := json.Marshal(e)
	require.NoError(t, err)

	var got Event
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, e, got)
}

func TestEvent_OmitsEmptyOptionalFields(t *testing.T) {
	// only required fields populated
	e := Event{
		Ts:         1,
		Channel:    "rpcx",
		Dst:        "service-x",
		Method:     "service.x:do-thing",
		Status:     "ok",
		DurationMs: 0,
	}
	b, err := json.Marshal(e)
	require.NoError(t, err)
	s := string(b)

	// optional fields should be absent
	for _, k := range []string{"src", "trace_id", "request_id", "saga_id", "req_headers", "req_payload", "resp_headers", "resp_payload", "error"} {
		assert.NotContains(t, s, "\""+k+"\"", "field %q should be omitted when empty", k)
	}

	// required fields should be present
	for _, k := range []string{"ts", "channel", "dst", "method", "status", "duration_ms"} {
		assert.True(t, strings.Contains(s, "\""+k+"\""), "field %q should always be present", k)
	}
}
