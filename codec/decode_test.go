package codec

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodePayload_JSON_valid(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{name: "object", in: `{"a":1}`},
		{name: "array", in: `[1,2,3]`},
		{name: "string", in: `"hi"`},
		{name: "null", in: `null`},
		{name: "number", in: `42`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodePayload(SerializeJSON, []byte(tc.in), 1024)
			require.NoError(t, err)
			assert.JSONEq(t, tc.in, got)
		})
	}
}

func TestDecodePayload_JSON_invalidWraps(t *testing.T) {
	data := []byte("not valid json")

	got, err := DecodePayload(SerializeJSON, data, 1024)
	require.NoError(t, err)

	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(got), &m))
	assert.Equal(t, "json_invalid", m["_serialize_type"])
	assert.Equal(t, "not valid json", m["_raw"])
}

func TestDecodePayload_CBOR_roundtrip(t *testing.T) {
	in := map[string]interface{}{
		"name":   "alice",
		"count":  42,
		"active": true,
		"nested": map[string]interface{}{
			"k": "v",
		},
	}
	data, err := cbor.Marshal(in)
	require.NoError(t, err)

	got, err := DecodePayload(SerializeCBOR, data, 1024)
	require.NoError(t, err)

	var out map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(got), &out))
	assert.Equal(t, "alice", out["name"])
	// numbers come back as float64 through json
	assert.Equal(t, float64(42), out["count"])
	assert.Equal(t, true, out["active"])
	assert.Equal(t, map[string]interface{}{"k": "v"}, out["nested"])
}

func TestDecodePayload_CBOR_invalid(t *testing.T) {
	data := []byte{0xff, 0xff, 0xff, 0xff}

	_, err := DecodePayload(SerializeCBOR, data, 1024)
	require.Error(t, err)
}

func TestDecodePayload_Gob_wraps(t *testing.T) {
	data := []byte("gob-encoded-bytes-here")

	got, err := DecodePayload(SerializeGob, data, 1024)
	require.NoError(t, err)

	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(got), &m))
	assert.Equal(t, "gob", m["_serialize_type"])

	decoded, err := base64.StdEncoding.DecodeString(m["_base64"])
	require.NoError(t, err)
	assert.Equal(t, data, decoded)
}

func TestDecodePayload_Unknown_wraps(t *testing.T) {
	data := []byte("unknown-codec-bytes")

	got, err := DecodePayload(SerializeType(99), data, 1024)
	require.NoError(t, err)

	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(got), &m))
	assert.Equal(t, "unknown_99", m["_serialize_type"])

	decoded, err := base64.StdEncoding.DecodeString(m["_base64"])
	require.NoError(t, err)
	assert.Equal(t, data, decoded)
}

func TestDecodePayload_truncated(t *testing.T) {
	big := make([]byte, 2048)
	for i := range big {
		big[i] = 'x'
	}

	got, err := DecodePayload(SerializeJSON, big, 1024)
	require.NoError(t, err)

	var m map[string]int
	require.NoError(t, json.Unmarshal([]byte(got), &m))
	assert.Equal(t, 1024, m["__truncated_at_bytes"])
	assert.Equal(t, 2048, m["__original_size"])
}

func TestDecodePayload_truncateZeroDisables(t *testing.T) {
	// big payload with truncateBytes=0 should not be truncated;
	// JSON-invalid path triggers the wrap instead.
	big := make([]byte, 2048)
	for i := range big {
		big[i] = 'x'
	}

	got, err := DecodePayload(SerializeJSON, big, 0)
	require.NoError(t, err)

	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(got), &m))
	assert.Equal(t, "json_invalid", m["_serialize_type"], "should wrap as invalid JSON, not truncate")
}

func TestDecodePayload_CBOR_truncatedAhead(t *testing.T) {
	// truncation check runs before codec dispatch, so an oversized CBOR
	// payload gets a truncation marker rather than a decode attempt.
	big := make([]byte, 2048)

	got, err := DecodePayload(SerializeCBOR, big, 1024)
	require.NoError(t, err)

	var m map[string]int
	require.NoError(t, json.Unmarshal([]byte(got), &m))
	assert.Equal(t, 1024, m["__truncated_at_bytes"])
}
