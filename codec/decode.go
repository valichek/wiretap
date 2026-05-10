// Package codec converts raw rpcx payloads (JSON / CBOR / Gob / unknown) into
// JSON strings suitable for storage. SerializeType values match the standard
// rpcx protocol constants (1=JSON, 5=Gob, 6=CBOR).
package codec

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

// SerializeType matches values from github.com/smallnest/rpcx/protocol.SerializeType.
type SerializeType uint8

const (
	SerializeJSON SerializeType = 1
	SerializeGob  SerializeType = 5
	SerializeCBOR SerializeType = 6
)

// cborDecMode decodes CBOR maps into map[string]interface{} so the result can be
// directly json.Marshal'd. The default decoder produces map[interface{}]interface{},
// which encoding/json refuses to marshal.
var cborDecMode cbor.DecMode

func init() {
	dm, err := cbor.DecOptions{
		DefaultMapType: reflect.TypeOf(map[string]interface{}{}),
	}.DecMode()
	if err != nil {
		// only happens on impossible config; treat as developer error.
		panic(fmt.Errorf("cbor decmode init: %w", err))
	}
	cborDecMode = dm
}

// DecodePayload converts data to a JSON string suitable for storage.
//
// Output shape per case:
//   - data > truncateBytes (and truncateBytes > 0): {"__truncated_at_bytes":N,"__original_size":M}
//   - JSON valid: data verbatim (validated as parseable JSON)
//   - JSON invalid: {"_serialize_type":"json_invalid","_raw":"<input as string>"}
//   - CBOR: data decoded to native, re-marshaled as JSON
//   - Gob: {"_serialize_type":"gob","_base64":"..."}
//   - Unknown serialize type: {"_serialize_type":"unknown_<n>","_base64":"..."}
//
// truncateBytes <= 0 disables truncation.
func DecodePayload(t SerializeType, data []byte, truncateBytes int) (string, error) {
	if truncateBytes > 0 && len(data) > truncateBytes {
		return marshalCanFail(struct {
			TruncatedAtBytes int `json:"__truncated_at_bytes"`
			OriginalSize     int `json:"__original_size"`
		}{TruncatedAtBytes: truncateBytes, OriginalSize: len(data)})
	}

	switch t {
	case SerializeJSON:
		var v any
		if err := json.Unmarshal(data, &v); err != nil {
			return marshalCanFail(map[string]string{
				"_serialize_type": "json_invalid",
				"_raw":            string(data),
			})
		}
		return string(data), nil

	case SerializeCBOR:
		var v any
		if err := cborDecMode.Unmarshal(data, &v); err != nil {
			return "", fmt.Errorf("cbor unmarshal: %w", err)
		}

		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("cbor → json: %w", err)
		}
		return string(b), nil

	case SerializeGob:
		return wrapBase64("gob", data), nil

	default:
		return wrapBase64(fmt.Sprintf("unknown_%d", t), data), nil
	}
}

func wrapBase64(label string, data []byte) string {
	encoded := base64.StdEncoding.EncodeToString(data)
	s, _ := marshalCanFail(map[string]string{
		"_serialize_type": label,
		"_base64":         encoded,
	})
	return s
}

// marshalCanFail json.Marshals a value and returns its string. The error path
// is unreachable for the types we pass in (maps and tagged structs), but we
// surface it explicitly rather than panicking.
func marshalCanFail(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("json marshal: %w", err)
	}
	return string(b), nil
}
