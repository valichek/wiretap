package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDuration_parse(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "0", want: 0},
		{in: "30s", want: 30 * time.Second},
		{in: "5m", want: 5 * time.Minute},
		{in: "2h", want: 2 * time.Hour},
		{in: "24h", want: 24 * time.Hour},
		{in: "7d", want: 7 * 24 * time.Hour},
		{in: "1d", want: 24 * time.Hour},
		{in: "2w", want: 14 * 24 * time.Hour},
		{in: "garbage", wantErr: true},
		// compound is not supported in v1 — `1d12h` would need a parser; just use `36h`
		{in: "1d12h", wantErr: true},
		{in: "1.5d", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			var d Duration
			err := d.parse(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, d.AsDuration())
		})
	}
}

func TestDuration_UnmarshalYAML(t *testing.T) {
	type wrapper struct {
		Field Duration `yaml:"field"`
	}
	tests := []struct {
		yamlStr string
		want    time.Duration
		wantErr bool
	}{
		{yamlStr: `field: 7d`, want: 7 * 24 * time.Hour},
		{yamlStr: `field: 24h`, want: 24 * time.Hour},
		{yamlStr: `field: "1w"`, want: 7 * 24 * time.Hour},
		{yamlStr: `field: 0`, want: 0},
		{yamlStr: `field: ""`, want: 0},
		{yamlStr: `field: bogus`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.yamlStr, func(t *testing.T) {
			var w wrapper
			err := yaml.Unmarshal([]byte(tc.yamlStr), &w)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, w.Field.AsDuration())
		})
	}
}

func TestDuration_UnmarshalText(t *testing.T) {
	var d Duration
	require.NoError(t, d.UnmarshalText([]byte("3d")))
	assert.Equal(t, 3*24*time.Hour, d.AsDuration())

	require.Error(t, d.UnmarshalText([]byte("nope")))
}
