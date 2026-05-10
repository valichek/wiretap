package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want cliFlags
	}{
		{
			name: "defaults",
			args: []string{},
			want: cliFlags{
				configPath:  "",
				dir:         "",
				showVersion: false,
				logLevel:    "info",
				prettyLogs:  true,
			},
		},
		{
			name: "all flags set",
			args: []string{"--config", "/tmp/wt.yaml", "--dir", "/tmp/wt", "--log-level", "debug", "--pretty=false"},
			want: cliFlags{
				configPath:  "/tmp/wt.yaml",
				dir:         "/tmp/wt",
				showVersion: false,
				logLevel:    "debug",
				prettyLogs:  false,
			},
		},
		{
			name: "version flag",
			args: []string{"--version"},
			want: cliFlags{
				configPath:  "",
				dir:         "",
				showVersion: true,
				logLevel:    "info",
				prettyLogs:  true,
			},
		},
		{
			name: "single dash also works",
			args: []string{"-dir", "/x"},
			want: cliFlags{
				configPath:  "",
				dir:         "/x",
				showVersion: false,
				logLevel:    "info",
				prettyLogs:  true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFlags(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseFlags_invalid(t *testing.T) {
	_, err := parseFlags([]string{"--unknown-flag"})
	require.Error(t, err)
}

func TestSetupLogger(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		pretty  bool
		wantErr bool
	}{
		{name: "info pretty", level: "info", pretty: true},
		{name: "debug json", level: "debug", pretty: false},
		{name: "trace pretty", level: "trace", pretty: true},
		{name: "invalid level", level: "bogus", pretty: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := setupLogger(tc.level, tc.pretty)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
