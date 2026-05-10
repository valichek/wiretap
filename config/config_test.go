package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveDir(t *testing.T) {
	const home = "/h"
	tests := []struct {
		name string
		flag string
		env  map[string]string
		want string
	}{
		{name: "default", env: map[string]string{}, want: "/h/.wiretap"},
		{name: "env override", env: map[string]string{"WIRETAP_DIR": "/etc/wt"}, want: "/etc/wt"},
		{name: "flag wins over env", flag: "/cli", env: map[string]string{"WIRETAP_DIR": "/etc/wt"}, want: "/cli"},
		{name: "tilde in env", env: map[string]string{"WIRETAP_DIR": "~/wt"}, want: "/h/wt"},
		{name: "tilde in flag", flag: "~/wt2", want: "/h/wt2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			envFn := func(k string) string { return tc.env[k] }
			assert.Equal(t, tc.want, resolveDir(tc.flag, envFn, home))
		})
	}
}

func TestResolveConfigPath(t *testing.T) {
	tests := []struct {
		name     string
		flag     string
		dir      string
		existsAt string
		want     string
	}{
		{name: "flag set", flag: "/cli/wt.yaml", dir: "/d", want: "/cli/wt.yaml"},
		{name: "dir yaml exists", dir: "/d", existsAt: "/d/wiretap.yaml", want: "/d/wiretap.yaml"},
		{name: "fallback to cwd", dir: "/d", want: "wiretap.yaml"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			existsFn := func(p string) bool { return tc.existsAt != "" && p == tc.existsAt }
			assert.Equal(t, tc.want, resolveConfigPath(tc.flag, tc.dir, existsFn))
		})
	}
}

func TestExpandTilde(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"~", "/h"},
		{"~/foo", "/h/foo"},
		{"~/foo/bar", "/h/foo/bar"},
		{"/abs", "/abs"},
		{"rel", "rel"},
		{"", ""},
	}
	for _, tc := range tests {
		name := tc.in
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, expandTilde(tc.in, "/h"))
		})
	}
}

func TestParseListenPort(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{in: ":18987", want: 18987},
		{in: "127.0.0.1:18987", want: 18987},
		{in: "0.0.0.0:0", want: 0},
		{in: "no-port", wantErr: true},
		{in: ":notnum", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range tests {
		name := tc.in
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			got, err := parseListenPort(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func validProxy() ProxyConfig {
	return ProxyConfig{Kind: "rpcx", Listen: ":18987", Upstream: "127.0.0.1:8987", Dst: "service-y"}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "valid",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{validProxy()},
			},
		},
		{
			name: "no proxies",
			cfg: Config{
				Store: StoreConfig{JSONL: true},
			},
			wantErr: "no proxies",
		},
		{
			name: "no store sink",
			cfg: Config{
				Proxies: []ProxyConfig{validProxy()},
			},
			wantErr: "store.jsonl or store.sqlite",
		},
		{
			name: "missing kind",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Listen: ":1", Upstream: "u", Dst: "d"}},
			},
			wantErr: "kind is required",
		},
		{
			name: "invalid kind",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Kind: "tcp", Listen: ":1", Upstream: "u", Dst: "d"}},
			},
			wantErr: "kind must be",
		},
		{
			name: "missing listen",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Kind: "rpcx", Upstream: "u", Dst: "d"}},
			},
			wantErr: "listen is required",
		},
		{
			name: "missing upstream",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Kind: "rpcx", Listen: ":1", Dst: "d"}},
			},
			wantErr: "upstream is required",
		},
		{
			name: "missing dst",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Kind: "rpcx", Listen: ":1", Upstream: "u"}},
			},
			wantErr: "dst is required",
		},
		{
			name: "invalid listen format",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Kind: "rpcx", Listen: "no-port", Upstream: "u", Dst: "d"}},
			},
			wantErr: "invalid listen",
		},
		{
			name: "port out of range",
			cfg: Config{
				Store:   StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{{Kind: "rpcx", Listen: ":99999", Upstream: "u", Dst: "d"}},
			},
			wantErr: "out of range",
		},
		{
			name: "duplicate listen",
			cfg: Config{
				Store: StoreConfig{JSONL: true},
				Proxies: []ProxyConfig{
					{Kind: "rpcx", Listen: ":18987", Upstream: "u1", Dst: "d1"},
					{Kind: "rpcx", Listen: ":18987", Upstream: "u2", Dst: "d2"},
				},
			},
			wantErr: "duplicate listen",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestConfig_applyDefaults(t *testing.T) {
	c := Config{}
	c.applyDefaults()
	assert.Equal(t, defaultTruncatePayloadBytes, c.Store.TruncatePayloadBytes)

	c2 := Config{Store: StoreConfig{TruncatePayloadBytes: 4096}}
	c2.applyDefaults()
	assert.Equal(t, 4096, c2.Store.TruncatePayloadBytes, "explicit value preserved")
}

func TestMkdirIfMissing(t *testing.T) {
	t.Run("creates with 0700 if missing", func(t *testing.T) {
		parent := t.TempDir()
		target := filepath.Join(parent, "newdir")

		require.NoError(t, mkdirIfMissing(target))
		info, err := os.Stat(target)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	t.Run("noop if exists; does not chmod", func(t *testing.T) {
		dir := t.TempDir() // created with default tempdir perms (0700 on unix typically)
		require.NoError(t, os.Chmod(dir, 0o755))

		require.NoError(t, mkdirIfMissing(dir))
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o755), info.Mode().Perm(), "existing dir perms preserved")
	})
}

func TestLoad_integration(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "wiretap.yaml")
	yaml := `
store:
  jsonl: true
  sqlite: false
  truncate_payload_bytes: 2048
proxies:
  - kind: rpcx
    listen: ":18987"
    upstream: "127.0.0.1:8987"
    src: "client-a"
    dst: "service-y"
  - kind: rpcx
    listen: ":18989"
    upstream: "127.0.0.1:8989"
    src: "client-a"
    dst: "service-x"
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(yaml), 0o644))

	got, err := Load(tmpDir, cfgPath)
	require.NoError(t, err)
	assert.Equal(t, tmpDir, got.Dir)
	assert.True(t, got.Store.JSONL)
	assert.False(t, got.Store.SQLite)
	assert.Equal(t, 2048, got.Store.TruncatePayloadBytes)
	require.Len(t, got.Proxies, 2)
	assert.Equal(t, "rpcx", got.Proxies[0].Kind)
	assert.Equal(t, ":18987", got.Proxies[0].Listen)
	assert.Equal(t, "service-y", got.Proxies[0].Dst)
	assert.Equal(t, "client-a", got.Proxies[0].Src)
}

func TestLoad_appliesTruncateDefault(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "wiretap.yaml")
	yaml := `
store:
  jsonl: true
proxies:
  - {kind: rpcx, listen: ":18987", upstream: "127.0.0.1:8987", dst: "x"}
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(yaml), 0o644))

	got, err := Load(tmpDir, cfgPath)
	require.NoError(t, err)
	assert.Equal(t, defaultTruncatePayloadBytes, got.Store.TruncatePayloadBytes)
}

func TestLoad_invalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "wiretap.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("not: valid: yaml: ::"), 0o644))

	_, err := Load(tmpDir, cfgPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}

func TestLoad_validationFails(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "wiretap.yaml")
	yaml := `
store: {jsonl: false, sqlite: false}
proxies:
  - {kind: rpcx, listen: ":18987", upstream: "127.0.0.1:8987", dst: "x"}
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(yaml), 0o644))

	_, err := Load(tmpDir, cfgPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.jsonl or store.sqlite")
}

func TestLoad_creatingMissingDir(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "newdir")
	cfgElsewhere := filepath.Join(parent, "wiretap.yaml")
	yaml := `
store: {jsonl: true}
proxies:
  - {kind: rpcx, listen: ":18987", upstream: "127.0.0.1:8987", dst: "x"}
`
	require.NoError(t, os.WriteFile(cfgElsewhere, []byte(yaml), 0o644))

	got, err := Load(target, cfgElsewhere)
	require.NoError(t, err)
	assert.Equal(t, target, got.Dir)

	info, err := os.Stat(target)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestLoad_missingConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := Load(tmpDir, filepath.Join(tmpDir, "does-not-exist.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}
