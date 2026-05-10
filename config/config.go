package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultTruncatePayloadBytes = 1 << 20 // 1 MB

// Config is the runtime config loaded from wiretap.yaml.
//
// Dir is set from the resolved data directory (--dir > $WIRETAP_DIR > ~/.wiretap),
// not from the yaml file. Any `dir:` field in the yaml is informational only.
type Config struct {
	Dir     string        `yaml:"dir"`
	Store   StoreConfig   `yaml:"store"`
	Proxies []ProxyConfig `yaml:"proxies"`
}

// StoreConfig — sinks for captured events.
type StoreConfig struct {
	JSONL                bool `yaml:"jsonl"`
	SQLite               bool `yaml:"sqlite"`
	TruncatePayloadBytes int  `yaml:"truncate_payload_bytes"`
	// Retention — files/rows older than this are pruned hourly.
	// Accepts time.ParseDuration formats plus `Nd` (days) / `Nw` (weeks)
	// shorthands. Zero / unset disables retention; captures grow forever.
	Retention Duration `yaml:"retention"`
	// DropStatuses — event statuses to skip at write time. Sink.Emit still
	// returns normally (the proxy contract is unchanged), but the writers
	// don't persist these events. Use to silence dial_error /
	// upstream_closed noise when an upstream is down for an extended period.
	DropStatuses []string `yaml:"drop_statuses"`
}

// ProxyConfig — one shadow listener.
type ProxyConfig struct {
	Kind     string `yaml:"kind"`     // "rpcx" or "http"
	Listen   string `yaml:"listen"`   // ":18987" or "127.0.0.1:18987"
	Upstream string `yaml:"upstream"` // "127.0.0.1:8987"
	Src      string `yaml:"src"`      // optional; consumer label (e.g. "client-a")
	Dst      string `yaml:"dst"`      // required; destination service label
}

// Load resolves paths, reads the yaml file, applies defaults, validates, and
// ensures the data directory exists. flagDir and flagConfigPath come from cli;
// either may be empty to fall through to env/defaults.
func Load(flagDir, flagConfigPath string) (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, fmt.Errorf("user home dir: %w", err)
	}

	dir := resolveDir(flagDir, os.Getenv, home)
	cfgPath := resolveConfigPath(flagConfigPath, dir, fileExists)

	if err := mkdirIfMissing(dir); err != nil {
		return Config{}, fmt.Errorf("ensure data dir %s: %w", dir, err)
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", cfgPath, err)
	}

	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", cfgPath, err)
	}
	c.Dir = dir
	c.applyDefaults()

	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate %s: %w", cfgPath, err)
	}
	return c, nil
}

// Validate checks the config for required fields, unique listen ports, and
// that at least one storage sink is enabled.
func (c Config) Validate() error {
	if !c.Store.JSONL && !c.Store.SQLite {
		return errors.New("at least one of store.jsonl or store.sqlite must be enabled")
	}
	if len(c.Proxies) == 0 {
		return errors.New("no proxies configured")
	}

	seen := make(map[string]int, len(c.Proxies))
	for i, p := range c.Proxies {
		if p.Kind == "" {
			return fmt.Errorf("proxies[%d]: kind is required", i)
		}
		if p.Kind != "rpcx" && p.Kind != "http" {
			return fmt.Errorf("proxies[%d]: kind must be 'rpcx' or 'http', got %q", i, p.Kind)
		}
		if p.Listen == "" {
			return fmt.Errorf("proxies[%d]: listen is required", i)
		}
		if p.Upstream == "" {
			return fmt.Errorf("proxies[%d]: upstream is required", i)
		}
		if p.Dst == "" {
			return fmt.Errorf("proxies[%d]: dst is required", i)
		}

		port, err := parseListenPort(p.Listen)
		if err != nil {
			return fmt.Errorf("proxies[%d] (%s): %w", i, p.Listen, err)
		}
		if port < 1 || port > 65535 {
			return fmt.Errorf("proxies[%d] (%s): port %d out of range [1, 65535]", i, p.Listen, port)
		}
		if dup, ok := seen[p.Listen]; ok {
			return fmt.Errorf("proxies[%d] (%s): duplicate listen address (also at proxies[%d])", i, p.Listen, dup)
		}
		seen[p.Listen] = i
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Store.TruncatePayloadBytes <= 0 {
		c.Store.TruncatePayloadBytes = defaultTruncatePayloadBytes
	}
}

// resolveDir picks the data dir using precedence: flag > $WIRETAP_DIR > ~/.wiretap.
// envFn is os.Getenv in production; injected for tests.
func resolveDir(flagDir string, envFn func(string) string, home string) string {
	if flagDir != "" {
		return expandTilde(flagDir, home)
	}
	if d := envFn("WIRETAP_DIR"); d != "" {
		return expandTilde(d, home)
	}
	return filepath.Join(home, ".wiretap")
}

// resolveConfigPath picks the config path using precedence:
// --config > <dir>/wiretap.yaml (if exists) > ./wiretap.yaml.
// existsFn is os.Stat-based in production; injected for tests.
func resolveConfigPath(flagConfig, dir string, existsFn func(string) bool) string {
	if flagConfig != "" {
		return flagConfig
	}
	if c := filepath.Join(dir, "wiretap.yaml"); existsFn(c) {
		return c
	}
	return "wiretap.yaml"
}

// expandTilde expands a leading "~" or "~/..." against home. Other paths returned as-is.
// Does not handle "~user/..." (overkill for v1).
func expandTilde(p, home string) string {
	if p == "~" {
		return home
	}
	if strings.HasPrefix(p, "~/") {
		return filepath.Join(home, p[2:])
	}
	return p
}

// mkdirIfMissing creates dir with 0700 perms if it does not exist; no-op if it does.
// We do not chmod existing dirs — respect what the user has set.
func mkdirIfMissing(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// MkdirAll's perm arg is subject to umask; force exact perms on the leaf.
	return os.Chmod(dir, 0o700)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func parseListenPort(listen string) (int, error) {
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return 0, fmt.Errorf("invalid listen address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	return port, nil
}
