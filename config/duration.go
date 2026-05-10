package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration extends time.ParseDuration with `d` (days) and `w` (weeks)
// suffixes. Single-unit only — `1d12h` is NOT supported (use `36h`).
//
// Examples in YAML:
//
//	retention: 7d   // 7 days
//	retention: 24h  // 24 hours
//	retention: 30m  // 30 minutes
//	retention: 0    // disabled
//	retention: 2w   // 2 weeks
type Duration time.Duration

// AsDuration returns the underlying time.Duration. Useful at use-sites that
// want to pass a stdlib value to APIs like time.NewTicker, time.Sleep, etc.
func (d Duration) AsDuration() time.Duration {
	return time.Duration(d)
}

// UnmarshalYAML supports stdlib time.ParseDuration plus `Nd` / `Nw` integer
// shorthands. Empty/zero parses to 0 (disabled-by-convention for retention).
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	return d.parse(strings.TrimSpace(value.Value))
}

// UnmarshalText is the same parser surface, used for env-var binding and
// other text-based config sources.
func (d *Duration) UnmarshalText(b []byte) error {
	return d.parse(strings.TrimSpace(string(b)))
}

func (d *Duration) parse(s string) error {
	if s == "" {
		*d = 0
		return nil
	}

	// allow bare "0" as disabled
	if s == "0" {
		*d = 0
		return nil
	}

	// d / w shorthand — single unit only
	if n, ok := splitNumberSuffix(s, "d"); ok {
		*d = Duration(time.Duration(n) * 24 * time.Hour)
		return nil
	}
	if n, ok := splitNumberSuffix(s, "w"); ok {
		*d = Duration(time.Duration(n) * 7 * 24 * time.Hour)
		return nil
	}

	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// splitNumberSuffix returns (n, true) when s is exactly `<int><suffix>`.
func splitNumberSuffix(s, suffix string) (int, bool) {
	if !strings.HasSuffix(s, suffix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(s, suffix))
	if err != nil {
		return 0, false
	}
	return n, true
}
