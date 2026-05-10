package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"wiretap/config"
	"wiretap/proxy"
	"wiretap/store"
)

// version is injected at build time via -ldflags; default for go run.
var version = "dev"

type cliFlags struct {
	configPath  string
	dir         string
	showVersion bool
	logLevel    string
	prettyLogs  bool
}

func parseFlags(args []string) (cliFlags, error) {
	var f cliFlags
	fs := flag.NewFlagSet("wiretap-rpcx", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&f.configPath, "config", "", "path to wiretap.yaml (default: $WIRETAP_DIR/wiretap.yaml or ./wiretap.yaml)")
	fs.StringVar(&f.dir, "dir", "", "wiretap data directory (default: $WIRETAP_DIR or ~/.wiretap)")
	fs.BoolVar(&f.showVersion, "version", false, "print version and exit")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level (trace, debug, info, warn, error)")
	fs.BoolVar(&f.prettyLogs, "pretty", true, "pretty console logs (set --pretty=false for json)")
	if err := fs.Parse(args); err != nil {
		return cliFlags{}, err
	}
	return f, nil
}

func setupLogger(level string, pretty bool) (zerolog.Logger, error) {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		return zerolog.Logger{}, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	zerolog.SetGlobalLevel(lvl)

	if pretty {
		return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
			With().Timestamp().Logger(), nil
	}
	return zerolog.New(os.Stderr).With().Timestamp().Logger(), nil
}

func main() {
	flags, err := parseFlags(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}

	if flags.showVersion {
		fmt.Println(version)
		return
	}

	logger, err := setupLogger(flags.logLevel, flags.prettyLogs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup logger: %v\n", err)
		os.Exit(1)
	}
	log.Logger = logger

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, flags, logger); err != nil {
		logger.Error().Err(err).Msg("wiretap-rpcx exited with error")
		os.Exit(1)
	}
}

// run is the testable entry point. main wraps it with flag parsing, logger
// setup, and signal handling. Tests construct a context and flags directly
// and cancel ctx to trigger shutdown.
func run(ctx context.Context, flags cliFlags, logger zerolog.Logger) error {
	cfg, err := config.Load(flags.dir, flags.configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	startUnix := time.Now().Unix()

	jsonlW := store.NewJSONLWriter(cfg.Dir, startUnix)

	var sqliteW *store.SQLiteWriter
	if cfg.Store.SQLite {
		sqliteW, err = store.NewSQLiteWriter(filepath.Join(cfg.Dir, "wiretap.db"))
		if err != nil {
			return fmt.Errorf("open sqlite: %w", err)
		}
	}

	pipeline := store.NewPipeline(jsonlW, sqliteW, 1024, cfg.Store.DropStatuses, logger)

	// optional retention — prunes old jsonl files + sqlite rows hourly
	if retainer := store.NewRetainer(cfg.Dir, sqliteW, cfg.Store.Retention.AsDuration(), logger); retainer != nil {
		go retainer.Run(ctx)
		logger.Info().Stringer("retention", cfg.Store.Retention.AsDuration()).Msg("retention enabled")
	}

	var (
		wg            sync.WaitGroup
		rpcxCount     int
		skippedCount  int
	)
	for _, pcfg := range cfg.Proxies {
		if pcfg.Kind != "rpcx" {
			skippedCount++
			continue
		}
		rpcxCount++

		p := proxy.New(pcfg, pipeline, logger, cfg.Store.TruncatePayloadBytes)
		wg.Add(1)
		go func(pc config.ProxyConfig) {
			defer wg.Done()
			if rerr := p.Run(ctx); rerr != nil && !errors.Is(rerr, context.Canceled) {
				logger.Error().
					Err(rerr).
					Str("dst", pc.Dst).
					Str("listen", pc.Listen).
					Msg("proxy stopped with error")
			}
		}(pcfg)
	}

	logger.Info().
		Str("version", version).
		Str("dir", cfg.Dir).
		Int("rpcx_proxies", rpcxCount).
		Int("skipped_non_rpcx", skippedCount).
		Bool("sqlite", cfg.Store.SQLite).
		Bool("jsonl", cfg.Store.JSONL).
		Int64("start_unix", startUnix).
		Msg("wiretap-rpcx started")

	<-ctx.Done()
	logger.Info().Msg("shutdown signal received; stopping proxies")

	wg.Wait()
	logger.Info().Msg("all proxies drained; closing storage")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := pipeline.Close(shutdownCtx); err != nil {
		logger.Warn().Err(err).Msg("pipeline close error")
	}

	logger.Info().Msg("wiretap-rpcx stopped")
	return nil
}
