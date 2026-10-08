package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/backfill"
	"reorgguard/internal/live"
	"reorgguard/internal/multirpc"
	"reorgguard/internal/operational"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		// Give canceled RPC/DB work and the repository close a bounded chance to
		// finish. SIGKILL remains an abrupt termination tested separately.
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case err = <-done:
		case <-timer.C:
			slog.Error("shutdown deadline exceeded")
			os.Exit(2)
		}
	}
	if ctx.Err() != nil && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		slog.Info("graceful shutdown complete")
		return
	}
	if err != nil {
		// Never render provider/driver-controlled error text: RPC URLs and DSNs can
		// contain credentials. Detailed safe error codes arrive with observability.
		slog.Error("backfill stopped", "error_type", fmt.Sprintf("%T", err))
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	shutdownTrace, traceErr := telemetry.Init(os.Getenv("REORGGUARD_TRACING"))
	if traceErr != nil {
		return traceErr
	}
	defer func() {
		done, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = shutdownTrace(done)
	}()
	rpcURL := os.Getenv("REORGGUARD_RPC_HTTP_URL")
	dsn := os.Getenv("REORGGUARD_DATABASE_URL")
	chainID, err := strconv.ParseUint(os.Getenv("REORGGUARD_CHAIN_ID"), 10, 64)
	if err != nil || chainID == 0 {
		return errors.New("invalid chain ID")
	}
	start, err := strconv.ParseUint(os.Getenv("REORGGUARD_START_BLOCK"), 10, 64)
	if err != nil {
		return errors.New("invalid start block")
	}
	maxDepth, err := strconv.ParseUint(os.Getenv("REORGGUARD_MAX_REORG_DEPTH"), 10, 64)
	if err != nil || maxDepth == 0 || maxDepth > 10000 {
		return errors.New("invalid maximum reorg depth")
	}
	addressRaw := os.Getenv("REORGGUARD_ADDRESS")
	if !common.IsHexAddress(addressRaw) {
		return errors.New("invalid filter address")
	}
	genesis, err := parseHash(os.Getenv("REORGGUARD_GENESIS_HASH"))
	if err != nil {
		return errors.New("invalid genesis hash")
	}
	var anchor common.Hash
	if start > 1 {
		anchor, err = parseHash(os.Getenv("REORGGUARD_ANCHOR_HASH"))
		if err != nil {
			return errors.New("invalid anchor hash")
		}
	}
	filter := rpc.Filter{Addresses: []common.Address{common.HexToAddress(addressRaw)}}
	client, err := rpc.NewHTTPClient(rpcURL, 10*time.Second, 16<<20)
	if err != nil {
		return err
	}
	repo, err := store.Open(ctx, dsn, 10*time.Second)
	if err != nil {
		return err
	}
	defer repo.Close()
	workers, inFlight := 0, 0
	if raw := os.Getenv("REORGGUARD_BACKFILL_WORKERS"); raw != "" {
		workers, err = strconv.Atoi(raw)
		if err != nil || workers < 1 || workers > 32 {
			return errors.New("invalid backfill worker count")
		}
	}
	if workers > 1 {
		inFlight = workers * 2
		if raw := os.Getenv("REORGGUARD_MAX_INFLIGHT_RANGES"); raw != "" {
			inFlight, err = strconv.Atoi(raw)
			if err != nil || inFlight < workers || inFlight > 64 {
				return errors.New("invalid in-flight range limit")
			}
		}
	} else if os.Getenv("REORGGUARD_MAX_INFLIGHT_RANGES") != "" {
		return errors.New("in-flight range limit requires parallel workers")
	}
	runner := backfill.Runner{Source: client, Store: repo, ChainID: chainID, Genesis: genesis, AnchorHash: anchor, StartBlock: start, Filter: filter, Metrics: &backfill.Metrics{}, Config: backfill.Config{
		InitialRange: 64, MinRange: 1, MaxRange: 1024, MaxAttempts: 6, RangeTimeout: 45 * time.Second,
		RetryDelay: 250 * time.Millisecond, HealthyMaxLogs: 1000, HealthyLatency: 3 * time.Second,
		GrowthAfter: 3, MaxLogsPerRange: 10000, Workers: workers, MaxInFlight: inFlight,
	}}
	engine := &reorg.Engine{Source: client, Store: repo, ChainID: chainID, Filter: filter,
		MaxDepth: maxDepth, Timeout: 90 * time.Second, MaxLogsPerBlock: 10000,
		Logger: slog.Default(), Metrics: reorg.NewMetrics()}
	coordinator := reorg.Coordinator{Backfill: &runner, Engine: engine}
	var pool *multirpc.Pool
	if raw := os.Getenv("REORGGUARD_RPC_FALLBACK_URLS"); raw != "" {
		parts := strings.Split(raw, ",")
		if len(parts) > 3 {
			return errors.New("too many fallback RPC endpoints")
		}
		endpoints := []multirpc.Endpoint{{ID: "primary", Source: client}}
		for i, part := range parts {
			fallback, clientErr := rpc.NewHTTPClient(strings.TrimSpace(part), 10*time.Second, 16<<20)
			if clientErr != nil {
				return clientErr
			}
			endpoints = append(endpoints, multirpc.Endpoint{ID: fmt.Sprintf("fallback_%d", i+1), Source: fallback})
		}
		pool, err = multirpc.New(repo, endpoints, multirpc.Config{ChainID: chainID, Genesis: genesis, Filter: filter, ProbeTimeout: 10 * time.Second, Cooldown: 2 * time.Second, MaxSwitches: len(endpoints), Runner: &runner, Engine: engine, Logger: slog.Default()})
		if err != nil {
			return err
		}
	}
	mode := os.Getenv("REORGGUARD_SYNC_MODE")
	if mode == "live" {
		poll := 3 * time.Second
		lagTolerance := uint64(2)
		if raw := os.Getenv("REORGGUARD_LAG_TOLERANCE"); raw != "" {
			lagTolerance, err = strconv.ParseUint(raw, 10, 64)
			if err != nil || lagTolerance > 100000 {
				return errors.New("invalid readiness lag tolerance")
			}
		}
		if raw := os.Getenv("REORGGUARD_POLL_INTERVAL"); raw != "" {
			poll, err = time.ParseDuration(raw)
			if err != nil || poll < 10*time.Millisecond || poll > time.Hour {
				return errors.New("invalid polling interval")
			}
		}
		var ws *rpc.WSClient
		if wsURL := os.Getenv("REORGGUARD_RPC_WS_URL"); wsURL != "" {
			ws, err = rpc.NewWSClient(wsURL, chainID, genesis, filter)
			if err != nil {
				return err
			}
		}
		var syncer live.Sweeper
		if pool != nil {
			syncer = pool
		}
		service := &live.Service{Canonical: &coordinator, Head: client, Syncer: syncer, WS: ws, Config: live.Config{
			PollInterval: poll, SweepTimeout: 2 * time.Minute, BackoffMin: 250 * time.Millisecond,
			BackoffMax: 30 * time.Second, LagTolerance: lagTolerance,
		}}
		slog.Info("live synchronization starting", "chain_id", chainID)
		apiAddr := os.Getenv("REORGGUARD_API_ADDR")
		if apiAddr == "" {
			return service.Run(ctx)
		}
		host, _, splitErr := net.SplitHostPort(apiAddr)
		if splitErr != nil || (host != "localhost" && (net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback())) {
			return errors.New("operational API must bind a loopback address")
		}
		listener, listenErr := net.Listen("tcp", apiAddr)
		if listenErr != nil {
			return errors.New("operational API listen failed")
		}
		api := &operational.Server{ChainID: chainID, Store: repo, Live: service, Pool: pool, RPC: client, Reorg: engine.Metrics, Backfill: runner.Metrics, WSConfigured: ws != nil}
		httpServer := operational.NewHTTPServer(api.Handler())
		serverDone := make(chan error, 1)
		go func() { serverDone <- httpServer.Serve(listener) }()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdownCtx)
			<-serverDone
		}()
		slog.Info("operational API listening", "address", apiAddr)
		syncErr := service.Run(ctx)
		if syncErr != nil && ctx.Err() == nil {
			// Preserve an inspectable unhealthy process after a terminal live
			// failure. A durable unsafe marker still prevents progression.
			slog.Error("live synchronization halted", "error_type", fmt.Sprintf("%T", syncErr))
			<-ctx.Done()
			return ctx.Err()
		}
		return syncErr
	}
	if mode != "" && mode != "once" {
		return errors.New("invalid sync mode")
	}
	if pool != nil {
		result, target, runErr := pool.Sweep(ctx)
		if runErr != nil {
			return runErr
		}
		slog.Info("historical backfill complete", "chain_id", chainID, "checkpoint", result.Checkpoint.Number, "remote_head", target, "ranges", result.Ranges, "logs", result.Logs)
		return nil
	}
	result, err := coordinator.Run(ctx)
	if err != nil {
		return err
	}
	slog.Info("historical backfill complete", "chain_id", chainID, "checkpoint", result.Checkpoint.Number, "ranges", result.Ranges, "logs", result.Logs)
	return nil
}

func parseHash(raw string) (common.Hash, error) {
	if len(raw) != 66 || raw[:2] != "0x" {
		return common.Hash{}, errors.New("invalid hash")
	}
	b, err := hex.DecodeString(raw[2:])
	if err != nil {
		return common.Hash{}, errors.New("invalid hash")
	}
	return common.BytesToHash(b), nil
}
