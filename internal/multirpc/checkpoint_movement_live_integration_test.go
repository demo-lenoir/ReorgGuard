//go:build integration

package multirpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"reorgguard/internal/live"
	"reorgguard/internal/multirpc"
)

func TestDirectLiveCheckpointMovesBetweenHeaderAndRangeLogs(t *testing.T) {
	coordinator, source, db, dsn, want := multirpc.DirectCheckpointMovementFixture(t)
	svc := &live.Service{Canonical: coordinator, Head: source, Config: live.Config{
		PollInterval: time.Hour, SweepTimeout: 5 * time.Second,
		BackoffMin: time.Millisecond, BackoffMax: time.Second, LagTolerance: 0,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- svc.Run(ctx) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if svc.Status().Checkpoint == 3 && svc.Status().Ready {
			break
		}
		select {
		case err := <-result:
			cancel()
			t.Fatalf("live service stopped before B3 reconciliation: %v, status=%+v", err, svc.Status())
		case <-deadline.C:
			cancel()
			t.Fatalf("live service did not reconcile B3: %+v", svc.Status())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected live shutdown: %v", err)
	}
	multirpc.CheckDirectMovementProjection(t, db, dsn, want)
}
