package telemetry

import (
	"context"
	"testing"
)

func TestTracingOffAndInvalidMode(t *testing.T) {
	shutdown, err := Init("off")
	if err != nil || shutdown == nil {
		t.Fatalf("off: %v", err)
	}
	ctx, span := Start(context.Background(), "test.boundary")
	if ctx == nil || span == nil {
		t.Fatal("missing no-op span")
	}
	Failure(span, "bounded_failure")
	span.End()
	if err = shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = Init("remote-secret-url"); err == nil {
		t.Fatal("invalid mode accepted")
	}
}
