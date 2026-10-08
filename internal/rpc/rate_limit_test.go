package rpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTP429IsRateLimitedWithoutProviderBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("body-must-not-appear"))
	}))
	defer server.Close()
	client, err := NewHTTPClient(server.URL, time.Second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ChainID(context.Background())
	if !errors.Is(err, ErrRateLimited) || err.Error() != ErrRateLimited.Error() {
		t.Fatalf("rate limit response %v", err)
	}
}
