package operational

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"reorgguard/internal/live"
	"reorgguard/internal/model"
)

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connections:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }

func TestBackpressuredResponseReleasesCapacity(t *testing.T) {
	entered := make(chan struct{}, 32)
	release := make(chan struct{})
	close(release)
	api := &Server{ChainID: 1, Store: &fakeStore{entered: entered, release: release,
		rows: []model.Log{{BlockNumber: 1, Data: make([]byte, 32<<10)}}},
		Live:   fakeLive{x: live.Status{Ready: true, State: "healthy_caught_up"}},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	listener := &pipeListener{connections: make(chan net.Conn, 33), closed: make(chan struct{})}
	server := NewHTTPServer(api.Handler())
	server.WriteTimeout = 50 * time.Millisecond
	done := make(chan struct{})
	go func() { _ = server.Serve(listener); close(done) }()
	var clients []net.Conn
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
		_ = server.Close()
		<-done
	}()
	for range 32 {
		peer, client := net.Pipe()
		listener.connections <- peer
		clients = append(clients, client)
		if _, err := fmt.Fprint(client, "GET /v1/logs?limit=1 HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
	}
	for range 32 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("page handler did not enter")
		}
	}
	// The real server constructor applies the deadline. A deadline
	// of 50 ms keeps this regression fast while testing the production wiring.
	select {
	case <-time.After(120 * time.Millisecond):
	case <-done:
		t.Fatal("server stopped unexpectedly")
	}
	peer, client := net.Pipe()
	listener.connections <- peer
	clients = append(clients, client)
	_ = client.SetDeadline(time.Now().Add(time.Second))
	if _, err := fmt.Fprint(client, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("slow readers retained all capacity: status %d", response.StatusCode)
	}
}
