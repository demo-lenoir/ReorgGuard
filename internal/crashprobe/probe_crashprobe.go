//go:build crashprobe

package crashprobe

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"time"
)

// Pause is enabled only in the explicitly tagged test binary. The controller
// receives the named milestone over localhost and releases or kills the child.
// Production binaries cannot activate this path through environment variables.
func Pause(ctx context.Context, point string) error {
	if os.Getenv("REORGGUARD_CRASHPROBE_POINT") != point {
		return nil
	}
	addr := os.Getenv("REORGGUARD_CRASHPROBE_ADDR")
	if addr == "" {
		return errors.New("crashprobe controller missing")
	}
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if _, err = conn.Write([]byte(point + "\n")); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != "go" {
		return errors.New("crashprobe controller rejected milestone")
	}
	return nil
}
