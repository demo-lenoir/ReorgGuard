//go:build !crashprobe

package crashprobe

import "context"

// Pause is compiled to a no-op in every production build. The crashprobe
// build tag substitutes deterministic process-test synchronization.
func Pause(context.Context, string) error { return nil }
