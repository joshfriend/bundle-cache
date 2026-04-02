//go:build !linux

package gradlecache

import (
	"io"
	"os"
)

// adviseWillNeed falls back to reading the entire file on non-Linux platforms
// where fadvise is not available.
func adviseWillNeed(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, f)
	_ = f.Close()
}
