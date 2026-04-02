package gradlecache

import (
	"os"

	"golang.org/x/sys/unix"
)

// adviseWillNeed hints to the kernel that the file will be read soon,
// triggering async readahead without blocking on actual I/O.
func adviseWillNeed(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_WILLNEED)
	_ = f.Close()
}
