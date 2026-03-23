package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"
)

// safeTarEntryName validates that a tar entry name is a relative path that
// stays within the archive root. It rejects absolute paths and parent
// traversals. Returns the cleaned name.
func safeTarEntryName(name string) (string, error) {
	clean := filepath.Clean(name)
	if filepath.IsAbs(clean) {
		return "", errors.Errorf("tar entry %q: absolute path not allowed", name)
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", errors.Errorf("tar entry %q escapes destination directory", name)
	}
	return clean, nil
}

// safeSymlinkTarget validates that a symlink's target resolves within the
// archive root when evaluated relative to the symlink's own location in the
// tar namespace. Absolute targets are rejected outright. This validation is
// performed in the tar-entry namespace (before routing) so it is independent
// of which destination directory entries are routed to.
func safeSymlinkTarget(entryName, linkname string) error {
	if filepath.IsAbs(linkname) {
		return errors.Errorf("symlink %q -> %q: absolute target not allowed", entryName, linkname)
	}
	resolved := filepath.Clean(filepath.Join(filepath.Dir(entryName), linkname))
	if resolved == ".." || strings.HasPrefix(resolved, ".."+string(os.PathSeparator)) {
		return errors.Errorf("symlink %q -> %q escapes destination directory", entryName, linkname)
	}
	return nil
}
