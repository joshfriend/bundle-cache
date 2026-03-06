//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

// ─── Pure unit tests ────────────────────────────────────────────────────────

func TestBundleFilename(t *testing.T) {
	tests := []struct {
		cacheKey string
		want     string
	}{
		{"my-project:assembleRelease", "my-project-assembleRelease.tar.zst"},
		{"my-project:assemble", "my-project-assemble.tar.zst"},
		{"simple", "simple.tar.zst"},
		{"a:b:c", "a-b-c.tar.zst"},
	}
	for _, tt := range tests {
		if got := bundleFilename(tt.cacheKey); got != tt.want {
			t.Errorf("bundleFilename(%q) = %q, want %q", tt.cacheKey, got, tt.want)
		}
	}
}

func TestS3Key(t *testing.T) {
	tests := []struct {
		commit, cacheKey, bundleFile, want string
	}{
		{
			"abc123", "my-key", "my-key.tar.zst",
			"abc123/my-key/my-key.tar.zst",
		},
		{
			"deadbeef", "my-project:assemble", "my-project-assemble.tar.zst",
			"deadbeef/my-project:assemble/my-project-assemble.tar.zst",
		},
	}
	for _, tt := range tests {
		if got := s3Key(tt.commit, tt.cacheKey, tt.bundleFile); got != tt.want {
			t.Errorf("s3Key(%q, %q, %q) = %q, want %q",
				tt.commit, tt.cacheKey, tt.bundleFile, got, tt.want)
		}
	}
}

// ─── conventionBuildDirs tests ───────────────────────────────────────────────

func TestConventionBuildDirs(t *testing.T) {
	t.Run("empty directory returns nothing", func(t *testing.T) {
		root := t.TempDir()
		if got := conventionBuildDirs(root, []string{"buildSrc"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("buildSrc without build subdir is excluded", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc"), 0o755))
		if got := conventionBuildDirs(root, []string{"buildSrc"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("buildSrc/build is included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"buildSrc"})
		if len(got) != 1 || got[0] != "buildSrc/build" {
			t.Errorf("got %v, want [buildSrc/build]", got)
		}
	})

	t.Run("buildSrc not included when not in config", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		// build-logic is configured but doesn't exist; buildSrc exists but isn't configured.
		got := conventionBuildDirs(root, []string{"build-logic"})
		if len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("build-logic/build included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"build-logic"})
		if len(got) != 1 || got[0] != "build-logic/build" {
			t.Errorf("got %v, want [build-logic/build]", got)
		}
	})

	t.Run("multiple explicit dirs all checked", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"buildSrc", "build-logic"})
		sort.Strings(got)
		if len(got) != 2 || got[0] != "build-logic/build" || got[1] != "buildSrc/build" {
			t.Errorf("got %v, want [build-logic/build buildSrc/build]", got)
		}
	})

	t.Run("glob plugins/* finds subdirectory build dirs", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"plugins/*"})
		if len(got) != 1 || got[0] != "plugins/foo/build" {
			t.Errorf("got %v, want [plugins/foo/build]", got)
		}
	})

	t.Run("glob plugins/* excludes subdirs without a build dir", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo"), 0o755))
		if got := conventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("glob plugins/* excludes files named build", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo"), 0o755))
		must(t, os.WriteFile(filepath.Join(root, "plugins", "foo", "build"), []byte("nope"), 0o644))
		if got := conventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("glob plugins/* finds multiple subdirectories", func(t *testing.T) {
		root := t.TempDir()
		for _, p := range []string{"alpha", "beta", "gamma"} {
			must(t, os.MkdirAll(filepath.Join(root, "plugins", p, "build"), 0o755))
		}
		got := conventionBuildDirs(root, []string{"plugins/*"})
		if len(got) != 3 {
			t.Errorf("expected 3 entries, got %v", got)
		}
	})

	t.Run("missing glob parent directory is silently ignored", func(t *testing.T) {
		root := t.TempDir()
		if got := conventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty for missing parent, got %v", got)
		}
	})

	t.Run("buildSrc and plugins/* combined", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"buildSrc", "plugins/*"})
		sort.Strings(got)
		if len(got) != 2 || got[0] != "buildSrc/build" || got[1] != "plugins/foo/build" {
			t.Errorf("got %v, want [buildSrc/build plugins/foo/build]", got)
		}
	})
}

// ─── projectDirSources tests ─────────────────────────────────────────────────

func TestProjectDirSources(t *testing.T) {
	defaultBuilds := []string{"buildSrc"}

	t.Run("empty project dir returns no sources", func(t *testing.T) {
		root := t.TempDir()
		if got := projectDirSources(root, defaultBuilds); len(got) != 0 {
			t.Errorf("expected no sources, got %v", got)
		}
	})

	t.Run("configuration-cache source has correct BaseDir and Path", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, ".gradle", "configuration-cache"), 0o755))
		sources := projectDirSources(root, defaultBuilds)
		if len(sources) != 1 {
			t.Fatalf("expected 1 source, got %v", sources)
		}
		wantBase := filepath.Join(root, ".gradle")
		if sources[0].BaseDir != wantBase {
			t.Errorf("BaseDir = %q, want %q", sources[0].BaseDir, wantBase)
		}
		if sources[0].Path != "./configuration-cache" {
			t.Errorf("Path = %q, want ./configuration-cache", sources[0].Path)
		}
	})

	t.Run("buildSrc/build source has correct BaseDir and Path", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		sources := projectDirSources(root, defaultBuilds)
		if len(sources) != 1 {
			t.Fatalf("expected 1 source, got %v", sources)
		}
		if sources[0].BaseDir != root {
			t.Errorf("BaseDir = %q, want %q", sources[0].BaseDir, root)
		}
		if sources[0].Path != "./buildSrc/build" {
			t.Errorf("Path = %q, want ./buildSrc/build", sources[0].Path)
		}
	})

	t.Run("build-logic included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		sources := projectDirSources(root, []string{"build-logic"})
		if len(sources) != 1 || sources[0].Path != "./build-logic/build" {
			t.Errorf("expected build-logic/build, got %v", sources)
		}
	})

	t.Run("all dirs present with plugins glob returns expected count", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, ".gradle", "configuration-cache"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		sources := projectDirSources(root, []string{"buildSrc", "plugins/*"})
		// configuration-cache + buildSrc/build + plugins/foo/build = 3
		if len(sources) != 3 {
			t.Errorf("expected 3 sources, got %d: %v", len(sources), sources)
		}
	})
}

// ─── restoreProjectDirs tests ────────────────────────────────────────────────

func TestRestoreProjectDirs(t *testing.T) {
	defaultBuilds := []string{"buildSrc"}

	t.Run("no project dirs in bundle — no error, nothing created", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		if err := restoreProjectDirs(tmpDir, projectDir, defaultBuilds); err != nil {
			t.Fatal(err)
		}
		entries, _ := os.ReadDir(projectDir)
		if len(entries) != 0 {
			t.Errorf("expected empty project dir, got %v", entries)
		}
	})

	t.Run("configuration-cache symlinked into .gradle/", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		srcCC := filepath.Join(tmpDir, "configuration-cache")
		must(t, os.Mkdir(srcCC, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, defaultBuilds))

		dst := filepath.Join(projectDir, ".gradle", "configuration-cache")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != srcCC {
			t.Errorf("symlink target = %q, want %q", target, srcCC)
		}
	})

	t.Run("buildSrc/build symlinked into project", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		src := filepath.Join(tmpDir, "buildSrc", "build")
		must(t, os.MkdirAll(src, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"buildSrc"}))

		dst := filepath.Join(projectDir, "buildSrc", "build")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != src {
			t.Errorf("symlink target = %q, want %q", target, src)
		}
	})

	t.Run("build-logic/build symlinked when configured", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		src := filepath.Join(tmpDir, "build-logic", "build")
		must(t, os.MkdirAll(src, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"build-logic"}))

		dst := filepath.Join(projectDir, "build-logic", "build")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != src {
			t.Errorf("symlink target = %q, want %q", target, src)
		}
	})

	t.Run("build-logic not restored when not in config", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		// build-logic is in the bundle but not configured.
		must(t, os.MkdirAll(filepath.Join(tmpDir, "build-logic", "build"), 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"buildSrc"}))

		dst := filepath.Join(projectDir, "build-logic", "build")
		if _, err := os.Lstat(dst); err == nil {
			t.Errorf("build-logic/build should not have been restored when not configured")
		}
	})

	t.Run("plugins/foo/build symlinked via glob", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		src := filepath.Join(tmpDir, "plugins", "foo", "build")
		must(t, os.MkdirAll(src, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"plugins/*"}))

		dst := filepath.Join(projectDir, "plugins", "foo", "build")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != src {
			t.Errorf("symlink target = %q, want %q", target, src)
		}
	})

	t.Run("existing directory at destination is replaced by symlink", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		srcCC := filepath.Join(tmpDir, "configuration-cache")
		must(t, os.Mkdir(srcCC, 0o755))
		// Pre-create destination as a real directory (simulating a prior run).
		must(t, os.MkdirAll(filepath.Join(projectDir, ".gradle", "configuration-cache"), 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, defaultBuilds))

		dst := filepath.Join(projectDir, ".gradle", "configuration-cache")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s after replacement: %v", dst, err)
		}
		if target != srcCC {
			t.Errorf("symlink target = %q, want %q", target, srcCC)
		}
	})

	t.Run("all dirs present with correct config — all symlinked", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(tmpDir, "configuration-cache"), 0o755))
		must(t, os.MkdirAll(filepath.Join(tmpDir, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(tmpDir, "plugins", "bar", "build"), 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"buildSrc", "plugins/*"}))

		for _, dst := range []string{
			filepath.Join(projectDir, ".gradle", "configuration-cache"),
			filepath.Join(projectDir, "buildSrc", "build"),
			filepath.Join(projectDir, "plugins", "bar", "build"),
		} {
			if _, err := os.Readlink(dst); err != nil {
				t.Errorf("expected symlink at %s: %v", dst, err)
			}
		}
	})
}

// ─── Git history walk tests ──────────────────────────────────────────────────

// TestHistoryCommits creates a temporary git repository with a known commit
// graph and verifies that the author-block counting logic matches the
// bundled-cache-manager.rb algorithm.
func TestHistoryCommits(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx := context.Background()
	repo := t.TempDir()

	// run executes a git command in the test repo with a common identity.
	run := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test",
			"GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test",
			"GIT_COMMITTER_EMAIL=test@test.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}

	// commit makes an empty commit attributed to the given author name.
	commit := func(author, msg string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", "-C", repo, "commit", "--allow-empty", "-m", msg)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME="+author,
			"GIT_AUTHOR_EMAIL="+author+"@test.com",
			"GIT_COMMITTER_NAME="+author,
			"GIT_COMMITTER_EMAIL="+author+"@test.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("commit %q by %s: %v\n%s", msg, author, err, out)
		}
	}

	run("init")
	run("config", "user.email", "test@test.com")
	run("config", "user.name", "Test")

	// Build history (oldest → newest):
	//   Alice(3), Bob(2), Alice(1)
	// git log shows newest first:
	//   AliceFinal | Bob1 Bob0 | Alice2 Alice1 Alice0
	//    block 1       block 2      block 3
	for i := 0; i < 3; i++ {
		commit("Alice", fmt.Sprintf("alice %d", i))
	}
	for i := 0; i < 2; i++ {
		commit("Bob", fmt.Sprintf("bob %d", i))
	}
	commit("Alice", "alice final")

	t.Run("maxBlocks=1 returns only the most recent author block", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 1 {
			t.Errorf("expected 1 commit (just 'alice final'), got %d: %v", len(commits), commits)
		}
	})

	t.Run("maxBlocks=2 returns first two author blocks", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 2)
		if err != nil {
			t.Fatal(err)
		}
		// Block1: AliceFinal (1) + Block2: Bob1, Bob0 (2) = 3 commits
		if len(commits) != 3 {
			t.Errorf("expected 3 commits, got %d: %v", len(commits), commits)
		}
	})

	t.Run("maxBlocks=3 returns all commits", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 6 {
			t.Errorf("expected 6 commits, got %d: %v", len(commits), commits)
		}
	})

	t.Run("maxBlocks larger than actual blocks returns all commits", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 20)
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 6 {
			t.Errorf("expected 6 commits, got %d: %v", len(commits), commits)
		}
	})

	t.Run("all returned commits have 40-char hex SHAs", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, sha := range commits {
			if len(sha) != 40 {
				t.Errorf("SHA %q has length %d, want 40", sha, len(sha))
			}
		}
	})

	t.Run("invalid ref returns error", func(t *testing.T) {
		_, err := historyCommits(ctx, repo, "refs/heads/nonexistent", 5)
		if err == nil {
			t.Error("expected error for invalid ref, got nil")
		}
	})
}

// ─── Round-trip archive test ─────────────────────────────────────────────────

// TestTarZstdRoundTrip verifies that createTarZstd → extractTarZstd preserves
// the expected directory structure, including multi-source archives.
func TestTarZstdRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("tar not available")
	}
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd not available")
	}

	ctx := context.Background()
	srcDir := t.TempDir()

	// caches/ source (under gradle-home)
	gradleHome := filepath.Join(srcDir, "gradle-home")
	must(t, os.MkdirAll(filepath.Join(gradleHome, "caches", "modules"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleHome, "caches", "modules", "entry.bin"), []byte("gradle data"), 0o644))

	// configuration-cache/ source (under .gradle/ inside project)
	gradleDir := filepath.Join(srcDir, "project", ".gradle")
	must(t, os.MkdirAll(filepath.Join(gradleDir, "configuration-cache"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleDir, "configuration-cache", "hash.bin"), []byte("config cache"), 0o644))

	sources := []tarSource{
		{BaseDir: gradleHome, Path: "./caches"},
		{BaseDir: gradleDir, Path: "./configuration-cache"},
	}

	// Create archive into a buffer.
	var buf bytes.Buffer
	if err := createTarZstd(ctx, &buf, sources); err != nil {
		t.Fatalf("createTarZstd: %v", err)
	}

	// Extract into a fresh directory.
	dstDir := t.TempDir()
	if err := extractTarZstd(ctx, &buf, dstDir); err != nil {
		t.Fatalf("extractTarZstd: %v", err)
	}

	// Verify both source trees are present at the bundle root level.
	for _, rel := range []string{
		"caches/modules/entry.bin",
		"configuration-cache/hash.bin",
	} {
		path := filepath.Join(dstDir, rel)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected %s in extracted dir: %v", rel, err)
		}
	}

	// Verify file contents round-trip correctly.
	data, err := os.ReadFile(filepath.Join(dstDir, "caches", "modules", "entry.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "gradle data" {
		t.Errorf("content = %q, want %q", string(data), "gradle data")
	}
}

// TestTarZstdSymlinkDereference verifies that -h causes symlinked directories
// to be archived as real content (matching bundled-cache-manager.rb's -h flag).
func TestTarZstdSymlinkDereference(t *testing.T) {
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("tar not available")
	}
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd not available")
	}

	ctx := context.Background()
	srcDir := t.TempDir()
	realDir := t.TempDir()

	// Write a file into the real directory.
	must(t, os.WriteFile(filepath.Join(realDir, "data.txt"), []byte("hello"), 0o644))

	// Create caches/ as a symlink pointing to realDir.
	cachesLink := filepath.Join(srcDir, "caches")
	must(t, os.Symlink(realDir, cachesLink))

	var buf bytes.Buffer
	if err := createTarZstd(ctx, &buf, []tarSource{{BaseDir: srcDir, Path: "./caches"}}); err != nil {
		t.Fatalf("createTarZstd: %v", err)
	}

	dstDir := t.TempDir()
	if err := extractTarZstd(ctx, &buf, dstDir); err != nil {
		t.Fatalf("extractTarZstd: %v", err)
	}

	// The symlink should have been dereferenced — extracted as a real directory.
	info, err := os.Lstat(filepath.Join(dstDir, "caches"))
	if err != nil {
		t.Fatalf("caches/ not found in extracted dir: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Error("expected caches/ to be a real directory, got symlink (tar -h not working)")
	}

	// The file inside should be present.
	if _, err := os.Stat(filepath.Join(dstDir, "caches", "data.txt")); err != nil {
		t.Errorf("expected data.txt inside extracted caches/: %v", err)
	}
}

// ─── branchSlug tests ────────────────────────────────────────────────────────

func TestBranchSlug(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"main", "main"},
		{"feature/my-pr", "feature--my-pr"},
		{"fix/JIRA-123", "fix--JIRA-123"},
		{"refs/heads/main", "refs--heads--main"},
		{"branch with spaces", "branch-with-spaces"},
		{"a#b?c&d", "a-b-c-d"},
		{"feature/foo/bar", "feature--foo--bar"},
	}
	for _, tt := range tests {
		if got := branchSlug(tt.input); got != tt.want {
			t.Errorf("branchSlug(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDeltaCommit(t *testing.T) {
	tests := []struct {
		branch, want string
	}{
		{"main", "branches/main"},
		{"feature/my-pr", "branches/feature--my-pr"},
	}
	for _, tt := range tests {
		if got := deltaCommit(tt.branch); got != tt.want {
			t.Errorf("deltaCommit(%q) = %q, want %q", tt.branch, got, tt.want)
		}
	}
}

// ─── Delta archive round-trip test ───────────────────────────────────────────

// TestDeltaTarZstdRoundTrip verifies that createDeltaTarZstd/writeDeltaTar pack
// only the files listed and that they can be extracted back via extractTarZstd.
// It also exercises the mtime-based file selection used by SaveDeltaCmd: a
// "base" file is written before the marker and a "new" file after, and only the
// new file should appear in the delta.
func TestDeltaTarZstdRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd not available")
	}

	ctx := context.Background()

	// Set up a fake GradleUserHome with a caches/ directory.
	gradleHome := t.TempDir()
	cachesDir := filepath.Join(gradleHome, "caches")
	must(t, os.MkdirAll(filepath.Join(cachesDir, "modules-2"), 0o755))

	// Write a "base" file that predates the marker.
	baseFile := filepath.Join(cachesDir, "modules-2", "base.jar")
	must(t, os.WriteFile(baseFile, []byte("base content"), 0o644))

	// Touch the marker.
	markerPath := filepath.Join(gradleHome, ".cache-restore-marker")
	must(t, touchMarkerFile(markerPath))

	// Write a "new" file after the marker — this is what the build created.
	newFile := filepath.Join(cachesDir, "modules-2", "new.jar")
	must(t, os.WriteFile(newFile, []byte("new content"), 0o644))

	// Determine which files are newer than the marker (simulating SaveDeltaCmd's scan).
	markerInfo, err := os.Stat(markerPath)
	must(t, err)
	since := markerInfo.ModTime()

	var newFiles []string
	must(t, filepath.Walk(cachesDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil || !fi.Mode().IsRegular() {
			return err
		}
		if fi.ModTime().After(since) {
			rel, _ := filepath.Rel(cachesDir, path)
			newFiles = append(newFiles, filepath.Join("caches", rel))
		}
		return nil
	}))

	if len(newFiles) == 0 {
		t.Skip("mtime resolution too coarse to distinguish marker from new file; skipping")
	}

	// Pack the delta.
	var buf bytes.Buffer
	must(t, createDeltaTarZstd(ctx, &buf, gradleHome, newFiles))

	// Extract into a fresh directory and verify only the new file is present.
	dstDir := t.TempDir()
	must(t, extractTarZstd(ctx, &buf, dstDir))

	newPath := filepath.Join(dstDir, "caches", "modules-2", "new.jar")
	if _, err := os.Stat(newPath); err != nil {
		t.Errorf("expected new.jar in delta: %v", err)
	}
	basePath := filepath.Join(dstDir, "caches", "modules-2", "base.jar")
	if _, err := os.Stat(basePath); err == nil {
		t.Error("base.jar should not be in delta — it predates the marker")
	}

	data, err := os.ReadFile(newPath)
	must(t, err)
	if string(data) != "new content" {
		t.Errorf("new.jar content = %q, want %q", string(data), "new content")
	}
}

// ─── Delta scan benchmark ─────────────────────────────────────────────────────

// BenchmarkDeltaScan measures the mtime-walk hot path used by SaveDeltaCmd:
// EvalSymlinks + filepath.Walk + fi.ModTime().After(marker). The directory
// structure mirrors a real Gradle cache (nested group/artifact/version dirs).
//
// Run with:
//
//	go test -bench=BenchmarkDeltaScan -benchtime=5s ./cmd/gradle-cache/
//
// Output includes a "files/op" metric so ns/file is straightforward to derive.
func BenchmarkDeltaScan(b *testing.B) {
	for _, nFiles := range []int{5_000, 20_000, 50_000} {
		nFiles := nFiles
		b.Run(fmt.Sprintf("files=%d", nFiles), func(b *testing.B) {
			// Build a simulated Gradle cache:
			//   root/caches/group-N/artifact-N/vX.Y/file-N.jar
			// 50 groups × 20 artifacts gives ~1000 leaf dirs for 50k files.
			root := b.TempDir()
			caches := filepath.Join(root, "caches")
			for i := range nFiles {
				dir := filepath.Join(caches,
					fmt.Sprintf("group%d", i%50),
					fmt.Sprintf("artifact%d", i%20),
				)
				if err := os.MkdirAll(dir, 0o755); err != nil {
					b.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d.jar", i)), []byte("x"), 0o644); err != nil {
					b.Fatal(err)
				}
			}

			// Write marker after all "base" files — mirrors what restore does.
			markerPath := filepath.Join(root, ".cache-restore-marker")
			if err := touchMarkerFile(markerPath); err != nil {
				b.Fatal(err)
			}
			markerInfo, err := os.Stat(markerPath)
			if err != nil {
				b.Fatal(err)
			}
			since := markerInfo.ModTime()

			// Resolve the caches dir, just as SaveDeltaCmd does (it may be a symlink).
			realCaches, err := filepath.EvalSymlinks(caches)
			if err != nil {
				realCaches = caches
			}

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				var found int
				if walkErr := filepath.Walk(realCaches, func(path string, fi os.FileInfo, err error) error {
					if err != nil || !fi.Mode().IsRegular() {
						return err
					}
					if fi.ModTime().After(since) {
						found++
					}
					return nil
				}); walkErr != nil {
					b.Fatal(walkErr)
				}
				_ = found
			}
			b.ReportMetric(float64(nFiles), "files/op")
		})
	}
}

// BenchmarkDeltaScanReal exercises the production collectNewFiles path against a real
// extracted cache. Point GRADLE_CACHE_BENCH_DIR at the caches/ directory from a prior
// restore (the symlink or its real target) and run:
//
//	GRADLE_CACHE_BENCH_DIR=~/.gradle/caches \
//	  go test -bench=BenchmarkDeltaScanReal -benchtime=3x ./cmd/gradle-cache/
func BenchmarkDeltaScanReal(b *testing.B) {
	cachesDir := os.Getenv("GRADLE_CACHE_BENCH_DIR")
	if cachesDir == "" {
		b.Skip("set GRADLE_CACHE_BENCH_DIR to a Gradle caches/ directory to run this benchmark")
	}

	realCaches, err := filepath.EvalSymlinks(cachesDir)
	if err != nil {
		b.Fatalf("EvalSymlinks(%s): %v", cachesDir, err)
	}
	gradleHome := filepath.Dir(realCaches) // parent of caches/ is the Gradle user home

	// Count total files once, outside the timed loop.
	var totalFiles int
	if err := filepath.Walk(realCaches, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			totalFiles++
		}
		return nil
	}); err != nil {
		b.Fatalf("pre-count walk: %v", err)
	}
	b.Logf("cache: %d regular files at %s", totalFiles, realCaches)

	// Write the marker after all cache files so they all predate it — simulating
	// the "clean restore, no build has run yet" baseline.
	markerPath := filepath.Join(b.TempDir(), ".bench-marker")
	if err := touchMarkerFile(markerPath); err != nil {
		b.Fatal(err)
	}
	markerInfo, err := os.Stat(markerPath)
	if err != nil {
		b.Fatal(err)
	}
	since := markerInfo.ModTime()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		files, err := collectNewFiles(realCaches, since, gradleHome)
		if err != nil {
			b.Fatal(err)
		}
		_ = files
	}
	b.ReportMetric(float64(totalFiles), "files/op")
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
