// gradle-cache restores and saves Gradle build cache bundles from S3.
//
// Bundles are stored at s3://{bucket}/{commit}/{cache-key}/{bundle-file},
// where bundle-file is the cache key with colons replaced by dashes + ".tar.zst".
// This format is compatible with the bundled-cache-manager Ruby script.
//
// On restore, the tool walks the local git history (counting distinct-author
// "blocks") to find the most recent S3 hit, downloads it, extracts it to a
// temporary directory, and symlinks $GRADLE_USER_HOME/caches into place.
// With --project-dir, also restores configuration-cache and convention build
// dirs (buildSrc/build, plugins/*/build) if present in the bundle.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type CLI struct {
	Restore RestoreCmd `cmd:"" help:"Find the newest cached bundle in history and restore it to GRADLE_USER_HOME."`
	Save    SaveCmd    `cmd:"" help:"Bundle GRADLE_USER_HOME/caches and upload to S3 tagged with a commit SHA."`
}

type s3Flags struct {
	Bucket string `help:"S3 bucket name." required:""`
	Region string `help:"AWS region." default:"us-west-2"`
}

// RestoreCmd downloads and extracts a Gradle cache bundle, then symlinks
// $GRADLE_USER_HOME/caches to the extracted directory.
// Also restores configuration-cache and included build output dirs if present in the bundle.
type RestoreCmd struct {
	s3Flags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	GitDir         string   `help:"Path to the git repository used for history walking." default:"." type:"path"`
	Ref            string   `help:"Git ref to start the history walk from." default:"HEAD"`
	Commit         string   `help:"Specific commit SHA to try directly, skipping history walk."`
	MaxBlocks      int      `help:"Number of distinct-author commit blocks to search." default:"20"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to restore (relative to project root). Use 'dir/*' to restore build/ for all subdirectories. May be repeated." name:"included-build"`
}

func (c *RestoreCmd) AfterApply() error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	return nil
}

func (c *RestoreCmd) Run(ctx context.Context) error {
	client, err := newMinioClient(c.Region)
	if err != nil {
		return err
	}

	bundleFile := bundleFilename(c.CacheKey)

	// Determine which commits to check.
	var commits []string
	if c.Commit != "" {
		commits = []string{c.Commit}
	} else {
		commits, err = historyCommits(ctx, c.GitDir, c.Ref, c.MaxBlocks)
		if err != nil {
			return errors.Wrap(err, "walk git history")
		}
	}

	// Find the first S3 hit.
	var hitKey string
	for _, sha := range commits {
		key := s3Key(sha, c.CacheKey, bundleFile)
		_, statErr := client.StatObject(ctx, c.Bucket, key, minio.StatObjectOptions{})
		if statErr == nil {
			hitKey = key
			fmt.Fprintf(os.Stderr, "Cache hit: %s\n", key) //nolint:forbidigo
			break
		}
	}

	if hitKey == "" {
		fmt.Fprintln(os.Stderr, "No cache bundle found in history.") //nolint:forbidigo
		return nil
	}

	// Download and extract into a temp directory.
	tmpDir, err := os.MkdirTemp("", "gradle-cache-*")
	if err != nil {
		return errors.Wrap(err, "create temp dir")
	}

	fmt.Fprintf(os.Stderr, "Downloading %s...\n", hitKey) //nolint:forbidigo
	obj, err := client.GetObject(ctx, c.Bucket, hitKey, minio.GetObjectOptions{})
	if err != nil {
		return errors.Wrap(err, "get object")
	}
	defer obj.Close() //nolint:errcheck

	if err := extractTarZstd(ctx, obj, tmpDir); err != nil {
		return errors.Wrap(err, "extract bundle")
	}

	// Symlink $GRADLE_USER_HOME/caches → tmpDir/caches.
	cachesTarget := filepath.Join(tmpDir, "caches")
	if _, err := os.Stat(cachesTarget); err != nil {
		return errors.Errorf("extracted bundle does not contain a caches/ directory: %w", err)
	}
	localCaches := filepath.Join(c.GradleUserHome, "caches")
	if err := os.RemoveAll(localCaches); err != nil {
		return errors.Wrap(err, "remove existing caches dir")
	}
	if err := os.Symlink(cachesTarget, localCaches); err != nil {
		return errors.Wrap(err, "symlink caches dir")
	}
	fmt.Fprintf(os.Stderr, "Restored: %s -> %s\n", localCaches, cachesTarget) //nolint:forbidigo

	// Restore configuration-cache and convention build dirs from the current directory.
	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}
	if err := restoreProjectDirs(tmpDir, projectDir, c.IncludedBuilds); err != nil {
		return err
	}

	return nil
}

// restoreProjectDirs symlinks configuration-cache and included build output dirs
// from tmpDir into projectDir, if present in the extracted bundle.
// includedBuilds specifies which directories to check (see conventionBuildDirs).
func restoreProjectDirs(tmpDir, projectDir string, includedBuilds []string) error {
	// configuration-cache: archived at ./configuration-cache/ relative to the bundle root
	// (not under .gradle/), matching the bundled-cache-manager.rb archive format.
	srcCC := filepath.Join(tmpDir, "configuration-cache")
	if _, err := os.Stat(srcCC); err == nil {
		dstCC := filepath.Join(projectDir, ".gradle", "configuration-cache")
		if err := os.MkdirAll(filepath.Dir(dstCC), 0o750); err != nil {
			return errors.Wrap(err, "create .gradle dir")
		}
		if err := os.RemoveAll(dstCC); err != nil {
			return errors.Wrap(err, "remove existing configuration-cache")
		}
		if err := os.Symlink(srcCC, dstCC); err != nil {
			return errors.Wrap(err, "symlink configuration-cache")
		}
		fmt.Fprintf(os.Stderr, "Restored: %s -> %s\n", dstCC, srcCC) //nolint:forbidigo
	}

	// Included build output dirs present in the extracted bundle.
	for _, rel := range conventionBuildDirs(tmpDir, includedBuilds) {
		src := filepath.Join(tmpDir, rel)
		dst := filepath.Join(projectDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return errors.Errorf("create parent of %s: %w", dst, err)
		}
		if err := os.RemoveAll(dst); err != nil {
			return errors.Errorf("remove existing %s: %w", dst, err)
		}
		if err := os.Symlink(src, dst); err != nil {
			return errors.Errorf("symlink %s: %w", rel, err)
		}
		fmt.Fprintf(os.Stderr, "Restored: %s -> %s\n", dst, src) //nolint:forbidigo
	}

	return nil
}

// SaveCmd archives $GRADLE_USER_HOME/caches and uploads it to S3.
// Also includes configuration-cache and included build output dirs if they exist.
type SaveCmd struct {
	s3Flags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Commit         string   `help:"Commit SHA to tag this bundle with. Defaults to HEAD of --git-dir."`
	GitDir         string   `help:"Path to the git repository (used to resolve HEAD when --commit is not set)." default:"." type:"path"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to archive (relative to project root). Use 'dir/*' to archive build/ for all subdirectories. May be repeated." name:"included-build"`
}

func (c *SaveCmd) AfterApply(ctx context.Context) error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	if c.Commit == "" {
		sha, err := gitHead(ctx, c.GitDir)
		if err != nil {
			return errors.Wrap(err, "resolve HEAD commit (pass --commit to override)")
		}
		c.Commit = sha
	}
	return nil
}

func (c *SaveCmd) Run(ctx context.Context) error {
	cachesDir := filepath.Join(c.GradleUserHome, "caches")
	if _, err := os.Stat(cachesDir); err != nil {
		return errors.Errorf("caches directory not found at %s: %w", cachesDir, err)
	}

	client, err := newMinioClient(c.Region)
	if err != nil {
		return err
	}

	bundleFile := bundleFilename(c.CacheKey)
	key := s3Key(c.Commit, c.CacheKey, bundleFile)

	// Skip upload if bundle already exists.
	if _, err := client.StatObject(ctx, c.Bucket, key, minio.StatObjectOptions{}); err == nil {
		fmt.Fprintf(os.Stderr, "Bundle already exists: %s\n", key) //nolint:forbidigo
		return nil
	}

	// Build the list of tar sources: always include caches, plus any
	// configuration-cache and convention build dirs in the current directory.
	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}
	sources := []tarSource{{BaseDir: c.GradleUserHome, Path: "./caches"}}
	sources = append(sources, projectDirSources(projectDir, c.IncludedBuilds)...)

	fmt.Fprintf(os.Stderr, "Saving bundle to %s...\n", key) //nolint:forbidigo

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		defer pw.Close() //nolint:errcheck
		errCh <- createTarZstd(ctx, pw, sources)
	}()

	_, err = client.PutObject(ctx, c.Bucket, key, pr, -1, minio.PutObjectOptions{
		ContentType: "application/zstd",
	})
	if err != nil {
		return errors.Wrap(err, "upload bundle")
	}

	if err := <-errCh; err != nil {
		return errors.Wrap(err, "create bundle archive")
	}

	fmt.Fprintf(os.Stderr, "Saved: %s\n", key) //nolint:forbidigo
	return nil
}

// projectDirSources returns tarSource entries for project-specific dirs:
// configuration-cache (from projectDir/.gradle/) and included build output dirs,
// for any that exist on disk. The archive paths match bundled-cache-manager.rb.
func projectDirSources(projectDir string, includedBuilds []string) []tarSource {
	var sources []tarSource

	// configuration-cache is archived at ./configuration-cache/ (not .gradle/configuration-cache/)
	// so that restore can symlink it to a different location (projectDir/.gradle/).
	gradleDir := filepath.Join(projectDir, ".gradle")
	if _, err := os.Stat(filepath.Join(gradleDir, "configuration-cache")); err == nil {
		sources = append(sources, tarSource{BaseDir: gradleDir, Path: "./configuration-cache"})
	}

	// Included build output dirs relative to projectDir.
	for _, rel := range conventionBuildDirs(projectDir, includedBuilds) {
		sources = append(sources, tarSource{BaseDir: projectDir, Path: "./" + rel})
	}

	return sources
}

func main() {
	cli := &CLI{}
	kctx := kong.Parse(cli, kong.UsageOnError(), kong.HelpOptions{Compact: true})
	ctx := context.Background()
	kctx.BindTo(ctx, (*context.Context)(nil))
	kctx.FatalIfErrorf(kctx.Run(ctx))
}

// bundleFilename converts a cache key to its S3 filename, matching the Ruby bundled-cache-manager.
func bundleFilename(cacheKey string) string {
	return strings.ReplaceAll(cacheKey, ":", "-") + ".tar.zst"
}

// s3Key builds the S3 object key for a given commit, cache key, and bundle filename.
func s3Key(commit, cacheKey, bundleFile string) string {
	return commit + "/" + cacheKey + "/" + bundleFile
}

// tarSource specifies a (base directory, relative path) pair for inclusion in a tar archive.
type tarSource struct {
	BaseDir string
	Path    string
}

// newMinioClient builds a minio client using the standard AWS credential chain,
// with IRSA (Kubernetes service account token) support when the appropriate
// environment variables are set.
func newMinioClient(region string) (*minio.Client, error) {
	var creds *credentials.Credentials

	if tokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"); tokenFile != "" {
		stsEndpoint := "https://sts." + region + ".amazonaws.com"
		stsWebID := &credentials.STSWebIdentity{
			STSEndpoint: stsEndpoint,
			GetWebIDTokenExpiry: func() (*credentials.WebIdentityToken, error) {
				token, err := os.ReadFile(tokenFile) //nolint:gosec // tokenFile comes from AWS_WEB_IDENTITY_TOKEN_FILE env var
				if err != nil {
					return nil, errors.Wrap(err, "read web identity token")
				}
				return &credentials.WebIdentityToken{Token: string(token)}, nil
			},
		}
		creds = credentials.New(stsWebID)
	} else {
		transport, err := minio.DefaultTransport(true)
		if err != nil {
			return nil, errors.Wrap(err, "create default transport")
		}
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{Client: &http.Client{Transport: transport}},
		})
	}

	client, err := minio.New("s3.amazonaws.com", &minio.Options{
		Creds:  creds,
		Secure: true,
		Region: region,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create minio client")
	}
	return client, nil
}

// historyCommits runs git log from the given ref and returns commit SHAs within
// maxBlocks distinct-author "blocks" (same algorithm as bundled-cache-manager.rb).
func historyCommits(ctx context.Context, gitDir, ref string, maxBlocks int) ([]string, error) {
	rawCount := maxBlocks * 10
	//nolint:gosec // ref is a user-supplied git ref, not a shell injection vector
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "log", "--first-parent",
		fmt.Sprintf("-n%d", rawCount), "--format=%H\t%an", ref)
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.Errorf("git log: %w", err)
	}

	var commits []string
	prevAuthor := ""
	blocksSeen := 0

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) != 2 {
			continue
		}
		sha, author := parts[0], parts[1]
		if author != prevAuthor {
			blocksSeen++
			prevAuthor = author
			if blocksSeen > maxBlocks {
				break
			}
		}
		commits = append(commits, sha)
	}
	return commits, errors.Wrap(scanner.Err(), "scan git log")
}

// gitHead returns the SHA of HEAD in the given git directory.
func gitHead(ctx context.Context, gitDir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "rev-parse", "HEAD") //nolint:gosec
	out, err := cmd.Output()
	if err != nil {
		return "", errors.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// extractTarZstd decompresses a zstd-compressed tar stream from r into dir.
func extractTarZstd(ctx context.Context, r io.Reader, dir string) error {
	zstdCmd := exec.CommandContext(ctx, "zstd", "-dc", "-T0")
	tarCmd := exec.CommandContext(ctx, "tar", "-xpf", "-", "-C", dir) //nolint:gosec

	zstdCmd.Stdin = r
	zstdStdout, err := zstdCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "zstd stdout pipe")
	}

	var zstdStderr, tarStderr bytes.Buffer
	zstdCmd.Stderr = &zstdStderr
	tarCmd.Stdin = zstdStdout
	tarCmd.Stderr = &tarStderr

	if err := zstdCmd.Start(); err != nil {
		return errors.Wrap(err, "start zstd")
	}
	if err := tarCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "start tar"), zstdCmd.Wait())
	}

	zstdErr := zstdCmd.Wait()
	tarErr := tarCmd.Wait()

	var errs []error
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar: %w: %s", tarErr, tarStderr.String()))
	}
	return errors.Join(errs...)
}

// createTarZstd creates a zstd-compressed tar archive from the given sources and
// writes it to w. Uses -h to dereference symlinks, matching bundled-cache-manager.rb.
// Multiple sources map to multiple -C baseDir path entries in the tar command,
// which is how bundled-cache-manager.rb combines caches + configuration-cache +
// convention build dirs into a single flat archive.
func createTarZstd(ctx context.Context, w io.Writer, sources []tarSource) error {
	args := []string{"-chf", "-"}
	for _, src := range sources {
		args = append(args, "-C", src.BaseDir, src.Path)
	}
	tarCmd := exec.CommandContext(ctx, "tar", args...) //nolint:gosec
	zstdCmd := exec.CommandContext(ctx, "zstd", "-T0", "-c")

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "tar stdout pipe")
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr
	zstdCmd.Stdin = tarStdout
	zstdCmd.Stdout = w
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "start tar")
	}
	if err := zstdCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "start zstd"), tarCmd.Wait())
	}

	tarErr := tarCmd.Wait()
	zstdErr := zstdCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar: %w: %s", tarErr, tarStderr.String()))
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// conventionBuildDirs returns the relative paths of included build output directories
// (i.e. <dir>/build) that exist within root, based on the includedBuilds configuration.
//
// Each entry in includedBuilds is a directory path relative to root. If the entry ends
// with "/*", all immediate subdirectories of the parent are scanned and any that contain
// a build/ subdirectory are included. Otherwise, <entry>/build is checked directly.
//
// Example values: "buildSrc", "build-logic", "plugins/*"
func conventionBuildDirs(root string, includedBuilds []string) []string {
	var result []string
	for _, entry := range includedBuilds {
		if strings.HasSuffix(entry, "/*") {
			// Scan all immediate subdirectories of the parent for a build/ subdir.
			parent := strings.TrimSuffix(entry, "/*")
			entries, err := os.ReadDir(filepath.Join(root, parent))
			if err != nil {
				continue
			}
			for _, sub := range entries {
				if !sub.IsDir() {
					continue
				}
				rel := parent + "/" + sub.Name() + "/build"
				if info, err := os.Stat(filepath.Join(root, rel)); err == nil && info.IsDir() {
					result = append(result, rel)
				}
			}
		} else {
			rel := entry + "/build"
			if info, err := os.Stat(filepath.Join(root, rel)); err == nil && info.IsDir() {
				result = append(result, rel)
			}
		}
	}
	return result
}
