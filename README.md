# gradle-cache

A CLI tool for saving and restoring Gradle build cache bundles from S3.

Bundles are stored keyed by commit SHA, so `restore` doesn't need to know
exactly which commit produced a given bundle. Instead, it walks the local git
history from a given ref (default: `HEAD`) and tries each commit SHA in order,
newest first, until it finds a bundle that exists. This means a developer
on a feature branch will automatically get the bundle from the most recent
main-branch commit that has one, without needing to know its SHA in advance.

The history walk counts distinct author-change boundaries rather than raw
commit count, so a long run of commits by the same author only consumes one
step of the search budget. The default search depth is 20 such boundaries.

## Installation

```sh
curl -fsSL https://raw.githubusercontent.com/joshfriend/gradle-cache-tool/main/scripts/install.sh | sh
```

This installs the latest release to `~/.local/bin`. Set `INSTALL_DIR` to override the destination, or `VERSION` to pin a specific release tag.

## Usage

### Base cache (main branch)

```
gradle-cache restore --bucket <bucket> --cache-key <key> [--ref main]
gradle-cache save    --bucket <bucket> --cache-key <key>
```

`--ref` controls where the history walk starts (default `HEAD`). When running
on a feature branch you typically want to pass `--ref main` so the walk
searches commits that CI has actually built cache bundles for.

`--included-build` (repeatable) controls which included build output directories
are archived alongside `$GRADLE_USER_HOME/caches`. Accepts a direct path
(`buildSrc`, `build-logic`) or a glob (`plugins/*`) to include all
subdirectories. Defaults to `buildSrc`.

### Branch delta cache (PR branches)

For PR builds, pass `--branch` to `restore` to apply a branch delta in the same invocation. The delta bundle is downloaded concurrently with the base extraction so it adds no extra latency:

```sh
# Restore phase (single invocation)
gradle-cache restore     --bucket <bucket> --cache-key <key> --ref main --branch $BRANCH_NAME

# ... run the Gradle build ...

# Save phase
gradle-cache save-delta  --bucket <bucket> --cache-key <key> --branch $BRANCH_NAME
```

After the build, `save-delta` scans for files created since the restore marker and uploads a cumulative delta bundle keyed by branch name — so it survives rebases and force-pushes without any extra bookkeeping.

If you need to apply a delta separately (e.g. the base was already restored by another step), `restore-delta` is still available as a standalone subcommand.

### Credentials

S3 credentials are resolved via the standard AWS credential chain (environment variables, IRSA, instance profiles, etc.).

## License

MIT
