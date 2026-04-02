const fs = require("fs");
const path = require("path");
const core = require("@actions/core");
const exec = require("@actions/exec");
const {
  backendArgs,
  commonArgs,
  gradleHomeArgs,
  includedBuildArgs,
  gitDirArgs,
  execOptions,
  resolveBranch,
} = require("./helpers");

async function run() {
  try {
    if (core.getInput("save") === "false") {
      core.info("Cache save skipped (save: false)");
      return;
    }

    const branch = resolveBranch();

    if (branch) {
      // On cold-start (no base cache found), there's no restore marker so
      // save-delta would fail. Detect this and skip gracefully.
      const gradleHome = core.getInput("gradle-user-home") || "~/.gradle";
      const marker = path.resolve(gradleHome, ".cache-restore-marker");
      if (!fs.existsSync(marker)) {
        core.info("No restore marker found (cold-start) — skipping delta save");
        return;
      }

      const args = [
        "save-delta",
        ...commonArgs(),
        ...backendArgs(),
        ...gradleHomeArgs(),
        "--branch",
        branch,
      ];
      await exec.exec("gradle-cache", args, execOptions());
    } else {
      const args = [
        "save",
        ...commonArgs(),
        ...backendArgs(),
        ...gradleHomeArgs(),
        ...includedBuildArgs(),
        ...gitDirArgs(),
      ];
      await exec.exec("gradle-cache", args, execOptions());
    }
  } catch (error) {
    core.setFailed(`Cache save failed: ${error.message}`);
  }
}

run();
