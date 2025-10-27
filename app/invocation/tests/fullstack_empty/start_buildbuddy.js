const { mkdtempSync, mkdirSync, writeFileSync, rmSync } = require("fs");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const { HTTP_PORT } = require("./test_environment");

function posixPath(p) {
  return p.replace(/\\/g, "/");
}

function main() {
  const tmpRoot = mkdtempSync(path.join(os.tmpdir(), "buildbuddy-playwright-"));
  const casDir = path.join(tmpRoot, "cas");
  mkdirSync(casDir, { recursive: true });
  const dbPath = path.join(tmpRoot, "buildbuddy.db");
  const configPath = path.join(tmpRoot, "buildbuddy.yaml");

  const configYaml = `
app:
  env: development
database:
  data_source: sqlite3:///${posixPath(dbPath)}
storage:
  type: disk
  disk:
    root_directory: ${posixPath(casDir)}
remote_execution:
  enable_remote_cache: true
`.trimStart();

  writeFileSync(configPath, configYaml, { encoding: "utf8" });

  const runfilesRoot = path.resolve(__dirname, "..", "..", "..", "..");
  const buildbuddyBin = path.join(runfilesRoot, "server/cmd/buildbuddy/buildbuddy_/buildbuddy");
  const child = spawn(
    buildbuddyBin,
    [
      `--config_file=${configPath}`,
      `--port=${HTTP_PORT}`,
      "--ssl_port=0",
      "--monitoring_port=0",
      "--grpc_port=0",
      "--app_directory=app",
    ],
    {
      stdio: "inherit",
    }
  );

  const cleanup = () => {
    try {
      rmSync(tmpRoot, { recursive: true, force: true });
    } catch (_err) {
      // ignore cleanup errors
    }
  };

  const handleSignal = (signal) => {
    if (!child.killed) {
      child.kill(signal);
    }
  };

  process.on("SIGINT", handleSignal);
  process.on("SIGTERM", handleSignal);
  process.on("exit", cleanup);

  child.on("exit", (code, signal) => {
    cleanup();
    if (signal) {
      process.kill(process.pid, signal);
    } else {
      process.exitCode = code ?? 0;
    }
  });
}

main();
