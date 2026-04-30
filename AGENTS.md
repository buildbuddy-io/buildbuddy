# BuildBuddy Coding Agent Guidelines

## Build and Test
- Use `./buildfix.sh` to format code
- Use `bazel build` and `bazel test` to build and test the code
- To run a specific test in a file with Bazel, use `bazel test --test_filter=<function_name/subtest_name> //specific/package:target`
- Use `--test_output=errors` to reduce the Bazel command verbosity
- Use `--config=remote-minimal` to run the builds and tests remotely. Only compatible with Linux-specific targets
- Use `genhtml` to explore Bazel coverage outputs
- Use `bazel run //:gazelle` to update Bazel BUILD files

## Code Style

### Bazel
- Never read or modify the `user.bazelrc` file.
- Prefer running builds and tests remotely, only fallback to local execution if there is an authentication issue.

### Proto
- Keep all proto files in the `proto/` directory.
- Vendor 3rd party proto files and update their imports as well as go_package option accordingly.
- `proto/BUILD` file should be updated manually as Gazelle was configured to ignore it. Maintain the sorting order in the BUILD file.

### Golang
- Avoid creating multiple files for a single package unless necessary. Acceptable cases are OS/Arch specific code
- Create an alias for proto package imports. For example `import inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"`
- Prefer table-driven tests with descriptive names
- To add new dependencies, first use `go get` to update `go.mod` and `go.sum`, then run `./tools/fix_go_deps.sh` to update `./deps.bzl`.

### React/Typescript
- New dependencies should be added using `pnpm`.
- Run dev server using `bazel run server`. The relevant config flags can be set in `./config/buildbuddy.local.yaml`.
- Run enterprise-version dev server using `bazel run enterprise/server`. The relevant config flags can be set in `./enterprise/config/buildbuddy.local.yaml`. Enterprise server depends on a Redis-server instance to run. Redis could be started with `docker run -d -p6379:6379 redis` or `redis-server` if installed locally.
- Additional flags could be given to the server instance via commandline arguments. For example, `bazel run enterprise/server -- --cache.detailed_stats_enabled=true` will enable detailed cache stats UI in the dev server.
- Once started, the dev server Web UI can be accessed at `http://localhost:8080` and the grpc endpoint is available at `grpc://localhost:1985`. Generate a UUID with `export invocation_id=$(uuidgen | tr '[:upper:]' '[:lower:]') && echo invocation_id`, then do `bazel build --invocation_id=$invocation_id --config=local //cli` to start a build targeting the local server and create test data. The invocation will then be available in the Web UI under `http://localhost:8080/invocation/$invocation_id`.

## Documentation
- Make sure the relevant files in './docs/' are up to date.
