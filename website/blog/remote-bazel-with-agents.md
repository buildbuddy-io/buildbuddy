---
slug: remote-bazel-with-agents
title: "Remote Bazel: The missing piece for AI coding agents"
authors: maggie, george
date: 2026-03-02 12:00:00
tags: [performance, infrastructure, AI]
---

AI coding agents like Claude, Cursor, and Codex are transforming how developers write software. For many developers coding with agents, the bottleneck has shifted from writing code to validating code written by agents - with builds and tests.

Remote Bazel ensures these builds and tests run quickly by running them on powerful remote runners. These runners are colocated with cache and RBE servers, reducing network latency. They can be cloned and reused for future builds, ensuring a warm analysis cache. All the local machine needs to do is stream back the logs.

It's like giving your AI a build farm.

<!-- truncate -->

## Looking for more?

This blog post is one of a series of related posts about using Remote Bazel with coding agents.

For tactical instructions on how to configure your agent to use Remote Bazel, see our [blog post on agent configuration](https://www.buildbuddy.io/blog/configuring-remote-bazel-with-agents).

For more details on the technical setup of our remote runners, see our [blog post on our Firecracker cloning architecture](https://www.buildbuddy.io/blog/fast-runners-at-scale).

## Why bazel is a bad fit for coding agents

Typically, coding agents run builds either on developers' local machines or in lightweight cloud VMs offered by AI providers. This is often a bad fit for Bazel, which is notoriously resource intensive.

When a coding agent runs `bazel test //...` on a developer's laptop or a lightweight cloud dev box, several things can go wrong:

1. **Network latency**: Network latency is often the biggest bottleneck in many Bazel Remote Build Execution and Remote Caching setups. Network round trips can quickly add up and hamper build times.
1. **Resource contention**: When running on local machines, developers may run multiple agents in parallel, or they themselves may be developing in tandem with agents. The cloud runners provided by most AI providers are typically resource constrained and have limited network bandwidth. Resource contention can quickly become a bottleneck, slowing down builds.
1. **Analysis cache thrash**: When multiple agents are running builds in parallel, the analysis cache can be thrown out if build options change. AI cloud runners are often ephemeral and scoped to each task, and are reset to a clean state between tasks. This means they have to restart the analysis phase from scratch each time, which can take several minutes.
1. **Architecture limitations**: Agents typically run builds on the same architecture as the machine they are running on. This can make it challenging to run tests on different architectures. For example, developers on Macs may want to run Linux-only tests.

Remote Bazel eliminates each of these:

1. **Fast network**: The runners are in the same datacenter as the remote cache servers and executors (sub-millisecond RTT), minimizing network latency.
1. **Powerful runners**: Runners can be easily configured to have significant CPU, memory and up to 100GB of disk. Parallel builds can each run on their own runner, avoiding resource contention.
1. **Warm Bazel instances**: Runners are snapshotted and cloned after each build, ensuring a warm analysis cache for future builds. Builds with different options can be configured to use different runners, avoiding analysis cache thrash.
1. **Cross-platform development**: Runners can be easily configured to run on different architectures, operating systems, and with different container images. One use case could be simulating the environment used in CI, to debug a test that fails in CI but passes locally.

## Using Remote Bazel

You can run any bash command on BuildBuddy remote runners with Remote Bazel. You can get started for free by creating a BuildBuddy account [here](https://www.buildbuddy.io/) and generating an API key. Remote runs can be triggered via curl request or by using our CLI and the `bb remote` command.

See our [Remote Bazel docs](https://www.buildbuddy.io/docs/remote-bazel/) for more details. They include instructions for how to configure secrets, timeouts, and other advanced features.

### Using the CLI

When using our CLI, `bb remote` supports some additional features out-of-the-box. It automatically:

1. Checks out your GitHub repo on the remote runner. It will even mirror your local git state, including uncommitted changes, so the remote runner sees the exact same working tree as your local machine.
2. Streams logs back to your terminal in real time.
3. Build outputs can be automatically fetched locally, so the agent sees the same output it would from a local build.
4. The VM is snapshotted after completion so future runs start warm.

`bb remote` is a drop-in replacement for `bazel`:

- Build: `bb remote build //path/to:target`
- Test: `bb remote test //path/to:target`
- Run: `bb remote run //path/to:target`

Any bash commands can be run on the remote runner with the `--script` flag.

```bash
bb remote --script='
./setup.sh
bazel run :gazelle
bazel test //path/to:target
echo "ALL DONE!"
'
```

### Making API requests via CURL

If installing the CLI isn't practical, agents can also initiate remote runs via CURL requests.

```bash
curl -d '{
    "repo": "git@github.com:your-org/your-repo.git",
    "branch": "main",
    "steps": [{"run": "bazel test //src/..."}]
}' \
-H "x-buildbuddy-api-key: YOUR_API_KEY" \
-H 'Content-Type: application/json' \
https://app.buildbuddy.io/api/v1/Run
```

A GitHub repo is not required to use the API, but if set, the remote runner will fetch the configured repository and reference and run the commands within the GitHub workspace.

The Run API returns an invocation ID. Agents can poll `GetInvocation` to check completion and `GetLog` to fetch logs.

### Cross-platform development

Another benefit of Remote Bazel is that it can be used to easily run builds on different platforms, regardless of what machine the agent is running on.

```bash
# Test on Linux AMD64 from a Mac
bb remote --os=linux --arch=amd64 test //...

# Use a specific container image
bb remote --container_image=docker://gcr.io/my-project/my-image:latest test //...
```

### Running from a different runner

For whatever reason, you may want to run a build on a different runner. One example is if you want to run a build with a different Bazel startup option, but don't want the analysis cache to be discarded on a warm runner you're using for other builds.

You can do this by setting a new execution property with the `--runner_exec_properties` flag with the CLI or the `platform_properties` field in the API request.

We take a hash of all execution properties and use that in the snapshot key. This means that if you change an execution property, you will get a new snapshot and runner. This can be used to get independent runners for different types of builds.

```bash
# These builds will run on different runners due to the different execution properties
bb remote test //...
bb remote --runner_exec_properties=runner-key=my-other-runner test //...
```

### Putting it together: an example agent workflow

Here's what a sample AI coding agent workflow might look like with Remote Bazel:

1. **User gives an agent a task**: "Add support for creating new user accounts with Google OAuth"
2. **Agent reads the associated code**: The files are available locally on the developer's local Linux machine
3. **Agent makes code changes**: Edits are made to local files
4. **Agent validates the new feature by running tests**: `bb remote test //src/auth:login_test`
5. **`bb remote` mirrors the local git state**: It uploads the local diffs to the remote runner, so the remote workspace contains the agent's local changes
6. **Remote runner runs the tests**: The remote runner resumes from a warm VM snapshot and runs the tests, streaming the output back to the agent. Nothing is running locally on the developer's machine. By default, the remote runner matches the local machine's platform, so the tests run on Linux
7. **Agent reads streamed output**: The agent sees the test results, and finds an error
8. **Agent makes another edit**: The agent applies a fix to the local code to fix the test failure
9. **Agent runs the test again**: The agent runs `bb remote test //src/auth:login_test` again. This command resumes from the warm VM snapshot from the first `bb remote` command, and runs much faster because the workspace is warm
10. **Agent reports success**: The agent sees that the tests passed and reports success to the user
11. **Agent runs the tests on Mac**: As an extra validation step, the agent runs the tests on Mac to ensure the feature works on all platforms with `bb remote --os=darwin --arch=amd64 test //src/auth:login_test`
12. **Agent runs the tests with the race detector enabled**: `bb remote --runner_exec_properties=runner-key=race test //src/auth:login_test --@io_bazel_rules_go//go/config:race`. Because the command is configured with a different execution property, a different runner will be used. This prevents the build from discarding the analysis cache from the first run, which it would typically do with the race flag. If the agent ran another build without `runner-key` set, it would start from the warm runner from step #9 with a warm analysis cache

In this example, the agent never needs to even install Bazel. It just edits local files and triggers remote runs, using limited resources on the local machine. All the heavy lifting happens on snapshotted remote runners.

### Tips

When looking for a warm snapshot, we try to find a runner on the same git branch to optimize performance. If no runner for that branch exists, we fall back to a runner on the repository's default branch (i.e. `main` or `master`). We recommend regularly running Remote Bazel on the default branch to ensure that a fresh snapshot is regularly generated and available as a starting point for builds on other branches.

## Conclusion

Remote Bazel lets AI coding agents run fast, warm, well-resourced Bazel builds without any local overhead. The combination of VM snapshots, cache colocation, and automatic git mirroring creates a tight feedback loop that's ideal for the edit-test-iterate pattern that coding agents rely on.

The setup is minimal: (1) install the CLI, (2) set an API key, (3) replace `bazel` with `bb remote`. The performance difference can be substantial, especially on repeated builds in the same repo. That speed difference can meaningfully change what an agent can accomplish in a single session.

We're building more features to support coding agents and would love to hear from our users. If you're facing
challenges with setup, have anecdotes about how you're using Remote Bazel with agents, or feature requests, please reach out. You can find us on our open source Slack channel or you can contact me by email at maggie@buildbuddy.io.
