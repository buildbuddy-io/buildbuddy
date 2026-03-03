---
slug: configuring-remote-bazel-with-agents
title: "Configuring Remote Bazel with agents"
authors: maggie
date: 2026-03-02 12:00:00
tags: [performance, infrastructure, AI]
---

Configuring AI coding agents to use Remote Bazel can unlock significant performance improvements for builds and tests. With more compute, a warm analysis cache, and fast network access
to co-located remote caches, our remote runners are optimized to ensure agents' time is well spent on logic and reasoning, instead of waiting for builds to complete.

For more details on what Remote Bazel is and some sample use cases, see our [blog post on Remote Bazel](https://www.buildbuddy.io/blog/remote-bazel-with-agents). For more details on the technical setup
of our remote runners, see this [blog post](https://www.buildbuddy.io/blog/fast-runners-at-scale).

<!-- truncate -->

## Skills file

A [skills file](https://agentskills.io/home) (SKILL.md) contains instructions that AI agents can use to complete tasks more effectively. Many popular coding agents - like OpenAI's Codex, Anthropic's Claude, GitHub's
Copilot, and Cursor - support skills files.

Skills can be invoked directly, via the name of the skill configured in the markdown file. However skills are often most useful when the agent invokes them dynamically,
when they're determined relevant to the task at hand.

Each skill should have a directory containing a SKILL.md file. These skills directories can live in a specific repository if they should only apply to a specific project. Or they can
live under the user's home directory if they should apply globally.

Each AI coding tool expects skills files to live at different places:

- Codex: `.codex/skills/<SKILL_NAME>/SKILL.md`
- Claude: `.claude/skills/<SKILL_NAME>/SKILL.md`
- Copilot: `.github/skills/<SKILL_NAME>/SKILL.md`
- Cursor: `.cursor/skills/<SKILL_NAME>/SKILL.md`
- etc.

To configure your agents to use Remote Bazel for builds, you can create a `remote-run` skill. You can copy and paste one of our [sample skills files](https://github.com/buildbuddy-io/buildbuddy/tree/master/docs/agent_skills) to get started.

## Using Remote Bazel with agents

There are 2 ways to use Remote Bazel with agents: using the BB CLI and with a curl request. With both methods, a snapshot of the runner will be taken after execution completion,
so future runs start on a warm worker.

The BB CLI [must be installed](https://www.buildbuddy.io/cli/), but supports some additional features out-of-the-box:

- Streams output logs to the terminal in real time
- Mirrors your local git state to the remote runner, including uncommitted changes
- Automatically sets some environment variables and flags to optimize the remote run
- The exit code of the remote execution is reflected locally

If you prefer to use a curl request to trigger the remote run, output logs and execution status can be fetched with additional APIs. Note that when using this method, your local git state will not be mirrored automatically to the remote runner.

Our sample skills files are checked in to our repo and may be updated from time-to-time. You can view them [here](https://github.com/buildbuddy-io/buildbuddy/tree/master/docs/agent_skills).

## Conclusion

AI coding agents accelerate developers. At BuildBuddy, we hope to accelerate the agents themselves.

We're just beginning to build more features in support of coding agents and would love to hear from our users. If you're facing
challenges with setup, have anecdotes about how you're using Remote Bazel with agents, or feature requests, please reach out. You can find us on our [open source Slack channel](http://community.buildbuddy.io/) or email me at maggie@buildbuddy.io.
