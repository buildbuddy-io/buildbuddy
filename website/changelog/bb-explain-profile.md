---
title: "Analyze slow builds with bb agent analyze-profile"
date: 2026-08-12T10:00:00
authors: maggie
tags: [AI, debugging]
---

You can now use the `bb` CLI to analyze a Bazel timing profile with an AI coding agent using `bb agent analyze-profile`.

The command downloads the timing profile for an invocation and returns a detailed report with actionable recommendations for speeding up the build.

The command accepts an invocation ID or URL:

```bash
bb agent analyze-profile <INVOCATION_ID>
bb agent analyze-profile https://app.buildbuddy.io/invocation/<INVOCATION_ID>
```

The command supports interpreting the timing profile with `--agent=codex` or `--agent=claude`. If available, it uses your
locally authenticated Claude Code or Codex subscription. Otherwise you can set the environment variable `ANTHROPIC_API_KEY` or `CODEX_API_KEY` to authorize API requests. You can also specify `--model` and `--effort` to control the agent's behavior.

```bash
bb agent analyze-profile --agent=codex --model=gpt-5.4 --effort=high <INVOCATION_ID>
```

See the [`bb agent analyze-profile` documentation](/docs/cli-commands#bb-agent-analyze-profile) for more details.
