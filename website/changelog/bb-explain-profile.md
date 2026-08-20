---
title: "Analyze slow builds with bb explain profile"
date: 2026-08-12T10:00:00
authors: maggie
tags: [AI, debugging]
---

You can now use the `bb` CLI to analyze a Bazel timing profile with an AI coding agent using `bb explain profile`.

The command downloads the timing profile for an invocation and returns a detailed report with actionable recommendations for speeding up the build.

The command accepts an invocation ID or URL:

```bash
bb explain profile <INVOCATION_ID>
bb explain profile https://app.buildbuddy.io/invocation/<INVOCATION_ID>
```

The command supports interpreting the timing profile with `--agent=codex` or `--agent=claude`. If available, it uses your
locally authenticated Claude Code or Codex subscription. Otherwise you can set the environment variable `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` to authorize API requests. You can also specify `--model` and `--effort` to control the agent's behavior.

```bash
bb explain profile --agent=codex --model=gpt-5.4 --effort=high <INVOCATION_ID>
```

See the [`bb explain profile` documentation](/docs/cli-commands#bb-explain-profile) for more details.
