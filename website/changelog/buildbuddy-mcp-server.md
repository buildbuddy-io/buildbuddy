---
title: "BuildBuddy MCP server"
date: 2026-04-15T10:00:00
authors: brandon
---

BuildBuddy now exposes an MCP endpoint for coding agents and other
MCP-capable clients.

MCP provides a way to let agents explore BuildBuddy data for mostly
read-only workflows like build and test metadata, logs, artifacts, target
statuses, and more.

You can connect agents like Codex, Claude Code, and Gemini CLI to:

```text
https://YOUR-BUILDBUDDY-ORGANIZATION.buildbuddy.io/mcp
```

Authenticate with:

```text
Authorization: Bearer <api-key>
```

See the new
[BuildBuddy MCP Server docs](https://www.buildbuddy.io/docs/enterprise-mcp)
for setup instructions and configuration examples.
