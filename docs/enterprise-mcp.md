---
id: enterprise-mcp
title: BuildBuddy MCP server
sidebar_label: Enterprise MCP
---

BuildBuddy's MCP server provides a suite of tools that enable agents to
explore build and test metadata, logs, artifacts, target statuses, and
more.

MCP enables powerful agentic workflows without requiring agents to
hand-write BuildBuddy API requests. For example, a local coding agent can
use BuildBuddy's MCP tools to check failing CI tests on BuildBuddy and
then fix issues automatically, or drill into performance issues. Coding
agents can also be triggered as part of CI, using BuildBuddy's
observability tools to quickly produce actionable diagnostics and
insights.

## Available MCP tools

The MCP server exposes tools for inspecting build metadata, test logs,
artifacts, and more, from a single BuildBuddy org.

Most BuildBuddy APIs are also available as MCP tools. For the underlying
API reference or field-level details, see the
[Enterprise API docs](enterprise-api.md).

The available tools are subject to change. It's strongly recommended not
to write agent instructions (`AGENTS.md`, skills, etc.) referencing
specific MCP tool names. Instead, give agents generic instructions and let
them explore build data using the currently available tools.

## Endpoint

Cloud BuildBuddy MCP servers are available at:

```text
https://<org>.buildbuddy.io/mcp
```

Replace `<org>` with your organization's BuildBuddy slug.

If you're running BuildBuddy on-prem, use your own BuildBuddy base URL instead:

```text
https://<your-buildbuddy-host>/mcp
```

## Authentication

Authenticate with a BuildBuddy API key.

Most MCP clients are easiest to configure with this header:

```text
Authorization: Bearer <api-key>
```

You can create API keys on your [organization settings page](https://app.buildbuddy.io/settings/org/api-keys). For more details, see the [Authentication Guide](guide-auth.md).

### Auth best practices

If you want the strongest guarantee that a BuildBuddy API key is not
exposed to an agent, run an MCP relay server that injects the
`Authorization: Bearer <api-key>` header on outbound requests, then point
the agent at that relay. Ideally, run the relay in an isolated environment
that the agent cannot inspect directly.

For the API key, a good pattern is to use a dedicated BuildBuddy user with
the `Reader` role and a user-owned API key, if your org has user-owned
keys enabled. This ensures that the narrowest possible scope for the key.

## Configuration

See the relevant MCP configuration instructions below for your agent.

- Claude: https://code.claude.com/docs/en/mcp
- Codex: https://developers.openai.com/codex/mcp
- Cursor: https://cursor.com/docs/mcp
- Gemini: https://geminicli.com/docs/tools/mcp-server/
- GitHub Copilot: https://docs.github.com/en/copilot/concepts/context/mcp
- OpenCode: https://opencode.ai/docs/mcp-servers/

Many other coding agents support MCP as well - prompting the agent itself
for MCP setup will often yield usable instructions or references.

## Self-hosted setup

For self-hosted BuildBuddy, enable both the public API and MCP endpoint:

```yaml title="config.yaml"
api:
  enable_api: true
  enable_mcp: true
```

Then point your MCP client at:

```text
https://<your-buildbuddy-host>/mcp
```
