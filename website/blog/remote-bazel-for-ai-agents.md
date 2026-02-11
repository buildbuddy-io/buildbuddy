---
slug: remote-bazel-for-ai-agents
title: "Teaching AI Coding Agents to Use Remote Bazel"
description: "A practical guide with a skills-file excerpt and a sample MCP server."
authors: maggie
date: 2026-02-11 12:00:00
tags: [ai, workflows, remote-bazel]
---

In our [Snapshot, Chunk, Clone](/blog/fast-runners-at-scale) post, we talked about using Remote Bazel with AI coding agents. This post is the practical follow-up: copy-paste examples you can use today.

We will cover two integration patterns:

1. A **skills file excerpt** (fastest to adopt).
2. A **small MCP server** (better control and reusability).

<!-- truncate -->

## Remote Bazel API recap

At the core, both approaches call the same endpoint:

```bash
curl --data '{
  "repo": "git@github.com:buildbuddy-io/buildbuddy.git",
  "branch": "master",
  "steps": [{"run": "bazel build //server/util/flagutil:flagutil"}]
}' \
--header 'x-buildbuddy-api-key: XXX' \
--header 'Content-Type: application/json' \
https://app.buildbuddy.io/api/v1/Run
```

For production usage, we recommend:

- Storing the API key in an env var instead of hardcoding it.
- Setting `"wait_until": "COMPLETED"` when you want a synchronous success/failure signal.
- Returning the invocation link to make logs and results easy to inspect.

## Option 1: Skills file excerpt

If your agent platform supports skills files, this is usually the fastest path. You describe when the agent should offload work, provide the command template, and add guardrails.

Sample excerpt:

```text
# File: .claude/skills/remote-bazel.md

# Skill: Remote Bazel execution on BuildBuddy

Use this skill when:
- The user asks to run `bazel build`, `bazel test`, or `bazel query`.
- The command is likely expensive for a local agent runner.

Prerequisites:
- `BUILDBUDDY_API_KEY` is available in environment variables.
- The repo is reachable by BuildBuddy (for private repos, ensure repo access is configured).

Procedure:
1) Build a single Bazel command string (must start with `bazel `).
2) Submit the command to BuildBuddy Remote Bazel with curl:

curl --fail --silent --show-error --data '{
  "repo": "git@github.com:buildbuddy-io/buildbuddy.git",
  "branch": "master",
  "steps": [{"run": "bazel build //server/util/flagutil:flagutil"}],
  "wait_until": "COMPLETED"
}' \
--header "x-buildbuddy-api-key: ${BUILDBUDDY_API_KEY}" \
--header "Content-Type: application/json" \
https://app.buildbuddy.io/api/v1/Run

3) Parse `invocation_id` from the response and share:
   https://app.buildbuddy.io/invocation/<invocation_id>

Safety rules:
- Only run commands that start with `bazel `.
- Never print or persist the API key.
- Ask for confirmation before destructive Bazel commands (for example, `bazel clean --expunge`).
```

### How to use this skills approach

1. Save the excerpt in your agent's skills directory.
2. Replace repo / branch defaults with your own repository settings.
3. Export `BUILDBUDDY_API_KEY` in the agent runtime.
4. Prompt the agent normally, for example:  
   _"Run bazel test //server/... and summarize failures."_

This approach is simple and effective, but every prompt has to rely on instruction-following. For stricter control and a better developer UX, use an MCP server.

## Option 2: Sample MCP server

An MCP server wraps Remote Bazel behind explicit tools. That gives agents a structured interface and gives you a single place for policy and authentication.

Below is a minimal TypeScript server with two tools:

- `remote_bazel_run` - starts a Remote Bazel run.
- `remote_bazel_get_invocation` - fetches invocation metadata.

```typescript
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const BUILDBUDDY_URL = process.env.BUILDBUDDY_URL ?? "https://app.buildbuddy.io";
const BUILDBUDDY_API_KEY = process.env.BUILDBUDDY_API_KEY;

if (!BUILDBUDDY_API_KEY) {
  throw new Error("Missing BUILDBUDDY_API_KEY");
}

async function postJson(path: string, body: unknown) {
  const res = await fetch(`${BUILDBUDDY_URL}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-buildbuddy-api-key": BUILDBUDDY_API_KEY,
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`${path} failed: ${res.status} ${await res.text()}`);
  }
  return res.json();
}

const server = new McpServer({ name: "remote-bazel", version: "0.1.0" });

server.tool(
  "remote_bazel_run",
  {
    repo: z.string().describe("Git URL, e.g. git@github.com:org/repo.git"),
    branch: z.string().default("master"),
    bazel_command: z.string().describe('Full bazel command, e.g. "bazel test //..."'),
    wait_until: z.enum(["QUEUED", "STARTED", "COMPLETED"]).default("COMPLETED"),
    timeout: z.string().optional(),
  },
  async ({ repo, branch, bazel_command, wait_until, timeout }) => {
    const trimmed = bazel_command.trim();
    if (!trimmed.startsWith("bazel ")) {
      throw new Error("Only bazel commands are allowed.");
    }

    const requestBody: Record<string, unknown> = {
      repo,
      branch,
      steps: [{ run: trimmed }],
      wait_until,
    };
    if (timeout) requestBody.timeout = timeout;

    const response = await postJson("/api/v1/Run", requestBody);
    const invocationId = response.invocation_id;
    if (!invocationId) {
      throw new Error(`Run response missing invocation_id: ${JSON.stringify(response)}`);
    }

    return {
      content: [
        {
          type: "text",
          text: [
            `Invocation ID: ${invocationId}`,
            `Invocation URL: ${BUILDBUDDY_URL}/invocation/${invocationId}`,
          ].join("\n"),
        },
      ],
    };
  }
);

server.tool(
  "remote_bazel_get_invocation",
  { invocation_id: z.string() },
  async ({ invocation_id }) => {
    const response = await postJson("/api/v1/GetInvocation", {
      selector: { invocation_id },
    });

    return {
      content: [{ type: "text", text: JSON.stringify(response, null, 2) }],
    };
  }
);

const transport = new StdioServerTransport();
await server.connect(transport);
```

### How to use this MCP approach

1. Create a small Node project for the server:

   ```bash
   npm init -y
   npm install @modelcontextprotocol/sdk zod
   ```

2. Save the server as `remote-bazel-mcp.ts` and run it via your MCP host (or transpile to JS first).

3. Pass environment variables in your MCP server config:
   - `BUILDBUDDY_API_KEY`
   - optional `BUILDBUDDY_URL` (defaults to `https://app.buildbuddy.io`)

4. In your agent prompt, call the tool with structured inputs (repo, branch, bazel command).

Compared to raw prompt instructions, MCP tools are easier to audit, easier to reuse across agents, and simpler to harden over time.

## Which one should you pick?

- **Skills file first** if you want the quickest possible rollout.
- **MCP server first** if you need central guardrails, tool-level observability, and portability across agent clients.

Many teams start with a skills file, prove value quickly, and then migrate to MCP for cleaner long-term operations.

## Hardening checklist (recommended for both)

- Use least-privilege API keys.
- Restrict allowed command prefixes (for example: `bazel build`, `bazel test`, `bazel query`).
- Add command timeout defaults.
- Return invocation links in every response.
- Log tool inputs and invocation IDs for auditability (never log secrets).

If you end up building on this pattern, we would love to hear what worked well and what you want to see next.
