# How BuildBuddy Handles Bazel Command Lines

This note traces how a Bazel command typed by an engineer flows through BuildBuddy and ends up on the **explicit** and **effective** command-line panels of the invocation details page.

## 1. What Bazel Emits

Regardless of whether developers invoke `bazel`, `bazelisk`, or the `bb` wrapper, Bazel itself is responsible for the command-line information that appears in the Build Event Protocol. After the user’s shell finishes parsing, Bazel streams these events:

- `OptionsParsed.cmd_line` – Bazel’s _effective_ command line (startup flags + rc expansions + overrides).
- `OptionsParsed.explicit_cmd_line` – The tokenized args the client passed on the command line after shell parsing. Quotes are gone, but tokens that included spaces remain whole.
- Structured command line events – carry the executable name and any “residual” args.

These events, along with other metadata, are streamed to BuildBuddy over gRPC as part of the Publish Build Tool Event stream.

## 2. Additional Metadata from the `bb` CLI

When developers use the BuildBuddy CLI (`bb`) or the CI runner, the wrapper augments Bazel’s own data:

- Right before Bazelisk starts, the CLI captures `os.Args`, serializes them to JSON, and appends `--build_metadata=EXPLICIT_COMMAND_LINE=<json>`.
- Additional default metadata (repo URL, branch, etc.) is added unless a workspace status script already supplies it.
- The CLI-provided `EXPLICIT_COMMAND_LINE` build metadata lets wrappers preserve the user-visible argv even after they mutate the Bazel command.

## 3. Server Ingestion and Redaction

- The build event handler service reads each BEP event and runs it through the streaming redactor before anything is persisted.
- The redactor strips secrets (API keys, remote headers, URL credentials, disallowed `--client_env`) and removes the sentinel `--build_metadata=EXPLICIT_COMMAND_LINE=…` flag so only user-visible args remain.
- The accumulator component keeps track of the most recent `OptionsParsed` payload and other metadata while the invocation is streamed.
- When the invocation is finalized, sanitized events are written to blob storage and the invocation row is updated. Any subsequent call to the build event server’s `GetInvocation` entry point re-runs redaction if the stored stream predates standard redactions.

> Tests in the build event handler package construct synthetic BEP sequences to assert that headers, env vars, and metadata are redacted end-to-end.

## 4. Serving the Invocation to the Web App

- The invocation details page requests the invocation service’s `GetInvocation` RPC, which returns the invocation proto plus its top-level events.
- The `InvocationModel` client component walks those events, memoizing:
  - The most recent `OptionsParsed` message.
  - The `EXPLICIT_COMMAND_LINE` override (if any).
  - Structured command-line and residual args for display helpers.

## 5. Rendering Explicit vs. Effective Command Lines

`InvocationModel` assembles display strings via its `commandLineOptionsToShellCommand` helper: executable + sub-command from the structured command line, the chosen options list (`explicit_cmd_line` or `cmd_line`), then any residual args prefixed with `--` when present. Each token is re-quoted for shell safety with the shared shlex utility.

If `EXPLICIT_COMMAND_LINE` metadata exists, it takes precedence over `OptionsParsed.explicit_cmd_line` so that wrapper-added flags do not surprise users. The effective command line always reflects Bazel’s canonical argv.

Both strings are displayed alongside copy buttons in the invocation details card. Because redaction happens on the server, the UI never sees secrets that should be hidden.

## 6. Summary

- **Emit (Bazel)** – `OptionsParsed` and structured command-line events describe both explicit and effective argv lists once the shell has tokenized them.
- **Augment (bb)** – When present, the BuildBuddy CLI records argv and stores a JSON copy in build metadata.
- **Redact** – Server removes secrets and redundant metadata before persisting events.
- **Model** – Web client hydrates an `InvocationModel` from the sanitized events.
- **Render** – UI reconstructs shell-safe strings for explicit and effective command lines.

This flow ensures the invocation page shows exactly what the user ran (explicit) and what Bazel executed after rc expansion (effective) while keeping sensitive data out of the UI and stored logs.
