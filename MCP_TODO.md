MCP TODOs:

Answering "why are my builds slow" would be much easier / more efficient
if we did the following things:

(Highest priority)

- [ ] Testing - run some real sessions w/ codex-ephemeral and debug cases where the agent
      doesn't use the tools effectively.
      To inspect the latest session:
      `bash
  $ latest ~/.codex-ephemeral/ -a | xargs printf '%s/sessions' | xargs -I{} find {} -name '\*.jsonl' | xargs cat | codex-session-viewer --text | cat -n
  `

(Higher priority)

- [ ] Remove "At least one search atom must be set" requirement from
      SearchInvocations. The GroupID can be set in order to make this
      error go away, but this doesn't make much sense because we already
      should have the group ID from auth info.
- [ ] Add a 'parent_workflow_name' column to Invocations, to make it
      easier to fetch all bazel invocations for a BB workflow. Currently, the
      agent has to do a bunch of guesswork to figure out if there is an
      appropriate filter that it can use.
- [ ] Add 'jobs' count to invocations table (either the effective --jobs
      flag or the host CPU count)
- [ ] Add 'timing_profile_uri' as a field to Invocations, to make it easier
      to do batch timing profile analysis (i.e. to avoid fetching the full BES
      stream for the Invocations being analyzed).
- [ ] Expose 'effective_isolation_type' in executions response, so that
      agents have access to isolation-type specific guidance (e.g. for
      `bare` isolation, CPU/IO information is less likely to be accurate.)

(Lower priority)

- [ ] Move the Executions data directly into the GetTarget API response so
      that we don't have to do multiple parallel API requests (one for
      GetTarget and one for GetExecution).
- [ ] (UI) Add a separate "Workflow action" field which is just an alias
      for pattern. In the future maybe add a separate explicit
      workflow_action_name column in the invocation DB
