# Milestones

(Preserve these milestones! Use `x` marks to indicate completion when done)

- [x] Add proxy scaffolding
- [x] Implement handleInfoRefs
- [x] Implement handleUploadPack
- [x] Exhaustive testing for handleInfoRefs
- [x] Exhaustive testing for handleUploadPack
- [x] Make the basic pull-through test pass
      Update it for the hashed filesystem layout.
- [x] Implement request forwarding (proxy all other requests)
- [ ] Implement repository maintenance and eviction
      Run maintenance through bounded workers and evict idle repositories without racing active requests.
- [ ] Add prometheus metrics: git process CPU/memory usage, bytes sent/received
      from upstream (by TLD plus one), bytes sent/received to clients (by TLD
      plus one).
- [ ] Add Grafana dashboard
- [ ] Wire up to CI runner - rewrite GitHub https URLs to https://<proxy_url>
      Configure as experiment in the apps, which sets a ci_runner flag.
- [ ] Attach x-buildbuddy-api-key to requests from ci_runner; wire up remote
      authenticator and attach group_id to metrics. Fail unauthorized requests.
      Track git CPU usage by group_id.
- [ ] Assess performance; benchmark and optimize.
- [ ] Write dev k8s config
- [ ] Add alerts
- [ ] Enable experiment in dev
- [ ] Enable experiment in prod

# Tasks

(Delete tasks when done)

## P0 (required for MVP)

- [ ] Relay redirects from forwarded upstream requests
      Do not follow redirects in `Forward`; preserve the original method and body and return the upstream response to the client.
- [ ] Make canceled fetches leave the repository writable
      Cancel Git without abandoning ref lock files, and verify that a fetch succeeds after a client disconnects.
- [ ] Stop creating remote-tracking refs that do not exist upstream
      Replace the default origin refspec instead of appending to it while still synchronizing the upstream default branch.
- [ ] Preserve mirrors when startup validation cannot run
      Delete only conclusively invalid directories; transient Git launch or termination failures must not remove repository data.
- [ ] Fetch requested object IDs still absent after refreshing upstream refs
      CI may request a commit SHA after a force-push removes every ref to it.
      Fetching a SHA through a fork URL also depends on this: GitHub serves any
      object in the shared fork network by SHA, but a refs-only mirror of the
      fork never receives objects unreachable from the fork's own refs, so the
      mirror answers "not our ref" where GitHub would succeed. Fetch missing
      wants by ID and fail only if upstream rejects the same request.
- [ ] Finish sanitizing Git failures returned to clients
      Keep command output and local paths in server logs rather than HTTP error bodies.
- [ ] Run the pull-through test against an HTTPS upstream
      Exercise certificate trust and HTTPS credential paths instead of relying only on allowlisted localhost HTTP.
- [ ] Add a gitmirror URL flag to `ci_runner`
- [ ] Stop embedding credentials in rewritten repository URLs
      A catch-all HTTPS rewrite would otherwise place URL userinfo in the mirror path.
- [ ] Scope the credential helper to the appropriate mirrored repository
      Return `REPO_TOKEN` only for matching upstream repositories.
- [ ] Rewrite all Git HTTPS URLs through the mirror
      Configure global Git `insteadOf` rules so system Git and Bazel use the mirror.
- [ ] Exempt the mirror URL from the global rewrite
      The catch-all `https://` rule also matches direct mirror URLs. Add a longer identity rule for the mirror prefix so Git leaves them unchanged.
- [ ] Test CI checkout, `git_repository`, and submodule fetches
- [ ] Stop processing after any response write failure
      In particular, stop the discovery response even when its context has not yet been canceled.
- [ ] Test that SSH URLs remain outside the HTTPS mirror rewrite
      Continue using Git's SSH transport until the mirror has an SSH frontend.
- [ ] Fix `bb remote` resolving the base commit from a different remote
      When the selected remote lacks the current branch, getBaseBranchAndCommit
      falls back to `<defaultBranch>@{upstream}`, which may track another
      remote. This can pair a fork URL with an upstream-only commit, which only
      works because GitHub serves fork-network objects by SHA. Resolve the
      fallback commit from `<remoteName>/<defaultBranch>` instead.

# P3

- [ ] Configure allowed private upstream networks
      Keep private addresses denied by default while supporting explicitly permitted self-hosted providers.
- [ ] Preserve percent-encoded separators in recognized repository paths
      `RawRepository` currently uses decoded `URL.Path`, so `%2F` aliases `/`; narrow the preservation comment until this is fixed.
- [ ] Reject empty owner access tokens in `mockgitserver`
      An empty configured token currently authorizes Basic authentication with an empty password.
- [ ] P2P replication and/or affinity routing / consistent hashing?

# P4

- [ ] SSH frontend? (We would have to accept SSH connections.)
