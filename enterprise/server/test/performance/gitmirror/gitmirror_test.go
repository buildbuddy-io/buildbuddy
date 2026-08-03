// Package gitmirror contains the performance / behavior test matrix for the
// git cache server ("gitmirror").
//
// # GOALS
//
// These benchmarks exist to compare different gitmirror implementation
// approaches against each other (and against a no-cache baseline) under
// realistic BuildBuddy client workloads. Every benchmark is written against
// the Implementation interface below, so a new candidate implementation only
// needs to provide a constructor to be run through the full matrix.
//
// Primary metrics (reported via b.ReportMetric on every benchmark):
//   - end-to-end fetch wall time (p50/p99 under concurrency)
//   - bytes fetched from the real upstream (egress saved is the headline win)
//   - number of requests hitting the real upstream (rate-limit pressure)
//   - bytes sent to clients (pack size efficiency; over-fetch detection)
//   - gitmirror shard CPU time (pack generation is the expensive part of
//     serving upload-pack from a bare mirror; pack-level caches trade this
//     for storage)
//   - cache hit rate (AC hits / total negotiations), where applicable
//
// MATRIX DIMENSIONS
//
//  1. Implementation under test (the comparison axis):
//     - baseline:    clients fetch directly from the upstream git server.
//     No gitmirror at all. Control group for all other rows.
//     - passthrough: gitmirror proxies to upstream without any caching.
//     Isolates proxy overhead from caching wins.
//     - packcache:   negotiation-keyed pack caching (ocicache-style):
//     AC key = hash(repo, wants, filter, depth, ...), pack bytes in CAS,
//     read-through on miss.
//     - baremirror:  gitmirror maintains local bare mirrors and serves
//     upload-pack itself (git-http-backend or go-git); mirrors freshened
//     from upstream.
//     - baremirror-cas: same, but mirror state is persisted to / hydrated
//     from CAS through the upstream cache proxy tier, so a restarted or
//     rescheduled shard does not re-clone from the real upstream.
//     - cdcpack (later experiment): serve NON-thin packs generated
//     deterministically (no intra-pack deltas via --window=0, zlib level
//     0, canonical object order) and store them CDC-chunked in CAS, with
//     zstd at the CAS/transport layer recovering compression. Trades
//     larger client downloads (cheap internal bandwidth; over-delivery
//     of objects the client already has is harmless) for upstream
//     egress, storage dedup across pack generations, and near-zero
//     pack-generation CPU. Requires quantizing the have-boundary (e.g.
//     one pack per push, old-tip..new-tip, plus periodic full packs) and
//     serving the smallest cached pack whose base is an ancestor of the
//     client's common base. Possible production shape: protocol v2
//     packfile-uris (small dynamic thin pack + URIs to cached pack
//     blobs; ci_runner controls client git config, so fetch.uriprotocols
//     can be enabled). Extra metrics for this row: client bytes
//     over-delivered vs. thin baseline, and CDC chunk dedup ratio
//     across successive packs of the same repo.
//
//  2. Topology: numClients x numShards x numCacheProxies, plus the client->
//     shard routing policy. Matrix points below intentionally include
//     multiple clients talking to multiple shards backed by zero, one, or
//     two upstream cache proxies:
//     - 1 client,  1 shard,  0 proxies   (functional floor / overhead)
//     - 8 clients, 1 shard,  1 proxy
//     - 8 clients, 3 shards, 1 proxy     (cross-shard duplication cost)
//     - 64 clients, 3 shards, 2 proxies  (herd + proxy fan-in)
//     Routing policies: repo-affinity (consistent hash of repo URL, the
//     expected production config) vs. random load-balancing (worst case for
//     mirror locality; measures how much each implementation depends on
//     affinity).
//
//  3. Client workload (modeled on real BuildBuddy clients; see ci_runner
//     main.go and cli/remotebazel):
//     - Workflows PR runs
//     - Workflows push-to-main runs
//     - bb remote / hosted runner runs (smart depth, full history, depth=N)
//     - Retries and multi-action fan-out (identical fetches, concurrently)
//     Each workload is crossed with the runner snapshot states below.
//
//  4. Repo shape:
//     - small   (~10MB, few refs)        : overhead-dominated
//     - medium  (~500MB, O(1k) refs)     : typical customer repo
//     - large   (~5GB monorepo, O(50k) refs, large binary blobs):
//     pack-generation and ref-advertisement costs dominate
//     - many-refs (small content, O(100k) refs): isolates info/refs
//     advertisement cost, which is paid even on a no-op fetch
//     - submodules (parent + N submodule repos): fetch fan-out
//
//  5. Fetch configuration. This is NOT a free axis: ci_runner derives it
//     from the workload (see fetchPushedRef / fetchTargetRef), so each
//     workload below defaults to its real production shape and only the
//     explicit-override rows sweep other values:
//     - defaults for every workflow fetch: --filter=blob:none
//     (config.GetGitFetchFilters) unless overridden in buildbuddy.yaml
//     - PR runs (merge-with-base, the default PR trigger): pushed ref
//     fetched at depth=0 (FULL history; forced even if a shallow depth
//     was explicitly requested, so the merge base is reachable), and the
//     target branch also fetched at depth=0. Two full-history
//     negotiations per run.
//     - normal pushes (main, tags, single branch): smart default depth=1
//     - bb remote: smart default (depth=1; no merge), overridable via
//     --git_fetch_depth (0=full, N)
//     - --depth is sticky per branch: a workspace whose .git was created
//     shallow (depth=1) that later needs full history fetches with
//     --unshallow, transferring nearly the entire history
//     - fallback: "unadvertised object" errors cause a retry of the whole
//     branch at depth=0
//     Explicit-override rows swept where marked: no-filter (full blobs),
//     depth=N, filter+depth combos, tags vs. no-tags.
//
//  6. Cache state at measurement time:
//     - everything cold (first-ever fetch of the repo)
//     - shard warm (steady state)
//     - shard cold, proxy warm (shard restart / reschedule; hydration path)
//     - AC entry present but referenced CAS blobs evicted (integrity
//     fallback: must re-fetch, must not serve a broken pack)
//
//  7. Runner snapshot state. Both workflows and remote bazel runners are
//     firecracker-recycled by default, and the snapshot key scheme (see
//     snaploader.SnapshotKeySet) determines what the runner's .git looks
//     like BEFORE the fetch we are measuring, i.e. which "haves" the client
//     brings to the negotiation:
//     - branch-snapshot:   resumed from the snapshot for GIT_BRANCH itself
//     (repeat run on the same PR branch / same bb remote branch / main).
//     Warm .git at the previous revision of that branch.
//     - fallback-snapshot: no snapshot for GIT_BRANCH; resumed from the
//     GIT_BASE_BRANCH or GIT_REPO_DEFAULT_BRANCH snapshot. This is what a
//     FIRST run on a new PR branch actually looks like: warm .git near
//     the base branch tip, fetching the PR head as a delta. NOT a cold
//     clone.
//     - true-cold: no usable snapshot at any key. Happens on first-ever use
//     of a (platform hash, configuration hash) combination, snapshot
//     eviction, explicit snapshot invalidation (version bump), or a
//     fallback snapshot older than DefaultMaxStaleFallbackSnapshotAge.
//     Only this state produces the full clone-from-scratch fetch.
//     - merge-queue: gh-readonly-queue/* branches read from the fallback
//     keys but WRITE their snapshot to the default branch key, so
//     successive merge queue runs chain off each other's state.
//     A crucial consequence for caching: all runners forked from the same
//     snapshot (same key + version) have byte-identical .git state, so
//     their incremental fetch negotiations are byte-identical too. That
//     makes even incremental fetches negotiation-cacheable, as long as the
//     fan-out shares snapshot lineage. Benchmarks that fetch from warm
//     workspaces must therefore distinguish "identical haves" (forked from
//     one snapshot) from "divergent haves" (organically aged workspaces),
//     because pack-level caches behave completely differently on the two.
//
// Not every cross-product cell is materialized: each benchmark below pins
// the dimensions that don't matter for what it measures and sweeps the ones
// that do. The scenario tables define the swept values in one place.
package gitmirror

import (
	"testing"
)

// -----------------------------------------------------------------------------
// Scaffolding: implementation-under-test plumbing
// -----------------------------------------------------------------------------

// Implementation abstracts a gitmirror candidate so every benchmark can run
// unmodified against each approach.
//
// type Implementation interface {
//     // Name returns the implementation name used in benchmark sub-test
//     // names, e.g. "baseline", "passthrough", "packcache", "baremirror",
//     // "baremirror-cas".
//     Name() string
//
//     // Start brings up numShards gitmirror instances and numProxies
//     // upstream cache proxies, wired to the given upstream git server
//     // (a testgit fixture wrapping server/util/mockgitserver, instrumented
//     // to count requests and bytes served). Implementations that don't
//     // use a tier ignore the corresponding counts (baseline ignores both).
//     Start(t testing.TB, topo Topology, upstream *InstrumentedUpstream) error
//
//     // CloneURL returns the URL a given client should use for the given
//     // repo, applying the topology's routing policy (repo-affinity hash vs.
//     // random shard choice). For baseline this is the upstream URL itself.
//     CloneURL(clientID int, repo string) string
//
//     // RestartShard simulates a shard reschedule (used by the hydration
//     // benchmarks): the shard loses all local state but proxies/CAS keep
//     // theirs.
//     RestartShard(i int) error
//
//     Shutdown() error
// }
//
// var implementations = []Implementation{ ... } // populated per candidate

// Topology describes one point on the deployment-shape axis.
//
// type Topology struct {
//     NumClients int
//     NumShards  int
//     NumProxies int
//     // Routing selects how clients map to shards: "repo-affinity" or
//     // "random".
//     Routing string
// }
//
// var topologies = []Topology{
//     {NumClients: 1, NumShards: 1, NumProxies: 0, Routing: "repo-affinity"},
//     {NumClients: 8, NumShards: 1, NumProxies: 1, Routing: "repo-affinity"},
//     {NumClients: 8, NumShards: 3, NumProxies: 1, Routing: "repo-affinity"},
//     {NumClients: 8, NumShards: 3, NumProxies: 1, Routing: "random"},
//     {NumClients: 64, NumShards: 3, NumProxies: 2, Routing: "repo-affinity"},
// }

// RepoShape describes one point on the repository-content axis. Fixture
// repos are generated once per benchmark binary run (deterministic seed) and
// hosted on the instrumented upstream.
//
// type RepoShape struct {
//     Name          string // "small", "medium", "large", "many-refs", "submodules"
//     ApproxBytes   int64
//     NumRefs       int
//     NumSubmodules int
// }

// FetchSpec describes one point on the fetch-configuration axis, mirroring
// the knobs ci_runner and bb remote actually pass to git.
//
// type FetchSpec struct {
//     Name    string // e.g. "partial-blob-none", "shallow-1", "full"
//     Depth   int    // 0 = full history
//     Filters []string
//     Tags    bool
// }

// Client-side helpers (to be implemented in a testgit util package):
//
//   - newColdWorkspace(t):        empty dir + `git init` (the true-cold
//                                 snapshot state: no usable snapshot at any
//                                 key)
//   - newSnapshotWorkspace(t, at) workspace with a prior fetch at commit
//                                 `at`, modeling a runner resumed from a
//                                 snapshot taken at that revision. Used for
//                                 both the branch-snapshot state (at = prior
//                                 revision of the pushed branch) and the
//                                 fallback-snapshot state (at = base /
//                                 default branch tip).
//   - forkSnapshotWorkspaces(ws, n) n exact copies of a snapshot workspace,
//                                 modeling n runners forked from the SAME
//                                 snapshot: identical .git, identical haves,
//                                 byte-identical negotiations.
//   - fetchCommit(ws, url, sha, spec) : fetch an exact SHA (workflows path:
//                                 fetchPushedRef / fetchTargetRef)
//   - fetchBranch(ws, url, branch, spec): fetch a branch head (bb remote
//                                 path: base branch + commit, with default-
//                                 branch fallback)
//   - pushCommits(upstream, repo, n, size): advance upstream state between
//                                 benchmark iterations
//   - forcePush(upstream, repo, branch)   : rewrite history (rebase case)

// -----------------------------------------------------------------------------
// 1. bb remote (hosted runner) fetch behavior
// -----------------------------------------------------------------------------

// BenchmarkBBRemoteRun measures the hosted runner (bb remote) fetch: the
// runner fetches the user's base branch at a specific commit before applying
// local patches. Hosted runners set recycle-runner=true and GIT_BRANCH /
// GIT_BASE_BRANCH / GIT_REPO_DEFAULT_BRANCH (see hostedrunner.go and
// cli/remotebazel), so they participate in the same snapshot key scheme as
// workflows.
//
// Sweeps: implementations x topologies x {medium, large} repo x
// FetchSpec {smart-default (resolves to depth=1: bb remote runs don't
// merge with a base), full (depth=0), depth=N} x snapshot state:
//   - branch-snapshot: the dominant repeat case. The user reruns bb remote
//     on the same branch with a small local edit; the runner resumes from
//     its own branch snapshot with the base commit usually already present,
//     so the fetch is a no-op or tiny delta. Establishes the "do no harm"
//     bound for a gitmirror tier.
//   - fallback-snapshot: first run on a new user branch; runner resumes
//     from the default branch snapshot and fetches the base commit as a
//     delta from near main tip.
//   - true-cold: no snapshot (new platform/config hash, eviction). Full
//     fetch; the highest-value case for every caching implementation.
//     Repeated true-cold iterations use the SAME base commit, so the fetch
//     is byte-identical every time; baseline pays full price every
//     iteration while caches should converge to hits.
//
// Per iteration:
//   - pick a client, create the workspace for the swept snapshot state
//   - fetchBranch(base branch @ pinned commit) via impl.CloneURL
//   - record wall time, upstream bytes/requests, client bytes
func BenchmarkBBRemoteRun(b *testing.B) {
	// for each impl / topology / repo shape / fetch spec / snapshot state:
	//   impl.Start(...)
	//   b.Run(name, func(b *testing.B) { ... loop as described above ... })
	//   impl.Shutdown()
}

// BenchmarkBBRemoteDefaultBranchFallback measures the fallback path where the
// user's local commit was never pushed upstream, so the runner fetches the
// repo default branch instead (see cli/remotebazel getBaseBranchAndCommit).
//
// Behavior under test (not just perf): the requested SHA does NOT exist
// upstream. Implementations must return the same protocol-level error the
// upstream would (so the runner's fallback logic still works), and must NOT
// cache the negative result in a way that masks the SHA once it is later
// pushed. After the SHA is pushed upstream, a follow-up fetch through the
// same (possibly warm) shard must succeed within the implementation's
// documented staleness bound.
func BenchmarkBBRemoteDefaultBranchFallback(b *testing.B) {
	// - fetch nonexistent SHA -> expect protocol error, record time-to-error
	// - pushCommits(upstream) making the SHA real
	// - fetch same SHA -> must succeed; record staleness window observed
}

// BenchmarkBBRemoteFetchDepthSweep isolates the cost of the
// --git_fetch_depth knob (smart default vs. 0=full vs. 1 vs. 100) on the
// large repo, per implementation. Answers: does a caching tier change the
// depth users should pick? (e.g. a warm pack cache may make full-history
// fetches cheap enough that depth tuning stops mattering.)
func BenchmarkBBRemoteFetchDepthSweep(b *testing.B) {
	// fixed topology {8 clients, 3 shards, 1 proxy}; sweep FetchSpec.Depth
}

// -----------------------------------------------------------------------------
// 2a. Workflows: repeated PR runs
// -----------------------------------------------------------------------------

// BenchmarkWorkflowPRFirstRun models the FIRST run on a new PR branch. With
// snapshot fallback keys this is NOT a cold clone: the runner resumes from
// the GIT_BASE_BRANCH (or GIT_REPO_DEFAULT_BRANCH) snapshot with a warm
// .git near the base branch tip. But because merge-with-base forces
// depth=0, both negotiations (pushed ref, then target branch) request FULL
// history with --filter=blob:none; how much actually transfers depends
// entirely on what the inherited snapshot's .git contains:
//   - snapshot written by a previous PR run: full history already present,
//     so the depth=0 fetches are small deltas. Cheap.
//   - snapshot written by push-to-main runs: .git is SHALLOW (main runs
//     fetch at depth=1), so the sticky-depth rule kicks in and the fetch
//     runs with --unshallow, transferring nearly the entire history. This
//     shallow-to-full transition is likely the single most expensive
//     recurring fetch in production (repos where main CI runs more often
//     than PR CI hit it on every new PR) and is a prime caching target:
//     every new PR branch pays a near-identical unshallow fetch.
//
// Iteration pattern "new PRs arriving":
//   - each iteration: create a new PR branch off main at a small delta,
//     resume a workspace from the swept snapshot lineage, fetch pushed ref
//     (depth=0) + target ref (depth=0)
//   - fan-out variant: k workflow actions (test, lint, build) triggered by
//     the same push, all forked from the SAME base snapshot. Their
//     negotiations are byte-identical, so this is negotiation-cacheable
//     even though the fetches are incremental. Measures whether packcache
//     captures it and whether coalescing works.
//   - true-cold variant: same workload with no snapshot at any key
//     (models snapshot eviction / new platform hash / first-ever workflow
//     for the repo). Produces a full-history clone from scratch.
//
// Sweeps: implementations x topologies x {small, medium, large} x
// snapshot lineage {full-history PR snapshot, shallow main snapshot,
// true-cold} x filters {blob:none (default), none}.
func BenchmarkWorkflowPRFirstRun(b *testing.B) {
	// report separate metrics for the single-fetch and fan-out phases so
	// implementations can be compared on both.
}

// BenchmarkWorkflowPRRepeatRun models repeated runs on the same PR: after
// the first run, the runner's snapshot is written under the PR branch key
// with FULL history in .git (the first run fetched at depth=0), so
// subsequent runs' depth=0 negotiations transfer only the delta for the
// new revision even though they request unlimited depth.
//
// Two have-distributions, which pack-level caches treat very differently:
//   - identical haves: all runners forked from the same PR branch snapshot
//     (the common steady state). Negotiations are byte-identical and
//     therefore cacheable; measures hit rate on incremental fetches.
//   - divergent haves: runners resumed from different snapshot versions
//     (staggered older revisions, e.g. after a snapshot version bump or
//     mixed executor pools). Every negotiation differs; packcache is
//     expected to miss and baremirror to serve natively. This quantifies
//     the gap between the two architectures on non-uniform traffic.
//
// Iteration pattern: push a new revision to the PR branch, then N runners
// fetch the delta concurrently. Compare against baseline to ensure a cache
// tier does not slow down the already-cheap incremental fetch.
func BenchmarkWorkflowPRRepeatRun(b *testing.B) {
	// - seed N workspaces per the swept have-distribution
	// - push a new revision; all N fetch the delta concurrently
}

// BenchmarkWorkflowPRForcePush covers rebase/force-push on a PR branch:
// history rewritten upstream, while the PR branch snapshot still holds the
// now-orphaned commits in its .git.
//
// Behavior under test: implementations must not serve stale packs for the
// rewritten ref; runners resumed from the stale snapshot (orphaned haves)
// must still converge (git handles this, but a negotiation-keyed cache
// could poison on the old key). Also measures cost: force-push invalidates
// delta reuse.
func BenchmarkWorkflowPRForcePush(b *testing.B) {
	// - fetch PR branch (warm caches, snapshot written for the branch key)
	// - forcePush(upstream, repo, branch)
	// - true-cold + stale-snapshot clients fetch; assert content correctness
	//   and record staleness window
}

// -----------------------------------------------------------------------------
// 2b. Workflows: repeated runs on main/master
// -----------------------------------------------------------------------------

// BenchmarkWorkflowMainPushSequence models the push-to-main workload:
// upstream main advances by a small commit every iteration and runners
// fetch the new tip each time. Default fetch shape: depth=1 (the smart
// default for non-merge runs) with --filter=blob:none, so the steady-state
// fetch is tiny and the snapshot's .git stays SHALLOW; the full history is
// never present unless a PR run later unshallows it. (That shallow .git is
// what makes the first PR run expensive; see BenchmarkWorkflowPRFirstRun.)
//
// Snapshot states swept:
//   - branch-snapshot (the steady state): main has its own snapshot key, so
//     runners resume warm at the previous main tip and fetch a small delta.
//     With m actions per push all forked from the same snapshot, the m
//     negotiations are byte-identical: cacheable by packcache, trivially
//     served by baremirror. Measures the "do no harm" bound and coalescing.
//   - true-cold: no snapshot (eviction, invalidation, new platform hash).
//     Every push changes the wanted SHA, so a (repo, want, no-haves) key
//     misses on the first fetch of each new tip and the cache only helps
//     the fan-out of the SAME tip. For bare mirrors the per-push cost is a
//     tiny mirror freshen plus pack generation from local objects. This is
//     the main head-to-head between the two architectures on cold traffic.
//
// Iteration pattern:
//   - pushCommits(upstream, main, 1, small)
//   - m clients fetch the new tip (fan-out: multiple actions per push)
//   - report: upstream bytes per push, client latency per fetch, shard CPU
//
// Sweeps: implementations x topologies x {medium, large} x
// {partial-blob-none, full} x fan-out m in {1, 4, 16} x snapshot state.
func BenchmarkWorkflowMainPushSequence(b *testing.B) {
}

// BenchmarkWorkflowMergeQueue models merge queue (gh-readonly-queue/*)
// branches. Per the snaploader write-key rules, merge queue runs READ from
// the fallback keys (default branch snapshot) and WRITE their snapshot to
// the default branch key, so successive queue entries chain off each
// other's state: entry N resumes from entry N-1's snapshot and fetches a
// queue branch that stacks N commits on main.
//
// Iteration pattern: enqueue a sequence of stacked queue branches, one run
// each, verifying the fetch is always a small delta off the previous
// entry's state. Measures whether the gitmirror tier handles the rapid
// creation/deletion of short-lived queue refs (ref advertisement churn) and
// whether per-branch cache keys pollute the cache with dead refs.
func BenchmarkWorkflowMergeQueue(b *testing.B) {
}

// -----------------------------------------------------------------------------
// 3. Cross-cutting cases the above don't isolate
// -----------------------------------------------------------------------------

// BenchmarkThunderingHerd: N clients (N = topology.NumClients, up to 64)
// simultaneously fetch a just-pushed SHA that no cache tier has seen.
// Measures request coalescing / single-flight behavior: the ideal
// implementation makes exactly ONE upstream fetch and fans the result out;
// the worst makes N. Sweeps routing policy: with random routing each shard
// sees an independent herd, so this also measures cross-shard duplication
// and whether the shared cache proxy tier deduplicates across shards.
//
// Two herd flavors, per the snapshot model:
//   - true-cold herd: N full clones of the new SHA (snapshot eviction or a
//     brand-new platform hash hitting a busy repo). Worst-case bytes.
//   - forked-snapshot herd: N incremental fetches with byte-identical haves
//     (all runners forked from the same snapshot; the common production
//     herd). Small bytes but identical negotiations, so coalescing should
//     reduce upstream requests to one here too.
func BenchmarkThunderingHerd(b *testing.B) {
	// - pushCommits(upstream)
	// - release N goroutines on a barrier, all fetching the new SHA
	// - assert upstream request count; report p50/p99 client latency
}

// BenchmarkFetchByExactSHA (behavior, not speed): workflow fetches request
// an exact commit SHA, not a ref (ci_runner fetchPushedRef uses commitSHA
// when set). Serving that requires uploadpack.allowAnySHA1InWant on
// whatever answers upload-pack, so this is a hard functional requirement
// for the baremirror implementations (git-http-backend does not allow it
// by default). If the server rejects the SHA, ci_runner falls back to
// re-fetching the whole branch at depth=0; the benchmark asserts the
// SHA-want path works on every implementation and, if the fallback ever
// triggers, attributes its (much larger) cost separately rather than
// silently folding it into fetch latency.
func BenchmarkFetchByExactSHA(b *testing.B) {
	// - fetch an advertised tip SHA, a recently-superseded SHA (still on the
	//   branch, no longer a tip), and a SHA on a deleted ref
	// - assert no unadvertised-object errors on the first two; record which
	//   implementations need the depth=0 branch fallback
}

// BenchmarkRefAdvertisement isolates GET /info/refs (ls-remote) cost on the
// many-refs repo shape. Every fetch pays this even when up to date, and it
// cannot be content-addressed (refs are mutable). Measures each
// implementation's advertisement freshness strategy: TTL-cached, always
// proxied, or served from mirror state; and records the staleness window
// between an upstream push and the new tip appearing in the advertisement
// served by a warm shard.
func BenchmarkRefAdvertisement(b *testing.B) {
	// - warm shard; loop: ls-remote through impl, occasionally push upstream,
	//   measure advertisement latency and observed staleness
}

// BenchmarkShardHydration measures shard restart cost: warm the system, then
// impl.RestartShard(i) and immediately drive the standard PR workload at the
// restarted shard.
//   - baremirror:      must re-clone from upstream (worst case)
//   - baremirror-cas:  hydrates mirror from the cache proxy tier: the
//     scenario that justifies wiring gitmirror into the CAS at all
//   - packcache:       stateless shard; no hydration, but every negotiation
//     goes to the proxy tier
//
// Report time-to-first-successful-fetch and upstream bytes during recovery.
func BenchmarkShardHydration(b *testing.B) {
}

// BenchmarkEvictedCASBlobs (packcache / baremirror-cas only): AC entries
// survive but some referenced CAS pack blobs have been evicted (evict
// directly via the proxy's cache interface). Behavior under test: the
// implementation must detect the missing blob (checkAllArtifactsExist-style
// validation), fall back to upstream, and repair the cache: never serve a
// truncated/corrupt pack. Measures the latency penalty of the integrity
// check on the happy path and of the fallback on the unhappy path.
func BenchmarkEvictedCASBlobs(b *testing.B) {
}

// BenchmarkUpstreamDegraded: upstream git server is slow (injected latency)
// or down entirely.
//   - slow: measures how much a warm cache insulates clients (fetch of a
//     cached SHA should not touch upstream at all)
//   - down: pins the availability policy per implementation: may a warm
//     mirror serve a previously-seen SHA while upstream is unreachable?
//     (ref advertisements must NOT be served stale-on-error without
//     surfacing staleness; exact-SHA fetches of immutable objects safely
//     can.) Records which requests succeed, fail, and how fast they fail.
func BenchmarkUpstreamDegraded(b *testing.B) {
}

// BenchmarkAuthOverhead measures per-fetch auth cost. gitmirror delegates
// credential validation to the upstream repo host with an auth cache keyed
// on hash(remote_url, hash(creds)) (see enterprise/server/gitmirror
// authCacheTTL). Cases:
//   - cold auth cache: every client's first fetch pays an upstream
//     round-trip
//   - warm auth cache: validation is local; measures residual overhead
//   - expired TTL under load: herd of requests at expiry must not stampede
//     upstream with validation calls
//   - invalid creds: must fail fast and must NOT negative-cache in a way
//     that locks out a client whose token was just fixed
func BenchmarkAuthOverhead(b *testing.B) {
}

// BenchmarkTenantIsolation (behavior, not speed): two groups fetch through
// the same shards.
//   - same public repo, different groups: is the cached data shared (best
//     for hit rate) or partitioned (best for isolation)? Record which, and
//     the hit-rate cost of partitioning: this is a product decision the
//     numbers should inform.
//   - private repo, group A's creds: group B presenting different (invalid
//     for that repo) creds must get an auth error, never cached content.
//     This must hold even when the pack for the same SHA is already in the
//     shared CAS tier.
func BenchmarkTenantIsolation(b *testing.B) {
}

// BenchmarkSubmoduleFanout: parent repo with N submodules; a workflow-style
// checkout fetches parent + all submodules. Measures whether shard routing
// keeps a repo family co-located (repo-affinity hashes each submodule URL
// independently, scattering them) and the aggregate latency of N serial or
// parallel fetch negotiations per checkout.
func BenchmarkSubmoduleFanout(b *testing.B) {
}

// BenchmarkConcurrentMixedWorkload: the "day in the life" soak: all
// workloads above running concurrently at the 64-client/3-shard/2-proxy
// topology against a mix of repo shapes, with pushes to main and PR branches
// interleaved on a timer. Not a microbenchmark: reports aggregate upstream
// egress, aggregate hit rate, and client latency percentiles per workload
// class. This is the single number to quote when comparing implementations
// end-to-end.
func BenchmarkConcurrentMixedWorkload(b *testing.B) {
}
