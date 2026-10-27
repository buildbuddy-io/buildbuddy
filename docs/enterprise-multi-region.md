---
id: enterprise-multi-region
title: Multi-region deployment and failover
sidebar_label: Enterprise Multi-Region
---

This doc covers the two ways to run BuildBuddy Enterprise across multiple regions, what happens in each when an entire region becomes unavailable, and some options for moving new builds to a healthy cluster, including automated ones.

## Summary

- Regions can either share MySQL, the blobstore, and ClickHouse ([Option 1](#option-1-multiple-regions-with-shared-mysql-blobstore-and-clickhouse)), or run as fully isolated deployments ([Option 2](#option-2-multiple-isolated-regions)). Apps, caches, executors, and Redis are always per-region.
- If a cluster goes down, BuildBuddy doesn't automatically redirect builds to another cluster. To fail over, new builds have to be pointed at a healthy cluster from outside BuildBuddy.
- One option is to choose the cluster in CI. In your CI job definition, you can run a setup step at the start of each job. That step could send a small test request to the preferred cluster and fall back to another cluster if the request fails or times out. See [Choosing the cluster in CI](#choosing-the-cluster-in-ci). Health-checked DNS is another option, but may be more operationally complex (see [Failing over with DNS](#failing-over-with-dns)).
- Builds that are running in the failed region will fail. Retried builds go to the healthy cluster. Optionally, when a build fails, the CI job can check the cluster's health and, if the check fails, retry the build on a healthy cluster within the same job (see [Retrying in-flight builds](#retrying-in-flight-builds)).
- Caches are not replicated between clusters, so expect more cache misses right after a failover. Bazel can recover on its own when a build expects outputs that only exist in the old cluster's cache (see [The cache after failover](#the-cache-after-failover)).
- With Option 1, the shared data stores need their own cross-region failover. If the region holding the MySQL primary goes down, every region is affected until the database fails over.

## Option 1: Multiple regions with shared MySQL, blobstore, and ClickHouse

With this option, the BuildBuddy apps in each region write to a shared MySQL database, blobstore bucket, and ClickHouse instance. Each region runs its own apps, cache, executors, and Redis.

- Every region's apps read the same database and bucket, so an invocation from any region can be viewed from a single UI hostname. Page loads are slightly slower for users far from the region serving the UI because of round-trip latency.
- Invocation-level uploads (build logs, persisted test logs, timing profiles, etc.) from every region must go to the shared bucket, or the UI can't read them. This usually works well, since invocation blobs are small compared to the rest of the data a build transfers.
- Run a read replica (`database.read_replica`) in each region. API key lookups go to the read replica, so a replica next to each region's apps keeps auth latency low in regions far from the primary.
- Some invocation pages (cache requests, action details, execution logs) read from the cache of the region where the build ran. Users always browse the UI at its main hostname, but on these pages the browser fetches data in the background from the app in the build's region. The UI picks that region by matching the hostname in the build's `--remote_cache` / `--remote_executor` flags against the `regions` config. To make this work:

  - Give each region other than the one serving the UI its own hostname suffix, under the same parent domain as the UI.
  - List every region in `regions`, including the one that serves the UI. Regional apps only accept cross-origin requests from origins that match the list.
  - Set `auth.domain_wide_cookies`, so the login cookie is sent with those background requests.
  - Set the same config on every region's apps.

  For example, with two regions:

  ```yaml
  auth:
    domain_wide_cookies: true
  regions:
    # Serves the UI at app.bb.example.com. Bazel uses remote.bb.example.com.
    - name: "US East"
      server: "https://app.bb.example.com"
      subdomains: "https://*.bb.example.com"
    # Bazel uses remote.us-west.bb.example.com.
    - name: "US West"
      server: "https://app.us-west.bb.example.com"
      subdomains: "https://*.us-west.bb.example.com"
  ```

- ClickHouse is not critical to builds, so running it in a single region with regular backups can be enough. Builds still succeed while ClickHouse is down, but analytics have gaps for that period (for example, the Executions page and Trends will be missing data).

### Pros

- Engineers can open every invocation link in the same UI, no matter which region ran the build, and invocation history covers all regions. After a failover, a build retried in another region shows up in the same history as its first attempt.
- Analytics (Trends, the Executions page, etc.) include builds from every region with no extra aggregation.
- Users, SSO, API keys, and org settings are set up once and shared by every region, so there is nothing to keep in sync.
- The same API key works in every region, so failing over only means changing which endpoints Bazel uses.

### Cons

- The shared data stores span regions. Losing the region that holds the MySQL primary affects every region until the database fails over.
- Writes from regions far from the MySQL primary pay cross-region latency.

### Surviving the loss of a region that hosts shared data

| Data store  | What to plan for                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MySQL       | Run the primary with a cross-region replica that can be promoted, such as Amazon Aurora Global Database, Cloud SQL for MySQL cross-region replicas, or Azure Database for MySQL cross-region read replicas. Depending on the service, promotion may be a manual step, and the new primary may have a different hostname. If it does, `database.data_source` has to be updated on every region's apps to point at it. Until the database fails over, apps in every region fail their readiness checks and builds fail everywhere.                                                                                                                                                                                                                                                |
| Blobstore   | Use storage that survives a region loss, such as a GCS dual-region or multi-region bucket, S3 with cross-region replication, or an Azure storage account with geo-redundant storage (GRS or GZRS). These differ in what failover involves. A GCS dual-region bucket keeps serving from the surviving region on its own. An S3 replica is a separate bucket, so BuildBuddy's storage config has to be repointed. An Azure account failover keeps the same endpoints, but you start it yourself and it can take about an hour. Replication is asynchronous in all three, so the most recent uploads may be unavailable or lost after a region failure. Without cross-region redundancy, invocation uploads fail, and logs and profiles stored in the lost region can't be viewed. |
| ClickHouse  | Nothing is needed to keep builds running. Restore from backup if the region is lost for good.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| UI hostname | The UI is served by one region's apps, so losing that region takes the UI down even though builds in other regions keep working. Every region's apps share the same data, so one option is to fail the UI hostname over to another region's apps (for example, with health-checked DNS).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

## Option 2: Multiple isolated regions

With this option, each region is a complete, independent BuildBuddy deployment with its own MySQL, blobstore, ClickHouse, and Redis. It's equivalent to running separate BuildBuddy installations.

### Pros

- Smallest blast radius. If a region becomes unavailable, the other regions keep working with no disruption, and there is no shared database to fail over.

### Cons

- Each region has its own UI. An invocation can only be viewed in the UI of the region that ran it, and history from a lost region is unavailable until the region comes back.
- Each region has its own orgs, users, and API keys. CI needs an API key for every cluster, and failing over has to switch the key along with the endpoints.
- Org settings, SSO, and secrets have to be configured in every deployment.
- Analytics across regions require custom aggregation on your end.

## Failover

### Scenario

A whole region fails: its apps, cache proxies, and executors all go down at once. For Option 1, this section assumes MySQL, the blobstore, and ClickHouse run in an HA setup and are still reachable. If they aren't, see [Surviving the loss of a region that hosts shared data](#surviving-the-loss-of-a-region-that-hosts-shared-data).

When this happens:

- Builds in flight against the failed region fail.
- Builds in other regions are unaffected.
- New builds keep going to the failed region until something points them at a healthy cluster.

The two approaches below are options to consider, not an exhaustive list, and they can be combined. Depending on the rest of your environment, a different approach may fit better.

### Choosing the cluster in CI

Since each CI job can be pointed at any cluster, one approach is to pick a cluster when the job starts. This takes three pieces:

1. A Bazel config for each cluster.
2. A health probe, so CI can tell which clusters are healthy.
3. A script that runs at the start of each CI job, probes the clusters, and selects the Bazel config for a healthy one.

#### 1. Bazel config for each cluster

```
# .bazelrc
common:bb-us-east --bes_backend=grpcs://remote.bb.example.com
common:bb-us-east --remote_cache=grpcs://remote.bb.example.com
common:bb-us-east --remote_executor=grpcs://remote.bb.example.com
# Option 1: every cluster links to the shared UI.
# Option 2: link to this cluster's own UI instead.
common:bb-us-east --bes_results_url=https://app.bb.example.com/invocation/

common:bb-us-west --bes_backend=grpcs://remote.us-west.bb.example.com
common:bb-us-west --remote_cache=grpcs://remote.us-west.bb.example.com
common:bb-us-west --remote_executor=grpcs://remote.us-west.bb.example.com
common:bb-us-west --bes_results_url=https://app.bb.example.com/invocation/

# Written by CI at the start of each job; not checked in.
try-import %workspace%/ci.bazelrc
```

#### 2. Health probe

The probe can be a small, read-only request to the same endpoint, with the same API key, that Bazel uses. That way it covers the whole path a build takes: DNS, the load balancer, TLS, authentication, the app, and the cache. One option is to look up the action cache entry for an action that doesn't exist, and treat a `NOT_FOUND` response within a few seconds as healthy. The server supports gRPC reflection, so [`grpcurl`](https://github.com/fullstorydev/grpcurl) can make this request without proto files:

```bash
grpcurl -max-time 5 \
  -H "x-buildbuddy-api-key: ${BB_API_KEY}" \
  -d '{"action_digest": {"hash": "0000000000000000000000000000000000000000000000000000000000000000", "size_bytes": 1}}' \
  remote.bb.example.com:443 \
  build.bazel.remote.execution.v2.ActionCache/GetActionResult
```

When the cluster answers, `grpcurl` exits with code 69 (64 plus the `NOT_FOUND` status code). Timeouts, connection failures, and other errors exit with a different code. If your load balancer doesn't route gRPC reflection requests, `grpcurl` can use the Remote Execution API `.proto` files instead (`-proto`).

The probe checks the apps and the cache, not executor capacity. For a stronger signal, you can also run a small scheduled canary build against each cluster and have the selection script take its result into account.

#### 3. Cluster selection script

The example script below probes each cluster in order of preference and selects the Bazel config for the first healthy one. It uses `grpcurl`, which needs to be installed on the CI machines, and expects the BuildBuddy API key in `BB_API_KEY`.

```bash
#!/usr/bin/env bash
# tools/select-bb-cluster.sh
# Picks a healthy BuildBuddy cluster for this CI job. Run it from the
# repository root before any Bazel commands, with the API key in BB_API_KEY.
set -euo pipefail

# Clusters in order of preference.
CLUSTERS=(us-east us-west)

# Each cluster's gRPC endpoint (the same one its Bazel config uses).
endpoint() {
  case "$1" in
    us-east) echo "remote.bb.example.com:443" ;;
    us-west) echo "remote.us-west.bb.example.com:443" ;;
  esac
}

# The API key to use for a cluster.
api_key() {
  # Option 1: the same key works for every cluster.
  echo "$BB_API_KEY"
  # Option 2: each cluster has its own key (e.g. BB_API_KEY_US_EAST).
  # local var="BB_API_KEY_$(tr 'a-z-' 'A-Z_' <<<"$1")"; echo "${!var}"
}

# An action digest that won't be in the cache.
PROBE_HASH=0000000000000000000000000000000000000000000000000000000000000000

# Looks up an action that doesn't exist. A NOT_FOUND response (grpcurl exit
# code 69) means the cluster answered.
healthy() {
  local addr key code
  addr="$(endpoint "$1")"
  key="$(api_key "$1")"
  # Retry so that a single blip doesn't trigger a failover.
  for _ in 1 2 3; do
    code=0
    grpcurl -max-time 5 -H "x-buildbuddy-api-key: ${key}" \
      -d '{"action_digest": {"hash": "'"${PROBE_HASH}"'", "size_bytes": 1}}' \
      "$addr" build.bazel.remote.execution.v2.ActionCache/GetActionResult \
      >/dev/null 2>&1 || code=$?
    if [[ "$code" == 69 ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Set BB_CLUSTER in the job's environment to skip the probe and pin a cluster.
if [[ -z "${BB_CLUSTER:-}" ]]; then
  # If no cluster is healthy, use the first one so the build fails with a real error.
  BB_CLUSTER="${CLUSTERS[0]}"
  for c in "${CLUSTERS[@]}"; do
    if healthy "$c"; then
      BB_CLUSTER="$c"
      break
    fi
  done
fi
echo "Using BuildBuddy cluster: ${BB_CLUSTER}"
rc="ci.bazelrc"
echo "common --config=bb-${BB_CLUSTER}" > "$rc"

# Option 2 only: send this cluster's API key with every Bazel request.
# echo "common --remote_header=x-buildbuddy-api-key=$(api_key "$BB_CLUSTER")" >> "$rc"
```

To run it at the start of each job:

- In Buildkite, call it from a `pre-command` hook (for example, `.buildkite/hooks/pre-command` in the repository).
- In GitHub Actions, add a step after checkout and before any Bazel steps:

  ```yaml
  - name: Select BuildBuddy cluster
    run: ./tools/select-bb-cluster.sh
    env:
      BB_API_KEY: ${{ secrets.BB_API_KEY }}
      # Empty unless set as a repository or organization variable.
      BB_CLUSTER: ${{ vars.BB_CLUSTER }}
  ```

When the preferred cluster is healthy, the probe adds well under a second to job startup. When it's down, the retries add up to about 20 seconds before the job moves to the next cluster.

#### Retrying in-flight builds

Builds that are running when their cluster fails will fail. Optionally, the CI job can retry them on another cluster. After a failed Bazel command, the job runs the selection script again. If the cluster the build used is no longer healthy, the script picks a different one and the job retries the command. If the cluster is still healthy, the selection doesn't change, so ordinary build and test failures aren't retried.

```bash
# The selection script already ran at the start of the job.
before="$(cat ci.bazelrc)"
bazel test //... || {
  status=$?
  # Re-check health. If this cluster is down, the script picks another one.
  ./tools/select-bb-cluster.sh
  # Only retry if the cluster changed.
  [[ "$(cat ci.bazelrc)" != "$before" ]] || exit "$status"
  bazel test //...
}
```

If `BB_CLUSTER` is pinned, the script never switches clusters, so nothing is retried. The retry runs against the other cluster's cache, so it's usually slower than a normal build (see [The cache after failover](#the-cache-after-failover)).

#### Manual override

To pin builds to a cluster during an incident, or to drain a cluster before planned maintenance, set `BB_CLUSTER` in the CI job's environment. For example:

- In Buildkite, set it on a single build, on a pipeline, or for every pipeline in the agents' `environment` hook.
- In GitHub Actions, set it as a repository or organization variable. With the step above, an organization variable acts as a single switch for every repository.

#### Failing back

With the script above, new builds go back to the preferred cluster as soon as it passes its health check again, and its cache is usually still warm. If you'd rather fail back by hand (for example, once the region has been stable for a while), pin `BB_CLUSTER` to the secondary cluster until you're ready.

### Failing over with DNS

Another approach is to put a single hostname (e.g. `remote.bb.example.com`) in front of all clusters, using health-checked DNS failover or a global load balancer, such as Amazon Route 53 failover routing, Google Cloud DNS routing policies with health checks, or Azure Traffic Manager priority routing. These can use the health endpoints BuildBuddy's apps expose for load balancers: the standard gRPC health check (`grpc.health.v1.Health/Check`), or `GET /readyz?server-type=buildbuddy-server`, which returns `200` when the app is ready. An app is ready when its MySQL primary and read replica, Redis, and cache all pass their health checks. Some of these services can't probe over gRPC, so they would use `/readyz`, and some can only probe endpoints that are reachable from the internet. This needs no CI changes and also covers developer machines. Some tradeoffs compared with choosing the cluster in CI:

- Failover is slower. It waits for the health checks to trip, then for the DNS TTL and any client-side DNS caching. Bazel also keeps its gRPC connections open, so it only picks up the new address when it reconnects.
- You can't pin an individual build, and a build can end up talking to two clusters if DNS changes partway through.
- With Option 1, it breaks some UI pages. The UI finds a build's cache by its region-specific hostname, so with one global hostname, cache-backed pages (action details, cache requests, execution logs) don't work for builds that ran outside the UI's region.
- With Option 2, it breaks auth. Isolated clusters have different API keys, so silently moving a client to another cluster fails authentication.

DNS failover can also work well for the UI hostname in Option 1, where any region's apps can serve the UI. During an incident, developers can switch clusters by hand with the same configs CI uses (for example, `--config=bb-us-west`).

### The cache after failover

Caches are not replicated between clusters, so failing over has two effects.

First, there will be more cache misses. Builds in the new cluster have to redo work the old cluster had cached, so they're slower, and executor and cache load is higher until the cache warms up. How much depends on how much the two clusters' normal workloads overlap.

Second, reused CI machines can hit lost inputs. If a CI machine keeps its Bazel output base between jobs and uses Build without the Bytes (the default since Bazel 7), Bazel remembers some outputs as stored in the old cluster's cache. When the next job targets the new cluster, those blobs aren't there. Bazel can recover on its own:

- Action rewinding (`--rewind_lost_inputs`) re-runs only the actions whose outputs are missing, within the same build. It is enabled by default in Bazel 9. If you're on Bazel 8, use a recent release and test it first, since support for recovering lost remote outputs matured over the 8.x releases.
- Build retries (`--experimental_remote_cache_eviction_retries`) reset Bazel's record of remote outputs and retry the whole command when rewinding can't recover. The default is 5 in recent versions, but older versions defaulted to 0, so set it explicitly:

  ```
  common --experimental_remote_cache_eviction_retries=5
  ```

  Each retry gets a new invocation ID. If your CI passes `--invocation_id` explicitly, check that retries still apply on your Bazel version, since some older versions disable them when it's set.

If a reused CI machine still fails with lost-input errors, running `bazel clean` on it clears the stale state. CI machines that start each job with a fresh workspace (such as GitHub-hosted runners) only see the extra cache misses.

### Capacity planning

Every cluster that can receive failover traffic needs headroom to absorb it:

- executor autoscaling limits
- cache disk and throughput, since a colder cache means more writes and more executions
- app replicas

If the surviving cluster can't scale up quickly enough, remote actions queue and builds slow down.

### Testing the procedure

It's worth rehearsing failover before it's needed. For example, in a staging environment, scale one cluster's apps to zero, then confirm that:

- new CI jobs land on the other cluster and pass
- the UI still works, including cache-backed invocation pages for builds from each region (Option 1)
- builds fail back once the apps return

## Checklist

A summary of the pieces described above, assuming the cluster is chosen in CI:

- [ ] A `--config` for each cluster in `.bazelrc`, plus a `try-import` of the CI-written bazelrc.
- [ ] A script that runs at the start of each CI job (for example, from a Buildkite `pre-command` hook or a GitHub Actions step), probes health, honors a `BB_CLUSTER` override, and writes the selected config.
- [ ] Optionally, an in-job retry of failed builds when their cluster is unhealthy.
- [ ] `grpcurl` installed on all CI machines, and the API key available to the selection script.
- [ ] `--experimental_remote_cache_eviction_retries` set in CI, and a Bazel version with action rewinding.
- [ ] Headroom in each cluster to absorb another cluster's traffic.
- [ ] A failover drill in staging.
- [ ] Option 1: a read replica in each region, per-region hostnames with `regions` and `auth.domain_wide_cookies` configured, cross-region MySQL failover, blob storage that survives a region loss, and optionally failover for the UI hostname.
- [ ] Option 2: an API key for each cluster available to CI, and the script selecting the right one.
