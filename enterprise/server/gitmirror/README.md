<!-- Agents: DO NOT EDIT -->

# BuildBuddy Git mirror server

This package contains the implementation of BuildBuddy's Git mirror server.

The Git mirror effectively acts as a read-through cache for Git providers such
as GitHub or GitLab. It allows the BuildBuddy CI runner to download fewer bytes
from upstream providers, which improves reliability and performance of Git
repository setup and operations.

See DEVELOPING.md for development notes.

## Basic usage

The server can be run without any arguments. By default, it will:

- Listen on `0.0.0.0:8180`.
- Store repositories in `$XDG_CACHE_HOME/.cache/buildbuddy/git-mirror`.
- Disallow insecure `http` remotes.

Run with `-help` to see configuration options (all options are prefixed with
`git.mirror.*`).

Local `git` clients can use the mirror via URL rewrites. For example, if the
service is exposed at https://git.buildbuddy.io, the following command will
proxy all of git's https traffic through the mirror:

```bash
git config --global url."https://git.buildbuddy.io/v1/".insteadOf "https://git.buildbuddy.io/v1/"
git config --global --add url."https://git.buildbuddy.io/v1/".insteadOf "https://"
```

The first rule prevents direct mirror URLs from being rewritten by the second
rule. Git selects the longest matching `insteadOf` prefix, so the mirror prefix
maps to itself while other HTTPS URLs map to the mirror.

If using credential helpers, the credential helper will see the mirror URL.
Helpers must map the mirrored path back to the upstream repository and return its
credentials. The following configuration is also needed:

```bash
git config --global credential."https://git.buildbuddy.io".useHttpPath true
```

Otherwise the credential helper will only see the mirror hostname, and will not
receive the path. Credential helpers should parse the HTTP path as follows: XXX

## API

The Git mirror serves Git's HTTP protocol, versions v0 through v2.

<!-- TODO: documentation link -->

The server matches all requests against the path pattern `/v1/<repo>/<git-path>`.
Example: https://git.buildbuddy.io/v1/github.com/buildbuddy-io/buildbuddy/info/refs?service=git-upload-pack

<!-- TODO(bduffany): require an x-buildbuddy-api-key header as well. -->

Two requests are handled specially:

- `/info/refs?service=git-upload-pack`: XXX
- `/git-upload-pack`: XXX

All other requests are proxied to the upstream as-is.

### Limitations

Because the mirror proxies unknown requests, it is mostly a drop-in replacement
for any existing git remote. There are a few known exceptions:

- The Git mirror does not currently support repository URLs containing query
  parameters, including catch-all gateway URLs such as
  `https://example.com/daemon.cgi?svc=git&q=`. Supporting these URLs would
  require encoding the repository's base query separately from Git's per-request
  query parameters. See the [Git HTTP protocol documentation][https://git-scm.com/docs/gitprotocol-http#_url_format].
- SSH is not yet supported.

## Caching behavior

TODO: describe how the mirror caches requests and saves traffic.

## Repo IDs

The git mirror needs a way to securely and consistently map an incoming repo URL
to a local directory. To accomplish this, a "repo ID" is computed for requested
repositories. To compute the repo ID:

- A request is made to the upstream to respect any provider-specific
  normalization. For example, GitHub redirects
  `https://github.com/buildbuddy-io/buildbuddy.git` to
  `https://github.com/buildbuddy-io/buildbuddy`, stripping the `.git` suffix.
  The redirect result is cached for a configurable TTL.
- The scheme and port are made explicit. `github.com/buildbuddy-io/buildbuddy`
  becomes `https://github.com:443/buildbuddy-io/buildbuddy`.
- The SHA256 of the resulting URL is the repo ID. For the buildbuddy repo, this
  is `be69d98478e7756fc41d26544069c5f3c01ace9af3bfd3e5703d138eefb1f53c`

## Storage

All mirrored repos are stored at `git.mirror.root_directory`, which defaults to
`$XDG_CACHE_HOME/.cache/buildbuddy/git-mirror`. The root directory tree looks
like the following:

```yaml
# Root (git.mirror.root_directory)
- /home/$USER/.cache/buildbuddy/git-mirror/:
    # Two-letter repo ID prefix
    - ae/:
        # "<repo-id>_<repo-label>.git"
        - ae52660917905a43af4250175ec083bcb4cf4e7e08d4bbc69440e0b0aa2c344e_github.com_buildbuddy-io_buildbuddy.git/:
            - config # Contains 'origin' remote with upstream repo URL
            - objects/ # Object DB
            - ... # Other git files
```

## Upstream repository resolution

<!--
TODO: should we require a BB api key in order to use the git mirror? Maybe
configured as a git CURL header?
-->

## Data retention

TODO: document

## Scaling and replication

The Git mirror server is designed to be stateless. It does not currently support
P2P replication. It can be scaled easily by adding more shards. However, each
shard independently pulls from the git remote.

A future improvement could be to add each shard to a hash ring (using k8s
discovery), and use consistent hashing to designate one shard as the primary for
each repository URL. Peers could then pull through the primary shard, pulling
only the repository data they don't have locally.

## Metrics

TODO: document

TODO: attribute each `git` process CPU usage back to the group, and report in
metrics. Maybe also flush to usage table?

## ci_runner configuration

TODO: document

<!-- TODO: most helpful / relevant links? -->
