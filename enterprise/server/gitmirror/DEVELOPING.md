# Git mirror development

The Git mirror is a smart HTTP caching proxy. It serves Git fetches from local
bare repositories using `git upload-pack` and forwards all other requests to
the upstream Git server.

For an overview of the protocol, see the [Git smart HTTP primer](./PRIMER.md).

See also these Git documents:

- [HTTP transfer protocols](https://git-scm.com/docs/gitprotocol-http)
- [Git protocol version 2](https://git-scm.com/docs/gitprotocol-v2)
- [Pack protocol](https://git-scm.com/docs/gitprotocol-pack)
- [`git-upload-pack`](https://git-scm.com/docs/git-upload-pack)
- [Repository layout](https://git-scm.com/docs/gitrepository-layout)
- [Packfile format](https://git-scm.com/docs/gitformat-pack)

## Run locally

Run the standalone binary with:

```bash
bazel run -- //enterprise/server/cmd/gitmirror:gitmirror
```

The mirror requires a relatively new version of `git` (`2.48` or later) so that
it can track the upstream repository's default branch as part of each fetch. On
Ubuntu 24.04, you may need to install `git` via PPA:

```bash
sudo add-apt-repository ppa:git-core/ppa
sudo apt update
sudo apt install git
```

## Package layout

- `gitstorage` assigns persistent repository identities and manages the local
  bare repository mirrors.
- `gitremote` validates and resolves upstream repository URLs, handles
  credentials and redirects, and forwards HTTP requests to upstream servers.
- `gitmirror` composes resolution and storage behind the smart HTTP handlers,
  forwarding requests that it does not handle locally.
