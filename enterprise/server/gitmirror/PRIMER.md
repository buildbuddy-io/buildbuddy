# Git smart HTTP primer

The Git mirror only implements the "git-upload-pack" service, which is part of
Git's "smart" HTTP protocol. All other requests (pushes, Git LFS, older "dumb"
protocol requests, etc.) are just forwarded to the upstream.

A smart HTTP fetch begins with service discovery, followed by one or more
requests to the `git-upload-pack` service. These later requests discover refs
when necessary, negotiate which objects the client lacks, and transfer missing
objects in compressed bundles called packfiles, or "packs" for short.

## Discovery phase

The client starts by requesting `GET /info/refs?service=git-upload-pack`. Only
metadata is exchanged during this phase. Repository objects such as commits,
trees, blobs, and annotated tags are transferred later.

In protocol v2, the returned metadata does not contain any refs, despite the
endpoint name. The name is retained for backwards compatibility. Instead, the
endpoint advertises the commands and capabilities supported by the server.

Commands are operations the client can request next. A command may also
advertise command-specific features after its name:

- `ls-refs=unborn` lists selected refs and can report branches without a
  commit.
- `fetch=shallow filter sideband-all` negotiates and downloads objects, with
  support for shallow and partial clones and multiplexed output.
- `object-info=size` reports the sizes of selected objects without downloading
  their contents.
- `bundle-uri` discovers pre-generated repository bundles.

Other capabilities describe server or connection properties. For example,
`object-format=sha1` reports that the repository uses SHA-1 object IDs, while
`agent=git/...` identifies the server implementation.

Protocols v0 and v1 do not advertise separate commands. Discovery instead
returns the available refs and their object IDs, along with capabilities that
apply to the fetch; these clients do not use `ls-refs`. Clients request protocol
v1 or v2 using the `Git-Protocol` header. Omitting the header selects v0.

A protocol v2 client sends an `ls-refs` command to
`<repo>/git-upload-pack` to request the refs it needs. Although this is a POST
request, it remains part of discovery and does not transfer repository objects.

## Fetch phase

The client performs fetch negotiation using one or more
`POST <repo>/git-upload-pack` requests. The request that completes negotiation
returns a packfile containing the missing Git objects.

Small terminology note: If a delta within a pack references an object that is
not also in the packfile itself, then the pack is called a "thin" pack.

Packfile negotiation works as follows:

- The client sends `want` object IDs, usually commits at the tips of branches
  or tags it wants to fetch.

  - "Wanting" a commit asks for the objects reachable from it: its root tree,
    the nested trees and blobs beneath it, zero or more parent commits, and
    everything transitively reachable from those parents. The trees and blobs
    form a Merkle tree, similar to the one used by Bazel's Remote Execution API.
    Git blobs are content-addressed, like blobs stored in Bazel's CAS.
  - If the server supports filtering, the client may request
    `filter blob:none`. The server sends commits and trees but omits blobs,
    which the client can fetch later as needed. This reduces the initial
    transfer, but operations that inspect file contents, such as diffs and
    checkouts, may require network access and many smaller fetches.
  - The mirror sets `uploadpack.allowAnySHA1InWant=true`, allowing CI and Bazel
    clients to request an exact object ID that is already present locally. It
    does not make an object available when the upstream refused to serve it or
    the mirror has not fetched it.

- The client sends batches of `have` object IDs for commits already in its
  local repository, starting from commits at the tips of its local refs and
  walking backward through their parents.

  - A `have` tells the server that objects reachable from that commit need not
    be included in the pack.
  - A shallow client sends `shallow` lines naming commits whose parents it
    lacks. This prevents the server from assuming that history before the
    shallow boundary is present.

- The server acknowledges `have` commits that it also has. Each acknowledged
  commit establishes that the commit and its ancestors are common.

- The client stops sending `have` lines once additional lines would only name
  ancestors of commits the server has already acknowledged. Sending them would
  not help the server omit any more objects from the packfile.

  - The client may end negotiation by sending `done`.
  - Once the server has found an acceptable common base, it may instead signal
    `ready` and begin sending the packfile immediately, avoiding another round
    trip.

- The server walks the object graph starting from the wanted objects and
  excludes objects reachable from the acknowledged `have` commits. It encodes
  the remaining commits, trees, blobs, and annotated tag objects in a packfile
  and streams it to the client.

A clone normally has no `have` commits, while an incremental fetch can omit
most existing history.

For a simplified example, suppose the server's `main` is `def456`, whose parent
is the client's current `origin/main`, `abc123`. `C` means client and `S` means
server:

```text
C: want def456
C: have abc123
S: ACK abc123
S: ready
```

The acknowledged commit is the parent of the wanted commit, so the client
already has the earlier history. The server can send the objects introduced by
`def456` without another negotiation round. This example omits protocol framing,
and a protocol v2 server may omit the `ACK` when it sends `ready`.
