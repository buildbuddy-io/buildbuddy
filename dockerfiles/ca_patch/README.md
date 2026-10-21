# CA patch images

Minimal patches that stop BuildBuddy's Ubuntu RBE/workflow images from
trusting the compromised eMudhra intermediates (via the `emSign ECC Root CA -
G3` and `Comodo AAA Services` roots). Tracking issue:
buildbuddy-io/buildbuddy-internal#8615.

Each patched image is `FROM <current digest>` plus one layer. Every existing
layer, `Env`, `Cmd`, `User` etc. stays the same. None of the bases set `USER`,
so the patch layer runs as root and nothing needs restoring.

- `Dockerfile`: `apt-get install --only-upgrade ca-certificates`, for bases
  whose archive has the fixed package (`20260816ubuntu0.22.04.1`,
  `20260816ubuntu0.24.04.1`). apt verifies packages with Ubuntu's signing key,
  so this does not depend on the TLS trust being fixed.
- `Dockerfile.remove_roots`: for Ubuntu 20.04, whose archive's newest
  `ca-certificates` (`20240203~20.04.1`) still ships both roots. It deselects
  the two roots in `/etc/ca-certificates.conf` (a `!` prefix, the format
  debconf writes) and runs `update-ca-certificates`, which also rebuilds the
  Java `cacerts`. No packages change.

| Image | Base digest | Dockerfile | Platforms |
|---|---|---|---|
| `rbe-ubuntu20-04` | `sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622` | `Dockerfile.remove_roots` | amd64 |
| `rbe-ubuntu22-04` (`platform.go`) | `sha256:0d84a80bb0fc36ba5381942adcf6493249594dcc9044845c617b78c9b621cae3` | `Dockerfile` | amd64, arm64 (index) |
| `rbe-ubuntu22-04` (prod `executor.default_image`) | `sha256:904d95e322772c324428c9f91ace8844cb01d79e4a08d08db97f62b4063c99c7` | `Dockerfile` | arm64 only |
| `rbe-ubuntu24-04` | `sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a` | `Dockerfile` | amd64, arm64 (index) |
| `rbe-ubuntu20-04-workflows` | `sha256:ba28945426fcdf4310f18e8a8e3c47af670bdcf9ba76bd76b269898c0579089e` | `Dockerfile.remove_roots` | amd64 |
| `box-ubuntu24-04` | `sha256:a256e74676a4a73f0334038538a421bdbdce41169d7333e246a6cd0f59c5ed6b` | `Dockerfile` | amd64, arm64 (index) |
| `rbe-ubuntu20-04-webtest` | `sha256:754fee72cee06fc72215b5d7edf31ef45116a4468cf5a5ccd4301acd83cba531` | `Dockerfile.remove_roots` | amd64 |

## Validate

`validate.sh BASE DOCKERFILE LOCAL_TAG` builds the patch locally (pushes
nothing) and reports the `dpkg-query -W` diff, the layer comparison, the trust
store, TLS checks against the eMudhra test hosts (must fail) and normal hosts
(must pass), Java `cacerts`, and a compiler/java/python smoke test. To check an
arm64 base on an amd64 host, install `qemu-user-static` first and pass the
arm64 child manifest digest.

## Build and push (owner only)

Push by digest only; don't move any tag. Use the same platforms as the base so
a multi-arch index stays multi-arch. `FROM` an index digest resolves to the
matching child for each platform.

```sh
cd dockerfiles/ca_patch
NAME=rbe-ubuntu24-04
BASE=gcr.io/flame-public/$NAME@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a
# Pick the Dockerfile and --platform from the table above.
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg BASE="$BASE" -f Dockerfile --provenance=false \
  --output type=image,name=gcr.io/flame-public/$NAME,push-by-digest=true,name-canonical=true,push=true \
  --metadata-file /tmp/$NAME.json .
jq -r '."containerimage.digest"' /tmp/$NAME.json   # the new digest to pin
```

`push-by-digest=true` uploads the image without creating or moving any tag.
Re-run `validate.sh` against the pushed digest before pinning it.
