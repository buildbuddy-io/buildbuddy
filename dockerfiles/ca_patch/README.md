# CA patch images

Minimal patches that stop BuildBuddy's Ubuntu RBE/workflow images from
trusting the compromised eMudhra intermediates (via the `emSign ECC Root CA -
G3` and `Comodo AAA Services` roots). Tracking issue:
buildbuddy-io/buildbuddy-internal#8615.

Each patched image is `FROM <current digest>` plus one layer. Existing layers
and the runtime config (`Env`, `Cmd`, `User`, ...) don't change.

- `Dockerfile` installs exactly `ca-certificates=${CA_VERSION}` from the
  Ubuntu archive (apt verifies it with Ubuntu's signing key, not TLS). The
  build fails if `CA_VERSION` is empty or unavailable.
- `Dockerfile.remove_roots` is for releases with no fixed package. It deselects
  `CERTS` in `/etc/ca-certificates.conf` and runs `update-ca-certificates`. No
  packages change. Ubuntu 20.04's newest package (`20240203~20.04.1`) still
  ships both roots; 16.04 and 18.04 are end-of-life.
- Both run `check_roots_removed.sh` at build time (bind-mounted, not copied
  into the image). It fails unless each root is deselected, has no `.pem`
  link, is absent from `ca-certificates.crt` and the Debian Java keystore (by
  SHA-256 fingerprint), and `/etc/ssl/certs` has no dangling links.
- `validate.sh BASE PATCHED [CERTS]` compares two existing images and never
  builds. It exits nonzero if base layers changed, there isn't exactly one new
  layer, the config changed, any package other than `ca-certificates` changed,
  a test host doesn't fail certificate verification (curl exit 60), or
  google.com, `git ls-remote` against github.com, or Python against pypi.org
  fails.

| Image                                             | Base digest                                                               | Platforms                            | Dockerfile                | Build arg                                    |
| ------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------ | ------------------------- | -------------------------------------------- |
| `rbe-ubuntu22-04` (`platform.go`)                 | `sha256:0d84a80bb0fc36ba5381942adcf6493249594dcc9044845c617b78c9b621cae3` | amd64 `19a6ae9d…`, arm64 `8407a034…` | `Dockerfile`              | `CA_VERSION=20260816ubuntu0.22.04.1`         |
| `rbe-ubuntu22-04` (prod `executor.default_image`) | `sha256:904d95e322772c324428c9f91ace8844cb01d79e4a08d08db97f62b4063c99c7` | arm64 only                           | `Dockerfile`              | `CA_VERSION=20260816ubuntu0.22.04.1`         |
| `rbe-ubuntu24-04`                                 | `sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a` | amd64 `5d2d8b39…`, arm64 `01898e87…` | `Dockerfile`              | `CA_VERSION=20260816ubuntu0.24.04.1`         |
| `box-ubuntu24-04`                                 | `sha256:a256e74676a4a73f0334038538a421bdbdce41169d7333e246a6cd0f59c5ed6b` | amd64 `4caf8c7c…`, arm64 `0871e3dd…` | `Dockerfile`              | `CA_VERSION=20260816ubuntu0.24.04.1`         |
| `rbe-ubuntu20-04`                                 | `sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622` | amd64                                | `Dockerfile.remove_roots` | (default `CERTS`)                            |
| `rbe-ubuntu20-04-workflows`                       | `sha256:ba28945426fcdf4310f18e8a8e3c47af670bdcf9ba76bd76b269898c0579089e` | amd64                                | `Dockerfile.remove_roots` | (default `CERTS`)                            |
| `rbe-ubuntu20-04-webtest`                         | `sha256:754fee72cee06fc72215b5d7edf31ef45116a4468cf5a5ccd4301acd83cba531` | amd64                                | `Dockerfile.remove_roots` | (default `CERTS`)                            |
| `buildbuddy-ci-runner` (18.04)                    | `sha256:8cf614fc4695789bea8321446402e7d6f84f6be09b8d39ec93caa508fa3e3cfc` | amd64                                | `Dockerfile.remove_roots` | (default `CERTS`)                            |
| `executor-docker-default` (16.04)                 | `sha256:c346c46f3333d7d7f102c6e19a36bcf8403de465ad3cec91c2265a4d56987124` | amd64                                | `Dockerfile.remove_roots` | `CERTS=mozilla/Comodo_AAA_Services_root.crt` |

None of the bases set `USER`.

## Build, validate, push

Use one `docker-container` buildx builder throughout, so the push reuses the
exact layers that were validated. Run from this directory.

```sh
docker buildx create --name ca-patch --use
NAME=rbe-ubuntu24-04
BASE=gcr.io/flame-public/$NAME@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a
ARGS="--build-arg BASE=$BASE --build-arg CA_VERSION=20260816ubuntu0.24.04.1 -f Dockerfile --provenance=false"

# 1. Build each platform into the local image store.
docker buildx build $ARGS --platform linux/amd64 --load -t ca-patch/$NAME:amd64 .
docker buildx build $ARGS --platform linux/arm64 --load -t ca-patch/$NAME:arm64 .

# 2. Validate each against the matching base child digest (arm64 needs an
#    arm64 host or qemu-user-static).
./validate.sh gcr.io/flame-public/$NAME@sha256:5d2d8b3935c4dad4d81e0b6d89eeb06734d0e708cdf3d8ec779cf619c9243860 ca-patch/$NAME:amd64
./validate.sh gcr.io/flame-public/$NAME@sha256:01898e8731f34ab4104c956909cb3b3020e630e9146c2cbbef145d8155febdf4 ca-patch/$NAME:arm64

# 3. Push by digest (no tag is created or moved); the build is a cache hit.
docker buildx build $ARGS --platform linux/amd64,linux/arm64 \
  --output type=image,name=gcr.io/flame-public/$NAME,push-by-digest=true,name-canonical=true,push=true \
  --metadata-file /tmp/$NAME.json .
jq -r '."containerimage.digest"' /tmp/$NAME.json   # the new digest to pin
```

Warning: without `push-by-digest=true`, `--push` tags and pushes `:latest`.

Before pinning, re-run `validate.sh` against the pushed per-platform digests
(`docker buildx imagetools inspect gcr.io/flame-public/$NAME@<digest>`).
