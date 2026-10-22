# RBE images

The default RBE images (`gcr.io/flame-public/rbe-ubuntu{20,22,24}-04`) are
built from the Dockerfiles in this directory by CI, not by hand.
`build_image.py` refuses to push them (pushing is its default); it can still
build them locally with `--no-push`.

To ship a new image:

1. Dispatch the internal `build-rbe-image` workflow with inputs `image` (which
   RBE image to build) and `ref` (the buildbuddy ref to build from). It builds
   the image and (once implemented) checks it with `rbe_image_check.sh` and
   pushes it under a new, unique tag. It never moves `:latest`.
2. Open a PR that pins the new digest in `server/util/platform/platform.go`.
3. Ship it in a release and let it soak.
4. Only then move `:latest`, by hand, noting the old digest first so it can be
   rolled back.

The 20.04 image is amd64-only: its Dockerfile downloads amd64 Go and clang and
hard-codes an amd64 `JAVA_HOME`.

`rbe_image_check.sh` runs inside the image it checks, e.g. as an action on
BuildBuddy RBE. To check an image locally:

```sh
docker run --rm -v "$PWD/dockerfiles:/c:ro" <image> /c/rbe_image_check.sh             # tools, setup, https, git
docker run --rm -v "$PWD/dockerfiles:/c:ro" <image> /c/rbe_image_check.sh --packages  # sorted dpkg list
```
