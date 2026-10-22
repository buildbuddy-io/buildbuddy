# RBE images

The default RBE images (`gcr.io/flame-public/rbe-ubuntu{20,22,24}-04`) are
built from the Dockerfiles in this directory by CI, not by hand.
`build_image.py` refuses `--tag=latest` for them.

To ship a new image:

1. Dispatch the internal `build-rbe-image` workflow with inputs `image` (which
   RBE image to build) and `ref` (the buildbuddy ref to build from). It builds
   and pushes a new digest without touching `:latest`.
2. Read the run summary: the dpkg diff against the currently pinned image, the
   smoke check results (`rbe_image_check.sh`), and the remote build results.
3. Open a PR that pins the new digest in `server/util/platform/platform.go`.
4. Ship it in a release and let it soak.
5. Only then move `:latest`, by hand, recording the old digest first so it can
   be rolled back:

   ```sh
   gcloud container images describe <image>:latest   # note the old digest
   gcloud container images add-tag <image>@<digest> <image>:latest
   ```

To check an image locally:

```sh
dockerfiles/rbe_image_check.sh [--family=22.04] <image>   # tools, https, git
dockerfiles/rbe_image_check.sh --packages <image>          # sorted dpkg list
```
