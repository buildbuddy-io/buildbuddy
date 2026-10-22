# RBE images

The default RBE images (`gcr.io/flame-public/rbe-ubuntu{20,22,24}-04`) are
built from the Dockerfiles in this directory by CI, not by hand.
`build_image.py` refuses `--push` for them; it can still build them locally
with `--no-push`.

To ship a new image:

1. Dispatch the internal `build-rbe-image` workflow with inputs `image` (which
   RBE image to build) and `ref` (the buildbuddy ref to build from). It builds
   and pushes each arch untagged by digest, runs the checks on BuildBuddy RBE
   against those digests, and only if they pass gives the image a new, unique
   tag (`<YYYYMMDD>-<sha7>-<run_id>-<run_attempt>`). It never touches
   `:latest` or any existing tag.
2. Read the run summary: the dpkg diff against the currently pinned image and
   the smoke check results (`rbe_image_check.sh`, run on RBE).
3. Open a PR that pins the new digest in `server/util/platform/platform.go`.
4. Ship it in a release and let it soak.
5. Only then move `:latest`, by hand, recording the old digest first so it can
   be rolled back:

   ```sh
   gcloud container images describe <image>:latest   # note the old digest
   gcloud container images add-tag <image>@<digest> <image>:latest
   ```

`rbe_image_check.sh` runs inside the image it checks; the workflow runs it as
an action on BuildBuddy RBE. To check an image locally:

```sh
docker run --rm -v "$PWD/dockerfiles:/c:ro" <image> /c/rbe_image_check.sh             # tools, https, git
docker run --rm -v "$PWD/dockerfiles:/c:ro" <image> /c/rbe_image_check.sh --packages  # sorted dpkg list
```
