# guest_kernel_builder

Image with dependencies required by `enterprise/vmsupport/kernel/rebuild.sh` for
building Firecracker guest kernels.

To build and push a multi-arch image with `buildx`:

```bash
./enterprise/tools/build_images/build_image.py \
  --registry=gcr.io \
  --repository=flame-public/guest-kernel-builder \
  --tag=latest \
  --no-suffix \
  --dockerfile=enterprise/dockerfiles/guest_kernel_builder/Dockerfile \
  --platform=amd64 \
  --platform=arm64
```

After pushing a new image, update the pinned `GUEST_KERNEL_BUILDER_IMAGE`
digest in `enterprise/vmsupport/kernel/defs.bzl` to match the current image
SHA.
