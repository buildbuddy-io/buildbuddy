"""Pinned OCI images and generated ext4 images used by Firecracker tests."""

BUSYBOX_IMAGE = "mirror.gcr.io/library/busybox@sha256:dc2d74b28e4cf8984fa52af1f39bc7c3d9c73760b41a74d629f5d11b1ab28616"
DOCKER_HUB_BUSYBOX_IMAGE = "docker.io/library/busybox@sha256:dc2d74b28e4cf8984fa52af1f39bc7c3d9c73760b41a74d629f5d11b1ab28616"
UBUNTU_20_04_IMAGE = "mirror.gcr.io/library/ubuntu@sha256:8feb4d8ca5354def3d8fce243717141ce31e2c428701f6682bd2fafe15388214"
DOCKER_ENABLED_IMAGE = "gcr.io/flame-public/executor-docker-default@sha256:c346c46f3333d7d7f102c6e19a36bcf8403de465ad3cec91c2265a4d56987124"

# TestPrebuiltImagesMatchPlatformImages verifies the Ubuntu 24.04 and workflows
# references stay synchronized with server/util/platform/platform.go.
UBUNTU_24_04_IMAGE = "gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"
DOCKER_DIND_IMAGE = "gcr.io/flame-public/test-docker-dind@sha256:68f6d9ab84623d1116c5432a3b924a07ee09960e6129ca1cb03ef14010588cb4"
WORKFLOWS_IMAGE = "gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:ba28945426fcdf4310f18e8a8e3c47af670bdcf9ba76bd76b269898c0579089e"

def _ext4_image(name, image):
    if "@sha256:" not in image:
        fail("Firecracker test image must use a pinned sha256 digest: %s" % image)
    native.genrule(
        name = name,
        outs = [name + ".ext4"],
        cmd = "$(location //enterprise/server/remote_execution/containers/firecracker/testdata/generate_ext4_image) --executor.exclude_root_device_nodes=true --image '%s' --output '$@'" % image,
        exec_compatible_with = ["@platforms//os:linux"],
        exec_properties = {
            # Network access is needed to pull OCI images.
            "dockerNetwork": "bridge",
            # More CPU resources help speed up image conversion.
            "EstimatedComputeUnits": "10",
        },
        target_compatible_with = ["@platforms//os:linux"],
        tools = ["//enterprise/server/remote_execution/containers/firecracker/testdata/generate_ext4_image"],
    )

def firecracker_test_images():
    """Generates the ext4 root filesystems used by firecracker_test."""
    _ext4_image("busybox", BUSYBOX_IMAGE)
    _ext4_image("ubuntu_20_04", UBUNTU_20_04_IMAGE)
    _ext4_image("docker_enabled", DOCKER_ENABLED_IMAGE)
    _ext4_image("ubuntu_24_04", UBUNTU_24_04_IMAGE)
    _ext4_image("docker_dind", DOCKER_DIND_IMAGE)
    _ext4_image("workflows", WORKFLOWS_IMAGE)
