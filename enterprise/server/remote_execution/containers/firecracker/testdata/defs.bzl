"""Builds Firecracker test images from pinned OCI images."""

# Update the pinned OCI digests below to rebuild the corresponding test images.

BUSYBOX_IMAGE = "mirror.gcr.io/library/busybox@sha256:dc2d74b28e4cf8984fa52af1f39bc7c3d9c73760b41a74d629f5d11b1ab28616"

DOCKER_HUB_BUSYBOX_IMAGE = BUSYBOX_IMAGE.replace("mirror.gcr.io/", "docker.io/")

UBUNTU_20_04_IMAGE = "mirror.gcr.io/library/ubuntu@sha256:8feb4d8ca5354def3d8fce243717141ce31e2c428701f6682bd2fafe15388214"

DOCKER_ENABLED_IMAGE = "gcr.io/flame-public/executor-docker-default@sha256:c346c46f3333d7d7f102c6e19a36bcf8403de465ad3cec91c2265a4d56987124"

DOCKER_DIND_IMAGE = "gcr.io/flame-public/test-docker-dind@sha256:68f6d9ab84623d1116c5432a3b924a07ee09960e6129ca1cb03ef14010588cb4"

UBUNTU_24_04_IMAGE = "gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"

WORKFLOWS_IMAGE = "gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:ba28945426fcdf4310f18e8a8e3c47af670bdcf9ba76bd76b269898c0579089e"

TEST_IMAGES = {
    "busybox": (BUSYBOX_IMAGE, ["x86_64", "arm64"]),
    "docker_dind": (DOCKER_DIND_IMAGE, ["x86_64", "arm64"]),
    "docker_enabled": (DOCKER_ENABLED_IMAGE, ["x86_64"]),
    "ubuntu_20_04": (UBUNTU_20_04_IMAGE, ["x86_64", "arm64"]),
    "ubuntu_24_04": (UBUNTU_24_04_IMAGE, ["x86_64", "arm64"]),
    "workflows": (WORKFLOWS_IMAGE, ["x86_64"]),
}

# To get deterministic EXT4 images, use e2fsprogs >=1.47.2 (via Ubuntu >=26.04),
# which supports clamping timestamps using the SOURCE_DATE_EPOCH env var.
_BUILDER_IMAGE = "docker://mirror.gcr.io/library/ubuntu@sha256:da6fc2be547864451aa253836dd926da33623312df4a9a243e35dc877c378a78"

def _ext4_image(name, image, arch):
    if "@sha256:" not in image:
        fail("Firecracker test image must use a pinned sha256 digest: %s" % image)
    native.genrule(
        name = name,
        testonly = True,
        srcs = ["@se_curl_cacert//file"],
        outs = [name + ".ext4"],
        cmd = """
            # Provision certs manually since the stock Ubuntu 26.04 image
            # doesn't have certs
            SSL_CERT_FILE=$(location @se_curl_cacert//file) \\
            $(location //enterprise/server/remote_execution/containers/firecracker/testdata/generate_ext4_image) \\
                --app.log_level=warn \\
                --executor.exclude_root_device_nodes=true \\
                --executor.reproducible_ext4_images=true \\
                --image '%s' \\
                --arch %s \\
                --output '$@'
        """ % (image, arch),
        exec_compatible_with = ["@platforms//os:linux"],
        exec_properties = {
            "container-image": _BUILDER_IMAGE,
            "dockerUser": "0:0",
            # Network access is needed to pull OCI images.
            "dockerNetwork": "bridge",
            # More CPU resources help speed up image conversion.
            "EstimatedComputeUnits": "10",
        },
        tags = ["manual"],
        target_compatible_with = ["@platforms//os:linux"],
        tools = ["//enterprise/server/remote_execution/containers/firecracker/testdata/generate_ext4_image"],
    )

def firecracker_test_images():
    """Defines conversion actions and test image aliases for each supported arch."""
    for name, (image, arches) in TEST_IMAGES.items():
        aliases = {}
        for arch in arches:
            build_name = "%s_%s_build" % (name, arch)
            _ext4_image(build_name, image, arch)
            aliases["@platforms//cpu:" + arch] = ":" + build_name
        native.alias(
            name = name + ".ext4",
            actual = select(aliases),
            tags = ["manual"],
        )
