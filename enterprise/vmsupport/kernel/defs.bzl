load("@io_bazel_rules_go//go:def.bzl", "go_binary")
load("//server/util/bb:platform_transition.bzl", "linux_arm64_alias", "linux_x86_64_alias")

GUEST_KERNEL_BUILDER_IMAGE = "docker://gcr.io/flame-public/guest-kernel-builder@sha256:0ee1083f4c7ae7443e9dd10b97d423b89a025f1576eafbe34f25f509fec61d80"

def microvm_kernel(
        name,
        arch,
        config_src,
        out,
        kernel_version):
    """Builds a guest kernel and emits matching public alias and upload targets.

    Args:
        name: Public kernel target name.
        arch: Kernel architecture, either "x86_64" or "arm64".
        config_src: Kernel config file.
        out: Output kernel image filename.
        kernel_version: Version string passed to the rebuild script and uploader.
    """
    build_name = name + "_build"
    cpu_constraint = _guest_kernel_cpu_constraint(arch)
    # Build the kernel artifact with a private implementation target so the
    # public name can stay reserved for the platform-aware alias.
    native.genrule(
        name = build_name,
        srcs = [config_src],
        outs = [out],
        cmd_bash = """
set -euo pipefail
tmpdir="$${TMPDIR:-$$PWD/.tmp}"
mkdir -p "$$tmpdir"
log_file="$$(mktemp "$$tmpdir/%s.XXXXXX.log")"
cleanup() {
  rm -f "$$log_file"
}
trap cleanup EXIT
if ! env \\
    TMPDIR="$$tmpdir" \\
    KERNEL_VERSION=%s \\
    SKIP_UPLOAD=1 \\
    OUTPUT_FILE="$@" \\
    $(location rebuild.sh) >"$$log_file" 2>&1; then
  cat "$$log_file" >&2
  exit 1
fi
""" % (build_name, kernel_version),
        exec_compatible_with = [
            cpu_constraint,
            "@platforms//os:linux",
        ],
        exec_properties = {
            "container-image": GUEST_KERNEL_BUILDER_IMAGE,
            "dockerNetwork": "bridge",
            "EstimatedComputeUnits": "16",
        },
        tags = ["manual"],
        target_compatible_with = [
            cpu_constraint,
            "@platforms//os:linux",
        ],
        tools = ["rebuild.sh"],
    )

    # Expose the built kernel through a platform-specific alias so callers use
    # a single public target name.
    alias_rule = _guest_kernel_alias(arch)
    alias_rule(
        name = name,
        actual = ":%s" % build_name,
        tags = ["manual"],
    )

    # Provide a runnable sibling target that builds this kernel and uploads just
    # its artifact with the matching arch/version metadata.
    go_binary(
        name = name + "_upload",
        args = [
            "--kernel_arch=%s" % _guest_kernel_upload_arch(arch),
            "--kernel_version=%s" % kernel_version,
            "--kernel_path=$(rlocationpath :%s)" % name,
        ],
        data = [":%s" % name],
        embed = ["//enterprise/vmsupport/kernel/upload:upload_lib"],
        tags = ["manual"],
    )

def _guest_kernel_alias(arch):
    if arch == "x86_64":
        return linux_x86_64_alias
    if arch == "arm64":
        return linux_arm64_alias
    fail("unsupported guest kernel arch: %s" % arch)

def _guest_kernel_cpu_constraint(arch):
    if arch == "x86_64":
        return "@platforms//cpu:x86_64"
    if arch == "arm64":
        return "@platforms//cpu:arm64"
    fail("unsupported guest kernel arch: %s" % arch)

def _guest_kernel_upload_arch(arch):
    if arch == "x86_64":
        return "x86_64"
    if arch == "arm64":
        return "aarch64"
    fail("unsupported guest kernel arch: %s" % arch)
