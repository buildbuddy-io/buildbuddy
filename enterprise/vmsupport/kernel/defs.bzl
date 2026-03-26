load("@io_bazel_rules_go//go:def.bzl", "go_binary")
load("//server/util/bb:platform_transition.bzl", "linux_arm64_alias", "linux_x86_64_alias")

GUEST_KERNEL_BUILDER_IMAGE = "docker://gcr.io/flame-public/guest-kernel-builder@sha256:5f6062d843752480f1c619f64af4a0311eff994cb640053b1e3fc1b2fcea88b3"

def microvm_kernel(
        name,
        arch,
        config,
        out,
        version):
    """Builds a guest kernel and emits matching public alias and upload targets.

    Args:
        name: Public kernel target name.
        arch: Kernel architecture, either "x86_64" or "arm64".
        config: Kernel config file.
        out: Output kernel image filename.
        version: Version string passed to the rebuild script and uploader.
    """
    if arch == "x86_64":
        cpu_constraint = "@platforms//cpu:x86_64"
        platform_transition_alias = linux_x86_64_alias
        upload_arch = "x86_64"
    elif arch == "arm64":
        cpu_constraint = "@platforms//cpu:arm64"
        platform_transition_alias = linux_arm64_alias
        upload_arch = "aarch64"
    else:
        fail("unsupported guest kernel arch: %s" % arch)

    # Build the kernel artifact with a private implementation target so the
    # public name can stay reserved for the platform-aware alias.
    native.genrule(
        name = name + "_build",
        srcs = [config],
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
""" % (name + "_build", version),
        exec_compatible_with = [
            cpu_constraint,
            "@platforms//os:linux",
        ],
        exec_properties = {
            "container-image": GUEST_KERNEL_BUILDER_IMAGE,
            "network": "external",
            # Building the kernel is resource-intensive; request lots of CPU.
            "EstimatedComputeUnits": "16",
        },
        tags = ["manual"],
        target_compatible_with = [
            cpu_constraint,
            "@platforms//os:linux",
        ],
        tools = ["rebuild.sh"],
    )

    # Use a transition to ensure the target platform for the genrule matches the
    # kernel architecture (and then the `exec_compatible_with` on the genrule
    # will ensure that the execution platform has matching constraints). This
    # platform specific target is the one exposed as <name>.
    platform_transition_alias(
        name = name,
        actual = ":%s_build" % name,
        tags = ["manual"],
    )

    # Provide a runnable sibling target that builds this kernel and uploads just
    # its artifact with the matching arch/version metadata.
    #
    # Note that if --platforms is set, then it has to match the host platform so
    # that this tool will be able to run locally (under `bazel run`).
    go_binary(
        name = name + "_upload",
        args = [
            "--kernel_arch=%s" % upload_arch,
            "--kernel_version=%s" % version,
            "--kernel_path=$(rlocationpath :%s)" % name,
        ],
        data = [":%s" % name],
        embed = ["//enterprise/vmsupport/kernel/upload:upload_lib"],
        tags = ["manual"],
    )
