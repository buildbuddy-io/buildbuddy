load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

# Handles uploading files to GCS.
#
# Example usage:
# ```
#   gcs(
#       name = "app_bundle_release",
#       srcs = ["//enterprise/app:app_bundle"],
#       bucket = "buildbuddy-static",
#       prefix = "release",
#       sha_prefix = "//enterprise/app:sha",
#   )
# ```
#
# Then to upload the files to GCS, run:
#   `bazel run :app_bundle_release.apply`
#
# In order to delete the files from GCS, run:
#   `bazel run :app_bundle_release.delete`
#
# Returns 0 only if a content-addressed upload has its completion marker:
#   `bazel run :app_bundle_release.artifacts_exist`
# Older uploads without a marker must be uploaded again. Unversioned uploads
# cannot be confirmed: a marker could describe stale, mutable contents.
# `_SUCCESS` is reserved for the completion marker. sha_prefix must identify
# immutable contents (the existing bundle-content hash), not a mutable alias.
#
# Read the entire hash, accepting files with or without a trailing newline and
# rejecting empty/invalid identities before contacting GCS.
_READ_SHA_PREFIX = [
    'SHA_PREFIX="$(cat -- "${1:?Missing sha_prefix file}")"',
    'case "$SHA_PREFIX" in ""|*[!a-zA-Z0-9_-]*) echo >&2 "Invalid or empty sha_prefix"; exit 1;; esac',
    'SHA_PREFIX="${SHA_PREFIX}/"',
]

def gcs(name, srcs, bucket, gsutil = "gsutil", prefix = "", sha_prefix = "", zip = True, disable_caching = False, **kwargs):
    # Apply a trailing slash to the prefix if not present.
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"

    # Zip the files if requested.
    copy_options = "-r"
    if zip:
        copy_options += " -Z"

    util_options = "-m"
    if disable_caching:
        util_options += " -h 'Cache-Control:no-store'"

    read_sha_prefix = _READ_SHA_PREFIX if sha_prefix else ['SHA_PREFIX=""']
    destination = '"gs://{bucket}/{prefix}${{SHA_PREFIX}}"'.format(bucket = bucket, prefix = prefix)
    marker = '"gs://{bucket}/{prefix}${{SHA_PREFIX}}_SUCCESS"'.format(bucket = bucket, prefix = prefix)

    # Only immutable, versioned uploads have a reusable completion marker.
    # A failed initial/partial upload never publishes a marker. A marker from
    # an earlier successful upload of the same hash remains valid on retries.
    publish_marker = [
        'MARKER="$(mktemp)"',
        "trap 'rm -f -- \"$MARKER\"' EXIT",
        '{gsutil} {util_options} cp "$MARKER" {marker}'.format(
            gsutil = gsutil,
            util_options = util_options,
            marker = marker,
        ),
    ] if sha_prefix else []

    # Generate a .push_only rule for uploading.
    write_file(
        name = name + ".push_only.script",
        out = name + ".push_only.out",
        content = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "unset -v PYTHONSAFEPATH",
        ] + read_sha_prefix + [
            "shift",
            'for src in "$@"; do if [[ "${src##*/}" == _SUCCESS ]]; then echo >&2 "_SUCCESS is reserved"; exit 1; fi; done',
            "{gsutil} {util_options} cp {copy_options} \"${{@}}\" {destination}".format(
                gsutil = gsutil,
                util_options = util_options,
                copy_options = copy_options,
                destination = destination,
            ),
        ] + publish_marker,
        is_executable = True,
        **kwargs
    )

    to_copy = ["../$(rlocationpath {})".format(src) for src in srcs]
    if sha_prefix != "":
        sha_prefix_location = "../$(rlocationpath {})".format(sha_prefix)

        # copy the sha_prefix file if it exists.
        to_copy.append(sha_prefix_location)
    else:
        sha_prefix_location = "/dev/null"

    sh_binary(
        name = name + ".push_only",
        # the first argument is where to read the sha_prefix from.
        args = [sha_prefix_location] + to_copy,
        srcs = [":" + name + ".push_only.script"],
        data = srcs + ([sha_prefix] if sha_prefix != "" else []),
        use_bash_launcher = True,
        **kwargs
    )

    # Checking does not depend directly on srcs or .push_only. Computing the
    # existing content hash can still build the bundle transitively.
    write_file(
        name = name + ".artifacts_exist.script",
        out = name + ".artifacts_exist.out",
        content = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "unset -v PYTHONSAFEPATH",
        ] + (_READ_SHA_PREFIX + [
            "exec {gsutil} stat {marker}".format(gsutil = gsutil, marker = marker),
        ] if sha_prefix else [
            'echo >&2 "Cannot confirm unversioned GCS artifacts without sha_prefix"',
            "exit 1",
        ]),
        is_executable = True,
        **kwargs
    )

    sh_binary(
        name = name + ".artifacts_exist",
        args = [sha_prefix_location] if sha_prefix else [],
        srcs = [":" + name + ".artifacts_exist.script"],
        data = [sha_prefix] if sha_prefix else [],
        use_bash_launcher = True,
        **kwargs
    )

    # gcs has no apply_only step; it just pushes.
    native.alias(
        name = name + ".apply",
        actual = ":" + name + ".push_only",
        tags = kwargs.get("tags", []),
    )

    # Uploading is the only deployment operation for a GCS bundle, so there
    # is nothing left to do during the apply-only phase.
    write_file(
        name = name + ".apply_only.script",
        out = name + ".apply_only.out",
        content = [
            "true",
        ],
        is_executable = True,
        **kwargs
    )

    sh_binary(
        name = name + ".apply_only",
        srcs = [
            ":" + name + ".apply_only.script",
        ],
        use_bash_launcher = True,
        **kwargs
    )

    # Generate a .diff rule for diffing.
    write_file(
        name = name + ".diff.script",
        out = name + ".diff.out",
        content = [
            "echo 'Diff not yet implemented for gcs uploads.'",
        ],
        is_executable = True,
        **kwargs
    )

    sh_binary(
        name = name + ".diff",
        srcs = [":" + name + ".diff.script"],
        use_bash_launcher = True,
        **kwargs
    )

    # Generate a .delete rule for deleting.
    write_file(
        name = name + ".delete.script",
        out = name + ".delete.out",
        content = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "unset -v PYTHONSAFEPATH",
        ] + read_sha_prefix + [
            "{gsutil} -m rm -r {destination}".format(
                gsutil = gsutil,
                destination = destination,
            ),
        ],
        is_executable = True,
        **kwargs
    )

    sh_binary(
        name = name + ".delete",
        args = [sha_prefix_location],
        srcs = [":" + name + ".delete.script"],
        data = [sha_prefix] if sha_prefix != "" else [],
        use_bash_launcher = True,
        **kwargs
    )
