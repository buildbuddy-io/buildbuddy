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

    # Generate a .push_only rule for uploading.
    write_file(
        name = name + ".push_only.script",
        out = name + ".push_only.out",
        content = [
            "set -e",
            "unset -v PYTHONSAFEPATH",
            "read SHA_PREFIX < \"${1}\" && export SHA_PREFIX=\"${SHA_PREFIX}/\"",
            "shift",
            "{gsutil} {util_options} cp {copy_options} \"${{@}}\" \"gs://{bucket}/{prefix}${{SHA_PREFIX}}\"".format(
                gsutil = gsutil,
                util_options = util_options,
                copy_options = copy_options,
                bucket = bucket,
                prefix = prefix,
            ),
        ],
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

    # gcs has no apply_only step; it just pushes.
    native.alias(
        name = name + ".apply",
        actual = ":" + name + ".push_only",
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
            "set -e",
            "unset -v PYTHONSAFEPATH",
            "read SHA_PREFIX < \"${1}\" && export SHA_PREFIX=\"${SHA_PREFIX}/\"",
            "{gsutil} -m rm -r gs://{bucket}/{prefix}${{SHA_PREFIX}}".format(
                gsutil = gsutil,
                bucket = bucket,
                prefix = prefix,
            ),
        ],
        is_executable = True,
        **kwargs
    )

    sh_binary(
        name = name + ".delete",
        args = ["../$(rlocationpath %s)" % sha_prefix if sha_prefix != "" else ""],
        srcs = [":" + name + ".delete.script"],
        data = [sha_prefix] if sha_prefix != "" else [],
        use_bash_launcher = True,
        **kwargs
    )
