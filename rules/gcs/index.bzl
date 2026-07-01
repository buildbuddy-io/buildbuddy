load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load("@bazel_skylib//lib:shell.bzl", "shell")
load("@bazel_skylib//rules:write_file.bzl", "write_file")

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

    # Add the given sha to the prefix if provided.
    if sha_prefix:
        prefix += "$$(cat $(location %s))/" % sha_prefix
        srcs = srcs + [sha_prefix]

    # Zip the files if requested.
    copy_options = "-r"
    if zip:
        copy_options += " -Z"

    util_options = ""
    if disable_caching:
        util_options += " -h 'Cache-Control:no-store'"

    # Generate an .apply rule for uploading.
    write_file(
        name = name + ".apply.script",
        out = name + ".apply.out",
        content = [
            "unset -v PYTHONSAFEPATH; %s -m %s cp %s ${@:1:$#-1} gs://%s/${!#}" % (
                gsutil,
                util_options,
                copy_options,
                bucket,
            ),
        ],
        is_executable = True,
        **kwargs,
    )

    sh_binary(
        name = name + ".apply",
        args = [ "$(rlocationpaths %s)" % src for src in srcs ] + [ prefix ],
        srcs = [ name + ".apply.script" ],
        data = srcs,
        use_bash_launcher = True,
        **kwargs,
    )

    # Generate a .diff rule for diffing.
    write_file(
        name = name + ".diff.script",
        out = name + ".diff.out",
        content = [
            "echo 'Diff not yet implemented for gcs uploads.'",
        ],
        is_executable = True,
        **kwargs,
    )

    sh_binary(
        name = name + ".diff",
        srcs = [ name + ".diff.script" ],
        **kwargs,
    )

    # Generate a .delete rule for deleting.
    write_file(
        name = name + ".delete.script",
        out = name + ".delete.out",
        content = [
            "unset -v PYTHONSAFEPATH; %s -m rm -r gs://%s/%s" % (
                gsutil,
                bucket,
                prefix,
            ),
        ],
        is_executable = True,
        **kwargs
    )

    sh_binary(
        name = name + ".delete",
        srcs = [ name + ".delete.script" ],
        data = srcs,
        use_bash_launcher = True,
        **kwargs,
    )
