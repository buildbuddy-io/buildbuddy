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
def gcs(name, srcs, bucket, gsutil = "gsutil", prefix = "", sha_prefix = "", zip = True, **kwargs):
    # Apply a trailing slash to the prefix if not present.
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"

    # Add the given sha to the prefix if provided.
    if sha_prefix:
        prefix += "$$(cat $(location %s))/" % sha_prefix
        srcs = srcs + [sha_prefix]

    # Zip the files if requested.
    options = "-r"
    if zip:
        options += " -Z"

    # Generate an .apply rule for uploading.
    native.genrule(
        name = name + ".apply",
        srcs = srcs,
        outs = [name + ".apply.out"],
        cmd = "echo \"%s -m cp %s $(SRCS) gs://%s/%s\" > $@" % (gsutil, options, bucket, prefix),
        local = 1,
        executable = 1,
        **kwargs
    )

    # Generate a .diff rule for diffing.
    native.genrule(
        name = name + ".diff",
        srcs = srcs,
        outs = [name + ".diff.out"],
        cmd = "echo \"echo 'Diff not yet implemented for gcs uploads.'\" > $@",
        local = 1,
        executable = 1,
        **kwargs
    )

    # Generate a .delete rule for deleting.
    native.genrule(
        name = name + ".delete",
        srcs = srcs,
        outs = [name + ".delete.out"],
        cmd = "echo \"%s -m rm -r gs://%s/%s\" > $@" % (gsutil, bucket, prefix),
        local = 1,
        executable = 1,
        **kwargs
    )
