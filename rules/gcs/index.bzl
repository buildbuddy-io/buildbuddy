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

# Resolve the runfiles root. `bazel run` sets RUNFILES_DIR; rules_python-based
# launchers (e.g. rules_multirun, rules_k8s aggregators) set PYTHON_RUNFILES;
# otherwise fall back to the `<script>.runfiles` directory adjacent to the
# binary, or walk up from `${0}` for the nested-runfiles case.
_RUNFILES_PREAMBLE = """if [[ -z "${RUNFILES_DIR:-}" ]]; then
  if [[ -n "${PYTHON_RUNFILES:-}" ]]; then
    RUNFILES_DIR="${PYTHON_RUNFILES}"
  elif [[ -d "${0}.runfiles" ]]; then
    RUNFILES_DIR="$(cd "${0}.runfiles" && pwd)"
  else
    mydir="$(cd "$(dirname "${0}")" && pwd)"
    RUNFILES_DIR="${mydir%%.runfiles*}.runfiles"
  fi
fi
export RUNFILES_DIR
# gsutil is a Python program that imports bootstrap modules from the directory
# of its entry point; unset PYTHONSAFEPATH in case a rules_python-based
# launcher (e.g. rules_multirun) leaked it into the environment.
unset -v PYTHONSAFEPATH"""

def _runfiles_path(ctx, f):
    if f.short_path.startswith("../"):
        return f.short_path[3:]
    return ctx.workspace_name + "/" + f.short_path

def _gcs_run_impl(ctx):
    lines = ["#!/usr/bin/env bash", "set -euo pipefail"]
    runfiles_files = []
    dest = 'gs://%s/%s' % (ctx.attr.bucket, ctx.attr.prefix)

    if ctx.attr.mode in ("push", "delete"):
        lines.append(_RUNFILES_PREAMBLE)
        if ctx.file.sha_file != None:
            runfiles_files.append(ctx.file.sha_file)
            lines.append('SHA_PREFIX="$(cat "${RUNFILES_DIR}/%s")/"' % _runfiles_path(ctx, ctx.file.sha_file))
        else:
            lines.append('SHA_PREFIX=""')

    if ctx.attr.mode == "push":
        # The sha file, when present, is uploaded alongside the srcs, matching
        # the historical behavior of this rule.
        src_files = []
        for src in ctx.attr.srcs:
            src_files.extend(src[DefaultInfo].files.to_list())
        if ctx.file.sha_file != None:
            src_files.append(ctx.file.sha_file)
        runfiles_files.extend(src_files)

        # Resolve runfiles symlinks so gsutil sees regular files/directories.
        lines.append("SRCS=()")
        for f in src_files:
            lines.append('SRCS+=("$(readlink -f "${RUNFILES_DIR}/%s")")' % _runfiles_path(ctx, f))
        lines.append('exec %s cp %s "${SRCS[@]}" "%s${SHA_PREFIX}"' % (
            ctx.attr.gsutil_with_options,
            ctx.attr.copy_options,
            dest,
        ))
    elif ctx.attr.mode == "delete":
        lines.append('exec %s rm -r "%s${SHA_PREFIX}"' % (ctx.attr.gsutil_with_options, dest))
    elif ctx.attr.mode == "diff":
        lines.append("echo 'Diff not yet implemented for gcs uploads.'")
    elif ctx.attr.mode == "noop":
        lines.append("true")
    else:
        fail("unknown gcs mode: %s" % ctx.attr.mode)

    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(script, "\n".join(lines) + "\n", is_executable = True)
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(files = runfiles_files),
    )]

_gcs_run = rule(
    implementation = _gcs_run_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "sha_file": attr.label(allow_single_file = True),
        "bucket": attr.string(mandatory = True),
        "prefix": attr.string(),
        "gsutil_with_options": attr.string(default = "gsutil -m"),
        "copy_options": attr.string(default = "-r"),
        "mode": attr.string(mandatory = True, values = ["push", "delete", "diff", "noop"]),
    },
    executable = True,
    doc = "Generates an executable script that copies its runfiles to (or " +
          "deletes them from) a GCS bucket with gsutil. Unlike a genrule, the " +
          "srcs are staged as runfiles in the target configuration, so the " +
          "script keeps working when invoked through launchers such as " +
          "rules_multirun that run it outside the execroot.",
)

def gcs(name, srcs, bucket, gsutil = "gsutil", prefix = "", sha_prefix = "", zip = True, disable_caching = False, **kwargs):
    # Apply a trailing slash to the prefix if not present.
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"

    # Zip the files if requested.
    copy_options = "-r"
    if zip:
        copy_options += " -Z"

    gsutil_with_options = gsutil + " -m"
    if disable_caching:
        gsutil_with_options += " -h 'Cache-Control:no-store'"

    sha_file = sha_prefix or None

    # `.apply` and `.push_only` both upload the srcs (plus the sha file) to
    # `gs://<bucket>/<prefix>/<sha>/`. Uploading is the only deployment
    # operation for a GCS bundle, so `.apply_only` has nothing left to do.
    for action in [".apply", ".push_only"]:
        _gcs_run(
            name = name + action,
            srcs = srcs,
            sha_file = sha_file,
            bucket = bucket,
            prefix = prefix,
            gsutil_with_options = gsutil_with_options,
            copy_options = copy_options,
            mode = "push",
            **kwargs
        )

    _gcs_run(
        name = name + ".apply_only",
        bucket = bucket,
        mode = "noop",
        **kwargs
    )

    _gcs_run(
        name = name + ".diff",
        bucket = bucket,
        mode = "diff",
        **kwargs
    )

    _gcs_run(
        name = name + ".delete",
        sha_file = sha_file,
        bucket = bucket,
        prefix = prefix,
        gsutil_with_options = gsutil_with_options,
        mode = "delete",
        **kwargs
    )
