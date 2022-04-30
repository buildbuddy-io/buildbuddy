load("@npm//@bazel/typescript:index.bzl", "ts_project")
load("@npm//@bazel/esbuild:index.bzl", "esbuild")
load("@build_bazel_rules_nodejs//internal/common:copy_to_bin.bzl", "copy_to_bin")

def ts_library(name, srcs, **kwargs):
    # Copy TS sources to the bazel bin dir since TypeScript doesn't do well with
    # source trees at multiple roots, especially in multi-bazel-workspace
    # scenarios.
    # See the notes here:
    # https://github.com/aspect-build/rules_js/blob/43936ea96e085c0d6dc0f57403b0fb395d75f4f4/README.md?plain=1#L86-L100
    # This also lets us avoid having to list out every possible bazel-out/{config}/bin
    # dir in our tsconfig.json, as suggested here:
    # https://bazelbuild.github.io/rules_nodejs/TypeScript.html#ts_project
    copy_to_bin(
        name = "%s_binsrcs" % name,
        srcs = srcs,
        visibility = ["//visibility:private"],
    )
    ts_project(
        name = name,
        tsconfig = "//:tsconfig",
        composite = True,
        validate = False,
        srcs = [":%s_binsrcs" % name],
        **kwargs
    )
