load("@npm//@bazel/typescript:index.bzl", "ts_project")

def ts_library(**kwargs):
    ts_project(
        tsconfig = "//:tsconfig",
        composite = True,
        **kwargs
    )
