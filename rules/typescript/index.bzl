load("@npm//@bazel/concatjs:index.bzl", _ts_library = "ts_library")

# TODO(bduffany): ts_library is semi-deprecated; migrate to ts_project
ts_library = _ts_library
