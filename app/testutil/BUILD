load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "react",
    testonly = True,
    srcs = ["react.ts"],
    deps = [
        "//:node_modules/@testing-library/react",
        "//:node_modules/@types/jasmine",
        "//:node_modules/@types/jsdom",
        "//:node_modules/global-jsdom",
        "//:node_modules/tslib",
    ],
)

ts_jasmine_node_test(
    name = "example_test",
    srcs = ["example_test.tsx"],
    data = [
        "//:node_modules/global-jsdom",
        "//:node_modules/jsdom",
    ],
    esbuild_external = [
        "canvas",
        "global-jsdom",
        "jsdom",
    ],
    deps = [
        ":react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/service:rpc_service",
        "//app/util:async",
        "//proto:user_id_ts_proto",
        "//proto:user_ts_proto",
    ],
)
