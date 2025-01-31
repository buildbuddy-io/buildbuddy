load("@npm//@bazel/esbuild:index.bzl", "esbuild")
load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

esbuild(
    name = "app_bundle",
    args = {
        "loader": {
            ".ttf": "binary",
            ".css": "binary",
        },
        "bundle": True,
    },
    entry_points = ["index.tsx"],
    metafile = False,
    minify = select({
        "//:fastbuild": False,
        "//conditions:default": True,
    }),
    deps = [":index"],
)

ts_library(
    name = "ChartGrid",
    srcs = ["ChartGrid.tsx"],
    deps = [
        "//enterprise/tools/databuddy/app:model",
        "//enterprise/tools/databuddy/app/components:Plot",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//react",
        "@npm//tslib",
        "@npm//uplot",
    ],
)

ts_library(
    name = "Home",
    srcs = ["Home.tsx"],
    deps = [
        "//enterprise/tools/databuddy/app:initial_data",
        "//enterprise/tools/databuddy/app:rpc",
        "//enterprise/tools/databuddy/app:toast",
        "//enterprise/tools/databuddy/app/components:Panel",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "Query",
    srcs = ["Query.tsx"],
    deps = [
        "//app/util:sort",
        "//enterprise/tools/databuddy/app:ChartGrid",
        "//enterprise/tools/databuddy/app:Table",
        "//enterprise/tools/databuddy/app:initial_data",
        "//enterprise/tools/databuddy/app:model",
        "//enterprise/tools/databuddy/app:router",
        "//enterprise/tools/databuddy/app:rpc",
        "//enterprise/tools/databuddy/app:toast",
        "//enterprise/tools/databuddy/app/components:Editor",
        "//enterprise/tools/databuddy/app/components:Panel",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//@types/uuid",
        "@npm//csv-stringify",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//react-router5",
        "@npm//tslib",
        "@npm//uuid",
    ],
)

ts_library(
    name = "Table",
    srcs = ["Table.tsx"],
    deps = [
        "//app/util:dom",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "index",
    srcs = ["index.tsx"],
    deps = [
        "//enterprise/tools/databuddy/app:Home",
        "//enterprise/tools/databuddy/app:Query",
        "//enterprise/tools/databuddy/app:router",
        "//enterprise/tools/databuddy/app:toast",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//react",
        "@npm//react-dom",
        "@npm//react-router5",
    ],
)

ts_library(
    name = "monaco_config",
    srcs = ["monaco_config.tsx"],
    deps = [
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
        "@npm//monaco-editor",
        "@npm//tslib",
    ],
)

ts_library(
    name = "router",
    srcs = ["router.tsx"],
    deps = [
        "@npm//router5",
        "@npm//router5-plugin-browser",
    ],
)

ts_library(
    name = "toast",
    srcs = ["toast.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "model",
    srcs = ["model.tsx"],
    deps = ["//enterprise/tools/databuddy/proto:databuddy_ts_proto"],
)

ts_library(
    name = "initial_data",
    srcs = ["initial_data.tsx"],
    deps = ["//enterprise/tools/databuddy/proto:databuddy_ts_proto"],
)

ts_library(
    name = "storage",
    srcs = ["storage.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "rpc",
    srcs = ["rpc.tsx"],
    deps = [
        "//app/util:async",
        "//app/util:errors",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
        "//proto:grpc_code_ts_proto",
        "//proto:grpc_status_ts_proto",
        "@npm//@types/react",
        "@npm//browser-headers",
        "@npm//protobufjs",
        "@npm//react",
        "@npm//tslib",
    ],
)
