load("@aspect_rules_esbuild//esbuild:defs.bzl", "esbuild")
load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

esbuild(
    name = "app_bundle",
    # Causes splitting to be very slow and fail on unused references to "crypto" and "fs".
    # https://github.com/aspect-build/rules_esbuild/issues/190
    bazel_sandbox_plugin = False,
    bundle = True,
    config = {
        "loader": {
            ".ttf": "binary",
            ".css": "binary",
        },
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
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//:node_modules/uplot",
        "//enterprise/tools/databuddy/app:model",
        "//enterprise/tools/databuddy/app/components:Plot",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
    ],
)

ts_library(
    name = "Home",
    srcs = ["Home.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//enterprise/tools/databuddy/app:initial_data",
        "//enterprise/tools/databuddy/app:rpc",
        "//enterprise/tools/databuddy/app:toast",
        "//enterprise/tools/databuddy/app/components:Panel",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
    ],
)

ts_library(
    name = "Query",
    srcs = ["Query.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/@types/uuid",
        "//:node_modules/csv-stringify",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/react-router5",
        "//:node_modules/tslib",
        "//:node_modules/uuid",
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
    ],
)

ts_library(
    name = "Table",
    srcs = ["Table.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/util:dom",
    ],
)

ts_library(
    name = "index",
    srcs = ["index.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-dom",
        "//:node_modules/react",
        "//:node_modules/react-dom",
        "//:node_modules/react-router5",
        "//enterprise/tools/databuddy/app:Home",
        "//enterprise/tools/databuddy/app:Query",
        "//enterprise/tools/databuddy/app:router",
        "//enterprise/tools/databuddy/app:toast",
    ],
)

ts_library(
    name = "monaco_config",
    srcs = ["monaco_config.tsx"],
    deps = [
        "//:node_modules/monaco-editor",
        "//:node_modules/tslib",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
    ],
)

ts_library(
    name = "router",
    srcs = ["router.tsx"],
    deps = [
        "//:node_modules/router5",
        "//:node_modules/router5-plugin-browser",
    ],
)

ts_library(
    name = "toast",
    srcs = ["toast.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//enterprise/tools/databuddy/app/components:Spinner",
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
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "rpc",
    srcs = ["rpc.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/browser-headers",
        "//:node_modules/protobufjs",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/util:async",
        "//app/util:errors",
        "//enterprise/tools/databuddy/proto:databuddy_ts_proto",
        "//proto:grpc_code_ts_proto",
        "//proto:grpc_status_ts_proto",
    ],
)
