load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "api_keys",
    srcs = ["api_keys.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/auth:certificate_download_link",
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:clipboard",
        "//app/util:errors",
        "//app/util:proto",
        "//app/util:react",
        "//enterprise/app/api_keys:visibility",
        "//proto:api_key_ts_proto",
        "//proto:capability_ts_proto",
    ],
)

ts_library(
    name = "visibility",
    srcs = ["visibility.ts"],
    deps = ["//proto:api_key_ts_proto"],
)

ts_jasmine_node_test(
    name = "visibility_test",
    srcs = ["visibility_test.ts"],
    deps = [
        ":visibility",
        "//proto:api_key_ts_proto",
    ],
)
