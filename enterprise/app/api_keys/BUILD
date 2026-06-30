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
        "//app/service:rpc_service",
        "//app/util:clipboard",
        "//app/util:errors",
        "//proto:api_key_ts_proto",
        "//proto:capability_ts_proto",
    ],
)

ts_jasmine_node_test(
    name = "api_keys_test",
    srcs = ["api_keys_test.tsx"],
    data = [
        "//:node_modules/global-jsdom",
        "//:node_modules/jsdom",
    ],
    esbuild_external = [
        # jsdom probes for the optional canvas peer dependency at runtime.
        # We do not install it for browser component tests.
        "canvas",
        # Keep jsdom out of the esbuild bundle so it can load its Node
        # builtins and dynamic helper files from runfiles at test time.
        "global-jsdom",
        "jsdom",
    ],
    deps = [
        ":api_keys",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/service:rpc_service",
        "//app/testutil:react",
        "//app/util:async",
        "//proto:api_key_ts_proto",
        "//proto:capability_ts_proto",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
    ],
)
