load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["tap.css"])

ts_library(
    name = "tap",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-dom",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/react-dom",
        "//:node_modules/recharts",
        "//:node_modules/rxjs",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/filter_input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:git",
        "//app/util:math",
        "//proto:invocation_ts_proto",
        "//proto:target_ts_proto",
    ],
)
