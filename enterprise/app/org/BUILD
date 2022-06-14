load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "org",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/alert",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/checkbox",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)
