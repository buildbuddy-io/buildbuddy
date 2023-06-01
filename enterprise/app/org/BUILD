load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "create_org",
    srcs = ["create_org.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/banner",
        "//app/components/button",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/org:org_form",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "edit_org",
    srcs = ["edit_org.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/service:rpc_service",
        "//enterprise/app/org:org_form",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "join_org",
    srcs = ["join_org.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "org_form",
    srcs = ["org_form.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/select",
        "//app/util:errors",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "org_join_requests",
    srcs = ["org_join_requests.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "org_members",
    srcs = ["org_members.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/components/button:checkbox_button",
        "//app/components/checkbox",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)
