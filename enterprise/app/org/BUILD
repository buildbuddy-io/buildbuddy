load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "create_org",
    srcs = ["create_org.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/banner",
        "//app/components/button",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/org:org_form",
        "//proto:group_ts_proto",
    ],
)

ts_library(
    name = "edit_org",
    srcs = ["edit_org.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/service:rpc_service",
        "//enterprise/app/org:org_form",
        "//proto:group_ts_proto",
    ],
)

ts_library(
    name = "join_org",
    srcs = ["join_org.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:group_ts_proto",
    ],
)

ts_library(
    name = "org_form",
    srcs = ["org_form.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/select",
        "//app/util:errors",
        "//proto:group_ts_proto",
    ],
)

ts_library(
    name = "org_join_requests",
    srcs = ["org_join_requests.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/auth:user",
        "//app/components/button",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
    ],
)

ts_library(
    name = "org_members",
    srcs = ["org_members.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//enterprise/app/org:user_list",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
    ],
)

ts_library(
    name = "org_access_denied",
    srcs = ["org_access_denied.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/auth:user",
        "//app/components/button",
        "//proto:user_ts_proto",
    ],
)

ts_library(
    name = "org_user_lists",
    srcs = ["org_user_lists.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/components/banner",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/org:org_user_list",
        "//enterprise/app/org:user_list",
        "//proto:user_list_ts_proto",
    ],
)

ts_library(
    name = "org_user_list",
    srcs = ["org_user_list.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/alert:alert_service",
        "//app/auth:user",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/org:user_list",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
        "//proto:user_list_ts_proto",
    ],
)

ts_library(
    name = "user_list",
    srcs = ["user_list.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/components/banner",
        "//app/components/button:checkbox_button",
        "//app/components/checkbox",
        "//app/icons:github",
        "//app/icons:google",
        "//proto:group_ts_proto",
        "//proto:user_id_ts_proto",
    ],
)
