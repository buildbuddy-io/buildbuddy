load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "workflow_test",
    srcs = ["workflow_test.go"],
    # ci_runner gets invoked directly by this test since the RBE test framework
    # currently does not support dockerized execution of commands. So for now we
    # run the whole test using the CI runner image.
    # TODO(bduffany): Add an ubuntu 20.04-based CI runner image and use that
    # here.
    # exec_properties = {
    #     "container-image": "docker://gcr.io/flame-public/buildbuddy-ci-runner:v2.3.0",
    # },
    exec_properties = {
        # TODO: remove network dependency.
        "test.dockerNetwork": "bridge",
    },
    shard_count = 2,
    deps = [
        "//enterprise/server/invocation_search_service",
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//enterprise/server/workflow/service",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:git_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:user_id_go_proto",
        "//proto:workflow_go_proto",
        "//server/backends/github",
        "//server/backends/repo_downloader",
        "//server/interfaces",
        "//server/testutil/testbazel",
        "//server/testutil/testenv",
        "//server/testutil/testgit",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//codes",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
