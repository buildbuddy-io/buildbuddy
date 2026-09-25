package(default_visibility = ["//visibility:public"])

# Native Mac coverage for the executor's core lifecycle and filesystem paths.
# Keep this list explicit: server, CLI, and Linux-container integration suites
# otherwise start hundreds of unrelated processes on the shared Mac hosts.
test_suite(
    name = "darwin_tests",
    tests = [
        "//enterprise/server/remote_execution/container:container_test",
        "//enterprise/server/remote_execution/containers/bare:bare_test",
        "//enterprise/server/remote_execution/executor:executor_test",
        "//enterprise/server/remote_execution/executorplatform:executorplatform_test",
        "//enterprise/server/remote_execution/filecache:filecache_test",
        "//enterprise/server/remote_execution/operation:operation_test",
        "//enterprise/server/remote_execution/persistentworker:persistentworker_test",
        "//enterprise/server/remote_execution/workspace:workspace_test",
        "//server/testutil/testport:testport_test",
        "//server/util/disk:disk_test",
    ],
)
