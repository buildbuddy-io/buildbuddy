load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load(":index.bzl", "gcs")

write_file(
    name = "test_sha",
    out = "test.sha",
    content = ["abc123"],
)

write_file(
    name = "test_payload",
    out = "payload.txt",
    content = ["bundle contents"],
)

gcs(
    name = "test_bundle",
    testonly = True,
    srcs = [":test_payload"],
    bucket = "test-bucket",
    disable_caching = True,
    prefix = "release",
    sha_prefix = ":test_sha",
    tags = ["manual"],
)

gcs(
    name = "test_unversioned",
    testonly = True,
    srcs = [":test_payload"],
    bucket = "test-bucket",
    prefix = "mutable/",
    tags = ["manual"],
)

# This action must never run when building the predicate. A direct dependency
# on srcs or .push_only makes artifacts_exist_test fail at build time.
genrule(
    name = "poison_payload",
    testonly = True,
    outs = ["poison.txt"],
    cmd = "echo >&2 'The artifacts predicate built its payload'; exit 1",
    tags = ["manual"],
)

gcs(
    name = "test_poison",
    testonly = True,
    srcs = [":poison_payload"],
    bucket = "test-bucket",
    prefix = "release",
    sha_prefix = ":test_sha",
    tags = ["manual"],
)

sh_test(
    name = "artifacts_exist_test",
    srcs = ["artifacts_exist_test.sh"],
    data = [
        "mock_gsutil.sh",
        ":test_bundle.artifacts_exist",
        ":test_bundle.delete",
        ":test_bundle.push_only",
        ":test_payload",
        ":test_poison.artifacts_exist",
        ":test_sha",
        ":test_unversioned.artifacts_exist",
        ":test_unversioned.delete",
        ":test_unversioned.push_only",
    ],
    env = {
        "PUSH": "$(location :test_bundle.push_only)",
        "CHECK": "$(location :test_bundle.artifacts_exist)",
        "DELETE": "$(location :test_bundle.delete)",
        "POISON_CHECK": "$(location :test_poison.artifacts_exist)",
        "UNVERSIONED_PUSH": "$(location :test_unversioned.push_only)",
        "UNVERSIONED_CHECK": "$(location :test_unversioned.artifacts_exist)",
        "UNVERSIONED_DELETE": "$(location :test_unversioned.delete)",
        "SHA_FILE": "$(location :test_sha)",
        "PAYLOAD": "$(location :test_payload)",
        "MOCK_GSUTIL": "$(location mock_gsutil.sh)",
    },
)
