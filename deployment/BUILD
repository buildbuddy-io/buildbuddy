load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_k8s//k8s:object.bzl", "k8s_object")
load("@io_bazel_rules_k8s//k8s:objects.bzl", "k8s_objects")
load("//rules/flags:index.bzl", "write_flag_to_file")

# TODO(tylerw): break depoloyment.yaml apart.
# TODO(tylerw): do the etsy context thing to set dev/prod vars.
k8s_object(
    name = "buildbuddy_dev_app",
    cluster = "gke_flame-build_us-west1-a_bb-dev-cluster",
    images = {
        "gcr.io/flame-build/buildbuddy-app-dev:server_image": "//server:buildbuddy_image",
    },
    tags = ["manual"],
    template = ":buildbuddy-app.dev.yaml",
)

k8s_objects(
    name = "dev",
    objects = [
        ":buildbuddy_dev_app",
    ],
    tags = ["manual"],
)

# Set --//deployment:image_tag to set the tag when running container_push
# targets. If unset, defaults to "development".
#
# Note that this tag is shared across all targets, which is fine since only one
# container_push target can run at once.
string_flag(
    name = "image_tag",
    build_setting_default = "development",
)

write_flag_to_file(
    name = "image_tag_file",
    out = "image_tag_file",
    flag = ":image_tag",
    visibility = ["//visibility:public"],
)
