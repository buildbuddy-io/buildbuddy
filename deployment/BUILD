load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@k8s_deploy//:defaults.bzl", "k8s_deploy")
load("@io_bazel_rules_k8s//k8s:object.bzl", "k8s_object")
load("@io_bazel_rules_k8s//k8s:objects.bzl", "k8s_objects")
load("@io_bazel_rules_docker//container:push.bzl", "container_push")
load("//rules/flags:index.bzl", "write_flag_to_file")

# TODO(tylerw): break depoloyment.yaml apart.
# TODO(tylerw): do the etsy context thing to set dev/prod vars.
k8s_object(
    name = "buildbuddy_dev_app",
    cluster = "gke_flame-build_us-west1-a_bb-dev-cluster",
    images = {
        "gcr.io/flame-build/buildbuddy-app-dev:server_image": "//server:buildbuddy_image",
    },
    template = ":buildbuddy-app.dev.yaml",
)

k8s_objects(
    name = "dev",
    objects = [
        ":buildbuddy_dev_app",
    ],
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
    flag = ":image_tag",
    visibility = ["//visibility:public"],
)

# Definition for uploading new releases of the on_prem docker image.
# Running this? You may want to update the onprem image version configured in
# deployment/buildbuddy-app.onprem.yaml so on-prem users actually get your
# new version!
container_push(
    name = "release_onprem",
    format = "Docker",
    image = "//server:buildbuddy_image",
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-app-onprem",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
