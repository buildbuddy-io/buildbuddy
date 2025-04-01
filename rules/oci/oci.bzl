"""
There are a few things to note about our container image usage:

1. We always want to build with "--compilation_mode=opt" to ensure that we include all the necessary
   runfiles in the image. Without "-c opt", targets such as //enterprise/app:bundle or //app:bundle
   will be empty and cause our app to fail to startup.

2. We always want either linux_arm64 or linux_amd64 as the target platform. There is no MacOS container
   image and we have yet to support Windows images today.

This means that for all go_binary that we are adding to the image, we want to apply a transition
to ensure that the Go binary was built with "opt" and targeting Linux.
This should be true regardless of the existing host platform. For example, if a developer were to run

  # Assume that
  #   //some/target:go_binary --is a dep of--> //some/target:oci_image
  #
  > bazel build //some/target:oci_image

on a MacOS laptop, we want to either force the go_binary to be opt+linux (via RBE or pure go cross 
compilation), or to fail the build as there is no compatible execution platform.

Given these constraints, we want a setup that looks like this:

    go_binary(
        name = "some_binary",
        ...
    )

    opt_linux_transition_rule(
        name = "opt_linux_some_binary",
        inputs = [":some_binary"],
        target_compatible_with = ["@platforms//os:linux"],
        tags = ["manual"],
    )

    pkg_tar(
        name = "tar",
        srcs = [":opt_linux_some_binary"],
        ...
    )

    oci_image(
        name = "oci_image",
        tars = [":tar"],
        ...
    )
"""
