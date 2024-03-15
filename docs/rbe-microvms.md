---
id: rbe-microvms
title: RBE with Firecracker MicroVMs
sidebar_label: RBE with MicroVMs
---

BuildBuddy Cloud has experimental support for running remote build actions
within [Firecracker microVMs](https://github.com/firecracker-microvm/firecracker),
which are lightweight VMs that are optimized for fast startup time.

MicroVMs remove some of the restrictions imposed by the default Docker
container-based Linux execution environment. In particular, microVMs can
be used to run Docker, which means that actions run on BuildBuddy can
spawn Docker containers in order to easily run apps that require lots of
system dependencies, such as MySQL server.

## BUILD configuration

Let's say we have a BUILD file like this:

```python title="BUILD"
sh_test(
    name = "docker_test",
    srcs = ["docker_test.sh"],
)
```

And an executable shell script `docker_test.sh` that looks like this:

```shell
docker run --rm ubuntu:20.04 echo 'PASS' || exit 1
```

This test would normally fail when run using BuildBuddy's shared Linux
executors, since running Docker inside RBE actions is only supported when
using self-hosted executors.

But we can instead run this test using **Docker-in-Firecracker** by
adding a few `exec_properties` to the test runner action:

```python title="BUILD"
sh_test(
    name = "docker_test",
    srcs = ["docker_test.sh"],
    exec_properties = {
        # Tell BuildBuddy to run this test using a Firecracker microVM.
        "test.workload-isolation-type": "firecracker",
        # Tell BuildBuddy to ensure that the Docker daemon is started
        # inside the microVM before the test starts, so that we don't
        # have to worry about starting it ourselves.
        "test.init-dockerd": "true",
    },
)
```

:::note

The `test.` prefix on the `exec_properties` keys ensures that the
properties are only applied to the action that actually runs the test,
and not the actions which are building the test code. See
[execution groups](https://bazel.build/extending/exec-groups) for more
info.

:::

And that's it! This test now works on BuildBuddy's shared Linux executors.

However, it's a bit slow. On each action, a fresh microVM is created. This
is normally fine, because microVMs start up quickly. But the Docker daemon
also has to be re-initialized, which takes a few seconds. Worse yet, it
will be started from an empty Docker image cache, meaning that any images
used in the action will need to be downloaded and unpacked from scratch
each time this action is executed.

Fortunately, we can mitigate both of these issues using runner recyling.

## Preserving microVM state across actions

MicroVM state can be preserved across action runs by enabling the
`recycle-runner` exec property:

```python title="BUILD"
sh_test(
    name = "docker_test",
    srcs = ["docker_test.sh"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        # Tell BuildBuddy to preserve the microVM state across test runs.
        "test.recycle-runner": "true",
    },
)
```

Then, subsequent runs of this test should be able to take advantage of a
warm microVM, with Docker already up and running, and the `ubuntu:20.04`
image already cached from when we ran the previous action.

:::tip

When using runner recycling, the entire microVM state is preserved—not
just the disk contents. You can think of it as being put into "sleep mode"
between actions.

This means that you can leave Docker containers and other processes
running to be reused by subsequent actions, which is helpful for
eliminating startup costs associated with heavyweight processes.

For example, instead of starting MySQL server with `docker run mysql` on
each test action (which is quite slow), you can leave MySQL server running
at the end of each test, and instead re-connect to that server during test
setup of the next test. You can use `docker container inspect` to see if
it the server is already running, and SQL queries like `DROP DATABASE IF EXISTS`
followed by `CREATE DATABASE` to get a clean DB instance.

See
[BuildBuddy's test MySQL implementation](https://github.com/buildbuddy-io/buildbuddy/blob/master/server/testutil/testmysql/testmysql.go)
for an example in Golang.

:::

## Using custom images

If you are using a custom RBE image, you do not need to do anything
special to make it work with Firecracker. BuildBuddy will automatically
convert your Docker image to a disk image compatible with Firecracker. The
`container-image` execution property is specified using the same `docker://`
prefix, like: `docker://some-registry.io/foo/bar`.

To run Docker containers in your microVM (Docker-in-Firecracker), you will
need to make sure your container image has Docker installed. BuildBuddy's
default RBE image already has Docker installed, but when using a custom
image, you may need to install Docker yourself.

See [Install Docker Engine](https://docs.docker.com/engine/install/) for
the commands that you'll need to add to your Dockerfile in order to
install Docker.

Once you've built your custom image, test that Docker is properly
installed by running:

```shell
docker run --rm -it --privileged --name=docker-test your-image.io/foo dockerd --storage-driver=vfs
```

Then, once Docker is finished booting up, run the following command
from another terminal. You should see "Hello world!" printed if Docker
is properly installed:

```shell
docker exec -it docker-test docker run busybox echo "Hello world!"
```
