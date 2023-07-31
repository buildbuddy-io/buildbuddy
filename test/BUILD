[
    genrule(
        name = "test_fetcher_{}".format(i),
        srcs = [
            "@bazel_gazelle//cmd/fetch_repo",
            "@go_sdk_linux//:bin/go",
        ],
        outs = ["out_{}.txt".format(i)],
        cmd = """
          export GOBIN=$$(dirname $$(readlink -f $(location @go_sdk_linux//:bin/go)))
          export PATH=$$PATH:$$GOBIN
          export GOPATH=$$(mktemp -d)
          export DOWNLOAD_DIR=$$(mktemp -d)

          $(location @bazel_gazelle//cmd/fetch_repo) \\
              -dest=$$DOWNLOAD_DIR \\
              -importpath=github.com/go-redis/redis/extra/rediscmd/v8 \\
              -version=v8.11.5 \\
              -sum=h1:ftG8tp8SG81xyuL2woNEx5t2RZ8mOJuC2+tumi+/NR8=

          ls -al $$DOWNLOAD_DIR
          touch $@
        """,
        exec_properties = {
            "workload-isolation-type": "firecracker",
            "recycle-runner": "true",
        },
    )
    for i in range(1000)
]
