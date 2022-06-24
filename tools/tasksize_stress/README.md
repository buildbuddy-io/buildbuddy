This package contains tests that consume a predictable amount of
resources, which can be useful for manually testing RBE task sizing logic.

Optionally use `--test_filter` to run just the test action(s) you want:

```shell
# Hold 1G of memory for a short time
bazel test //tools/tasksize_stress:all --test_filter=Memory1G
# Run a busy loop w/ 4 cores
bazel test //tools/tasksize_stress:all --test_filter=CPU4
# Run all memory tests in parallel
bazel test //tools/tasksize_stress:all --test_filter=Memory
```
