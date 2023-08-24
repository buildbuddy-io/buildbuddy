#!/usr/bin/env bash

if ! [[ -e /tmp/vmlinux ]]; then
	./build_kernel.sh
fi

exec bazel test \
	enterprise/server/remote_execution/containers/firecracker:firecracker_test_blockio \
	--test_arg=-test.failfast \
	--notest_keep_going \
	--test_arg=-executor.firecracker_debug_stream_vm_logs \
	--test_tag_filters=+bare,+manual \
	--test_timeout=45 \
	--test_filter=SnapshotAndResume \
	--nocache_test_results \
	--runs_per_test=50 \
	--override_repository=org_kernel_git_linux_kernel-vmlinux=/tmp/vmlinux \
	"$@"

