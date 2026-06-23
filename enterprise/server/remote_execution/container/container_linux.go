//go:build linux && !android

package container

// CgroupContainer is optionally implemented by containers that use Linux
// cgroup v2.
type CgroupContainer interface {
	// CgroupPath returns the absolute cgroup v2 path for the container's cgroup.
	// It may return an empty string if the cgroup has not been created yet or if
	// cgroup v2 is not supported by the runtime.
	CgroupPath() string
}
