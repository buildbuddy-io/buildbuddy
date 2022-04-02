package config

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var (
	// Executor config
	appTarget                     = flag.String("executor.app_target", "", "The GRPC url of a buildbuddy app server.")
	pool                          = flag.String("executor.pool", "", "Executor pool name. Only one of this config option or the MY_POOL environment variable should be specified.")
	rootDirectory                 = flag.String("executor.root_directory", "", "The root directory to use for build files.")
	hostRootDirectory             = flag.String("executor.host_root_directory", "", "Path on the host where the executor container root directory is mounted.")
	localCacheDirectory           = flag.String("executor.local_cache_directory", "", "A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result.")
	localCacheSizeBytes           = flag.Int64("executor.local_cache_size_bytes", 0, "The maximum size, in bytes, to use for the local on-disk cache")
	disableLocalCache             = flag.Bool("executor.disable_local_cache", false, "If true, a local file cache will not be used.")
	dockerSocket                  = flag.String("executor.docker_socket", "", "If set, run execution commands in docker using the provided socket.")
	apiKey                        = flag.String("executor.api_key", "", "API Key used to authorize the executor with the BuildBuddy app server.")
	dockerMountMode               = flag.String("executor.docker_mount_mode", "", "Sets the mount mode of volumes mounted to docker images. Useful if running on SELinux https://www.projectatomic.io/blog/2015/06/using-volumes-with-docker-can-cause-problems-with-selinux/")
	maxRunnerCount                = flag.Int("executor.runner_pool.max_runner_count", 0, "Maximum number of recycled RBE runners that can be pooled at once. Defaults to a value derived from estimated CPU usage, max RAM, allocated CPU, and allocated memory.")
	maxRunnerDiskSizeBytes        = flag.Int64("executor.runner_pool.max_runner_disk_size_bytes", 0, "Maximum disk size for a recycled runner; runners exceeding this threshold are not recycled. Defaults to 16GB.")
	maxRunnerMemoryUsageBytes     = flag.Int64("executor.runner_pool.max_runner_memory_usage_bytes", 0, "Maximum memory usage for a recycled runner; runners exceeding this threshold are not recycled. Defaults to 1/10 of total RAM allocated to the executor. (Only supported for Docker-based executors).")
	dockerNetHost                 = flag.Bool("executor.docker_net_host", false, "Sets --net=host on the docker command. Intended for local development only.")
	dockerSiblingContainers       = flag.Bool("executor.docker_sibling_containers", false, "If set, mount the configured Docker socket to containers spawned for each action, to enable Docker-out-of-Docker (DooD). Takes effect only if docker_socket is also set. Should not be set by executors that can run untrusted code.")
	dockerInheritUserIDs          = flag.Bool("executor.docker_inherit_user_ids", false, "If set, run docker containers using the same uid and gid as the user running the executor process.")
	defaultXcodeVersion           = flag.String("executor.default_xcode_version", "", "Sets the default Xcode version number to use if an action doesn't specify one. If not set, /Applications/Xcode.app/ is used.")
	defaultIsolationType          = flag.String("executor.default_isolation_type", "", "The default workload isolation type when no type is specified in an action. If not set, we use the first of the following that is set: docker, firecracker, podman, or barerunner")
	enableBareRunner              = flag.Bool("executor.enable_bare_runner", false, "Enables running execution commands directly on the host without isolation.")
	enablePodman                  = flag.Bool("executor.enable_podman", false, "Enables running execution commands inside podman container.")
	enableFirecracker             = flag.Bool("executor.enable_firecracker", false, "Enables running execution commands inside of firecracker VMs")
	firecrackerMountWorkspaceFile = flag.Bool("executor.firecracker_mount_workspace_file", false, "Enables mounting workspace filesystem to improve performance of copying action outputs.")
	containerRegistries           = []config.ContainerRegistryConfig{}
	enableVFS                     = flag.Bool("executor.enable_vfs", false, "Whether FUSE based filesystem is enabled.")
	defaultImage                  = flag.String("executor.default_image", "", "The default docker image to use to warm up executors or if no platform property is set. Ex: gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4")
	warmupTimeoutSecs             = flag.Int64("executor.warmup_timeout_secs", 0, "The default time (in seconds) to wait for an executor to warm up i.e. download the default docker image. Default is 120s")
	startupWarmupMaxWaitSecs      = flag.Int64("executor.startup_warmup_max_wait_secs", 0, "Maximum time to block startup while waiting for default image to be pulled. Default is no wait.")
	exclusiveTaskScheduling       = flag.Bool("executor.exclusive_task_scheduling", false, "If true, only one task will be scheduled at a time. Default is false")
	memoryBytes                   = flag.Int64("executor.memory_bytes", 0, "Optional maximum memory to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MEMORY_BYTES env var.")
	milliCPU                      = flag.Int64("executor.millicpu", 0, "Optional maximum CPU milliseconds to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MILLICPU env var.")
)

func init() {
	flagutil.StructSliceVar(&containerRegistries, "executor.container_registries", "")
}

func Get() *config.ExecutorConfig {
	return &config.ExecutorConfig{
		AppTarget:           *appTarget,
		Pool:                *pool,
		RootDirectory:       *rootDirectory,
		HostRootDirectory:   *hostRootDirectory,
		LocalCacheDirectory: *localCacheDirectory,
		LocalCacheSizeBytes: *localCacheSizeBytes,
		DisableLocalCache:   *disableLocalCache,
		DockerSocket:        *dockerSocket,
		APIKey:              *apiKey,
		DockerMountMode:     *dockerMountMode,
		RunnerPool: config.RunnerPoolConfig{
			MaxRunnerCount:            *maxRunnerCount,
			MaxRunnerDiskSizeBytes:    *maxRunnerDiskSizeBytes,
			MaxRunnerMemoryUsageBytes: *maxRunnerMemoryUsageBytes,
		},
		DockerNetHost:                 *dockerNetHost,
		DockerSiblingContainers:       *dockerSiblingContainers,
		DockerInheritUserIDs:          *dockerInheritUserIDs,
		DefaultXcodeVersion:           *defaultXcodeVersion,
		DefaultIsolationType:          *defaultIsolationType,
		EnableBareRunner:              *enableBareRunner,
		EnablePodman:                  *enablePodman,
		EnableFirecracker:             *enableFirecracker,
		FirecrackerMountWorkspaceFile: *firecrackerMountWorkspaceFile,
		ContainerRegistries:           *&containerRegistries,
		EnableVFS:                     *enableVFS,
		DefaultImage:                  *defaultImage,
		WarmupTimeoutSecs:             *warmupTimeoutSecs,
		StartupWarmupMaxWaitSecs:      *startupWarmupMaxWaitSecs,
		ExclusiveTaskScheduling:       *exclusiveTaskScheduling,
		MemoryBytes:                   *memoryBytes,
		MilliCPU:                      *milliCPU,
	}
}
