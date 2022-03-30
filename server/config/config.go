package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v2"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	configFile = flag.String("config_file", "", "The path to a buildbuddy config file")
)

// When adding new storage fields, always be explicit about their yaml field
// name.
type generalConfig struct {
	Org             OrgConfig             `yaml:"org"`
	Integrations    integrationsConfig    `yaml:"integrations"`
	Github          GithubConfig          `yaml:"github"`
	API             APIConfig             `yaml:"api"`
	Storage         storageConfig         `yaml:"storage"`
	SSL             SSLConfig             `yaml:"ssl"`
	Auth            authConfig            `yaml:"auth"`
	RemoteExecution RemoteExecutionConfig `yaml:"remote_execution"`
	BuildEventProxy buildEventProxy       `yaml:"build_event_proxy"`
	App             appConfig             `yaml:"app"`
	Database        DatabaseConfig        `yaml:"database"`
	Cache           cacheConfig           `yaml:"cache"`
	Executor        ExecutorConfig        `yaml:"executor"`
}

type appConfig struct {
	BuildBuddyURL             flagutil.URLFlag   `yaml:"build_buddy_url" usage:"The external URL where your BuildBuddy instance can be found."`
	EventsAPIURL              flagutil.URLFlag   `yaml:"events_api_url" usage:"Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen."`
	CacheAPIURL               flagutil.URLFlag   `yaml:"cache_api_url" usage:"Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen."`
	RemoteExecutionAPIURL     flagutil.URLFlag   `yaml:"remote_execution_api_url" usage:"Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen."`
	LogLevel                  string             `yaml:"log_level" usage:"The desired log level. Logs with a level >= this level will be emitted. One of {'fatal', 'error', 'warn', 'info', 'debug'}"`
	GRPCMaxRecvMsgSizeBytes   int                `yaml:"grpc_max_recv_msg_size_bytes" usage:"Configures the max GRPC receive message size [bytes]"`
	GRPCOverHTTPPortEnabled   bool               `yaml:"grpc_over_http_port_enabled" usage:"Cloud-Only"`
	AddUserToDomainGroup      bool               `yaml:"add_user_to_domain_group" usage:"Cloud-Only"`
	DefaultToDenseMode        bool               `yaml:"default_to_dense_mode" usage:"Enables the dense UI mode by default."`
	CreateGroupPerUser        bool               `yaml:"create_group_per_user" usage:"Cloud-Only"`
	EnableTargetTracking      bool               `yaml:"enable_target_tracking" usage:"Cloud-Only"`
	EnableStructuredLogging   bool               `yaml:"enable_structured_logging" usage:"If true, log messages will be json-formatted."`
	LogIncludeShortFileName   bool               `yaml:"log_include_short_file_name" usage:"If true, log messages will include shortened originating file name."`
	NoDefaultUserGroup        bool               `yaml:"no_default_user_group" usage:"Cloud-Only"`
	LogEnableGCPLoggingFormat bool               `yaml:"log_enable_gcp_logging_format" usage:"If true, the output structured logs will be compatible with format expected by GCP Logging."`
	LogErrorStackTraces       bool               `yaml:"log_error_stack_traces" usage:"If true, stack traces will be printed for errors that have them."`
	TraceProjectID            string             `yaml:"trace_project_id" usage:"Optional GCP project ID to export traces to. If not specified, determined from default credentials or metadata server if running on GCP."`
	TraceJaegerCollector      string             `yaml:"trace_jaeger_collector" usage:"Address of the Jager collector endpoint where traces will be sent."`
	TraceServiceName          string             `yaml:"trace_service_name" usage:"Name of the service to associate with traces."`
	TraceFraction             float64            `yaml:"trace_fraction" usage:"Fraction of requests to sample for tracing."`
	TraceFractionOverrides    []string           `yaml:"trace_fraction_overrides" usage:"Tracing fraction override based on name in format name=fraction."`
	IgnoreForcedTracingHeader bool               `yaml:"ignore_forced_tracing_header" usage:"If set, we will not honor the forced tracing header."`
	CodeEditorEnabled         bool               `yaml:"code_editor_enabled" usage:"If set, code editor functionality will be enabled."`
	UserManagementEnabled     bool               `yaml:"user_management_enabled" usage:"If set, the user management page will be enabled in the UI."`
	GlobalFilterEnabled       bool               `yaml:"global_filter_enabled" usage:"If set, the global filter will be enabled in the UI."`
	TestGridV2Enabled         bool               `yaml:"test_grid_v2_enabled" usage:"Whether to enable test grid V2"`
	DisableCertConfig         bool               `yaml:"disable_cert_config" usage:"If true, the certificate based auth option will not be shown in the config widget."`
	UsageEnabled              bool               `yaml:"usage_enabled" usage:"If set, the usage page will be enabled in the UI."`
	UsageStartDate            string             `yaml:"usage_start_date" usage:"If set, usage data will only be viewable on or after this timestamp. Specified in RFC3339 format, like 2021-10-01T00:00:00Z"`
	UsageTrackingEnabled      bool               `yaml:"usage_tracking_enabled" usage:"If set, enable usage data collection."`
	DefaultRedisTarget        string             `yaml:"default_redis_target" usage:"A Redis target for storing remote shared state. To ease migration, the redis target from the remote execution config will be used if this value is not specified."`
	DefaultShardedRedis       ShardedRedisConfig `yaml:"default_sharded_redis" usage:"Configuration for storing ephemeral state across multiple Redis instances. Mutually exclusive with default_redis_target."`
	Region                    string             `yaml:"region" usage:"The region in which the app is running."`
}

type buildEventProxy struct {
	Hosts      []string `yaml:"hosts" usage:"The list of hosts to pass build events onto."`
	BufferSize int      `yaml:"buffer_size" usage:"The number of build events to buffer locally when proxying build events."`
}

type DatabaseConfig struct {
	DataSource             string        `yaml:"data_source" usage:"The SQL database to connect to, specified as a connection string."`
	ReadReplica            string        `yaml:"read_replica" usage:"A secondary, read-only SQL database to connect to, specified as a connection string."`
	StatsPollInterval      time.Duration `yaml:"stats_poll_interval" usage:"How often to poll the DB client for connection stats (default: '5s')."`
	MaxOpenConns           int           `yaml:"max_open_conns" usage:"The maximum number of open connections to maintain to the db"`
	MaxIdleConns           int           `yaml:"max_idle_conns" usage:"The maximum number of idle connections to maintain to the db"`
	ConnMaxLifetimeSeconds int           `yaml:"conn_max_lifetime_seconds" usage:"The maximum lifetime of a connection to the db"`
	LogQueries             bool          `yaml:"log_queries" usage:"If true, log all queries"`
}

type storageConfig struct {
	Disk                   DiskConfig  `yaml:"disk"`
	GCS                    GCSConfig   `yaml:"gcs"`
	AwsS3                  AwsS3Config `yaml:"aws_s3"`
	Azure                  AzureConfig `yaml:"azure"`
	TTLSeconds             int         `yaml:"ttl_seconds" usage:"The time, in seconds, to keep invocations before deletion"`
	ChunkFileSizeBytes     int         `yaml:"chunk_file_size_bytes" usage:"How many bytes to buffer in memory before flushing a chunk of build protocol data to disk."`
	EnableChunkedEventLogs bool        `yaml:"enable_chunked_event_logs" usage:"If true, Event logs will be stored separately from the invocation proto in chunks."`
}

type DiskCachePartition struct {
	ID           string `yaml:"id" json:"id" usage:"The ID of the partition."`
	MaxSizeBytes int64  `yaml:"max_size_bytes" json:"max_size_bytes" usage:"Maximum size of the partition."`
}

type DiskCachePartitionMapping struct {
	GroupID     string `yaml:"group_id" json:"group_id" usage:"The Group ID to which this mapping applies."`
	Prefix      string `yaml:"prefix" json:"prefix" usage:"The remote instance name prefix used to select this partition."`
	PartitionID string `yaml:"partition_id" json:"partition_id" usage:"The partition to use if the Group ID and prefix match."`
}

type DiskConfig struct {
	RootDirectory     string                      `yaml:"root_directory" usage:"The root directory to store all blobs in, if using disk based storage."`
	Partitions        []DiskCachePartition        `yaml:"partitions"`
	PartitionMappings []DiskCachePartitionMapping `yaml:"partition_mappings"`
	UseV2Layout       bool                        `yaml:"use_v2_layout" usage:"If enabled, files will be stored using the v2 layout. See disk_cache.MigrateToV2Layout for a description."`
}

type GCSConfig struct {
	Bucket          string `yaml:"bucket" usage:"The name of the GCS bucket to store build artifact files in."`
	CredentialsFile string `yaml:"credentials_file" usage:"A path to a JSON credentials file that will be used to authenticate to GCS."`
	ProjectID       string `yaml:"project_id" usage:"The Google Cloud project ID of the project owning the above credentials and GCS bucket."`
}

type AwsS3Config struct {
	Region             string `yaml:"region" usage:"The AWS region."`
	Bucket             string `yaml:"bucket" usage:"The AWS S3 bucket to store files in."`
	CredentialsProfile string `yaml:"credentials_profile" usage:"A custom credentials profile to use."`

	// Useful for configuring WebIdentity based auth
	WebIdentityTokenFilePath string `yaml:"web_identity_token_file" usage:"The file path to the web identity token file."`
	RoleARN                  string `yaml:"role_arn" usage:"The role ARN to use for web identity auth."`
	RoleSessionName          string `yaml:"role_session_name" usage:"The role session name to use for web identity auth."`

	// Useful for configuring MinIO: https://docs.min.io/docs/how-to-use-aws-sdk-for-go-with-minio-server.html
	Endpoint                string `yaml:"endpoint" usage:"The AWS endpoint to use, useful for configuring the use of MinIO."`
	StaticCredentialsID     string `yaml:"static_credentials_id" usage:"Static credentials ID to use, useful for configuring the use of MinIO."`
	StaticCredentialsSecret string `yaml:"static_credentials_secret" usage:"Static credentials secret to use, useful for configuring the use of MinIO."`
	StaticCredentialsToken  string `yaml:"static_credentials_token" usage:"Static credentials token to use, useful for configuring the use of MinIO."`
	DisableSSL              bool   `yaml:"disable_ssl" usage:"Disables the use of SSL, useful for configuring the use of MinIO."`
	S3ForcePathStyle        bool   `yaml:"s3_force_path_style" usage:"Force path style urls for objects, useful for configuring the use of MinIO."`
}

type AzureConfig struct {
	AccountName   string `yaml:"account_name" usage:"The name of the Azure storage account"`
	AccountKey    string `yaml:"account_key" usage:"The key for the Azure storage account"`
	ContainerName string `yaml:"container_name" usage:"The name of the Azure storage container"`
}

type integrationsConfig struct {
	Slack            SlackConfig            `yaml:"slack"`
	InvocationUpload InvocationUploadConfig `yaml:"invocation_upload"`
}

type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url" usage:"A Slack webhook url to post build update messages to."`
}

type InvocationUploadConfig struct {
	Enabled            bool   `yaml:"enabled" usage:"Whether to upload webhook data to the webhook URL configured per-Group. ** Enterprise only **"`
	GCSCredentialsJSON string `yaml:"gcs_credentials" usage:"Credentials JSON for the Google service account used to authenticate when GCS is used as the invocation upload target. ** Enterprise only **"`
}

type GCSCacheConfig struct {
	Bucket          string `yaml:"bucket" usage:"The name of the GCS bucket to store cache files in."`
	CredentialsFile string `yaml:"credentials_file" usage:"A path to a JSON credentials file that will be used to authenticate to GCS."`
	ProjectID       string `yaml:"project_id" usage:"The Google Cloud project ID of the project owning the above credentials and GCS bucket."`
	TTLDays         int64  `yaml:"ttl_days" usage:"The period after which cache files should be TTLd. Disabled if 0."`
}

type S3CacheConfig struct {
	Region             string `yaml:"region" usage:"The AWS region."`
	Bucket             string `yaml:"bucket" usage:"The AWS S3 bucket to store files in."`
	CredentialsProfile string `yaml:"credentials_profile" usage:"A custom credentials profile to use."`
	TTLDays            int64  `yaml:"ttl_days" usage:"The period after which cache files should be TTLd. Disabled if 0."`

	// Useful for configuring WebIdentity based auth
	WebIdentityTokenFilePath string `yaml:"web_identity_token_file" usage:"The file path to the web identity token file."`
	RoleARN                  string `yaml:"role_arn" usage:"The role ARN to use for web identity auth."`
	RoleSessionName          string `yaml:"role_session_name" usage:"The role session name to use for web identity auth."`

	// Useful for configuring MinIO: https://docs.min.io/docs/how-to-use-aws-sdk-for-go-with-minio-server.html
	Endpoint                string `yaml:"endpoint" usage:"The AWS endpoint to use, useful for configuring the use of MinIO."`
	StaticCredentialsID     string `yaml:"static_credentials_id" usage:"Static credentials ID to use, useful for configuring the use of MinIO."`
	StaticCredentialsSecret string `yaml:"static_credentials_secret" usage:"Static credentials secret to use, useful for configuring the use of MinIO."`
	StaticCredentialsToken  string `yaml:"static_credentials_token" usage:"Static credentials token to use, useful for configuring the use of MinIO."`
	DisableSSL              bool   `yaml:"disable_ssl" usage:"Disables the use of SSL, useful for configuring the use of MinIO."`
	S3ForcePathStyle        bool   `yaml:"s3_force_path_style" usage:"Force path style urls for objects, useful for configuring the use of MinIO."`
}

type DistributedCacheConfig struct {
	ListenAddr        string   `yaml:"listen_addr" usage:"The address to listen for local BuildBuddy distributed cache traffic on."`
	RedisTarget       string   `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **"`
	GroupName         string   `yaml:"group_name" usage:"A unique name for this distributed cache group. ** Enterprise only **"`
	Nodes             []string `yaml:"nodes" usage:"The hardcoded list of peer distributed cache nodes. If this is set, redis_target will be ignored. ** Enterprise only **"`
	ReplicationFactor int      `yaml:"replication_factor" usage:"How many total servers the data should be replicated to. Must be >= 1. ** Enterprise only **"`
	ClusterSize       int      `yaml:"cluster_size" usage:"The total number of nodes in this cluster. Required for health checking. ** Enterprise only **"`
}

type RaftCacheConfig struct {
	RootDirectory string   `yaml:"root_directory" usage:"The root directory to use for storing cached data."`
	ListenAddr    string   `yaml:"listen_addr" usage:"The address to listen for local gossip traffic on. Ex. 'localhost:1991"`
	Join          []string `yaml:"join" usage:"The list of nodes to use when joining clusters Ex. '1.2.3.4:1991,2.3.4.5:1991...'"`
	HTTPPort      int      `yaml:"http_port" usage:"The address to listen for HTTP raft traffic. Ex. '1992'"`
	GRPCPort      int      `yaml:"grpc_port" usage:"The address to listen for internal API traffic on. Ex. '1993'"`
}

type RedisCacheConfig struct {
	RedisTarget       string             `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **"`
	Sharded           ShardedRedisConfig `yaml:"sharded" usage:"Configuration for storing cache data sharded across multiple Redis instances. Mutually exclusive with redis_target."`
	MaxValueSizeBytes int64              `yaml:"max_value_size_bytes" usage:"The maximum value size to cache in redis (in bytes)."`
}

type cacheConfig struct {
	Disk                   DiskConfig             `yaml:"disk"`
	RedisTarget            string                 `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **"`
	S3                     S3CacheConfig          `yaml:"s3"`
	GCS                    GCSCacheConfig         `yaml:"gcs"`
	MemcacheTargets        []string               `yaml:"memcache_targets" usage:"Deprecated. Use Redis Target instead."`
	Redis                  RedisCacheConfig       `yaml:"redis"`
	DistributedCache       DistributedCacheConfig `yaml:"distributed_cache"`
	RaftCache              RaftCacheConfig        `yaml:"raft"`
	MaxSizeBytes           int64                  `yaml:"max_size_bytes" usage:"How big to allow the cache to be (in bytes)."`
	InMemory               bool                   `yaml:"in_memory" usage:"Whether or not to use the in_memory cache."`
	ZstdTranscodingEnabled bool                   `yaml:"zstd_transcoding_enabled" usage:"Whether to accept requests to read/write zstd-compressed blobs, compressing/decompressing outgoing/incoming blobs on the fly."`
}

type authConfig struct {
	JWTKey               string          `yaml:"jwt_key" usage:"The key to use when signing JWT tokens."`
	APIKeyGroupCacheTTL  string          `yaml:"api_key_group_cache_ttl" usage:"Override for the TTL for API Key to Group caching. Set to '0' to disable cache."`
	OauthProviders       []OauthProvider `yaml:"oauth_providers"`
	EnableAnonymousUsage bool            `yaml:"enable_anonymous_usage" usage:"If true, unauthenticated build uploads will still be allowed but won't be associated with your organization."`
	SAMLConfig           SAMLConfig      `yaml:"saml" usage:"Configuration for setting up SAML auth support."`
	EnableSelfAuth       bool            `yaml:"enable_self_auth" usage:"If true, enables a single user login via an oauth provider on the buildbuddy server. Recommend use only when server is behind a firewall; this option may allow anyone with access to the webpage admin rights to your buildbuddy installation. ** Enterprise only **"`
	AdminGroupID         string          `yaml:"admin_group_id" usage:"ID of a group whose members can perform actions only accessible to server admins."`
	HttpsOnlyCookies     bool            `yaml:"https_only_cookies" usage:"If true, cookies will only be set over https connections."`
	DisableRefreshToken  bool            `yaml:"disable_refresh_token" usage:"If true, the offline_access scope which requests refresh tokens will not be requested."`
}

type OauthProvider struct {
	IssuerURL    string `yaml:"issuer_url" json:"issuer_url" usage:"The issuer URL of this OIDC Provider."`
	ClientID     string `yaml:"client_id" json:"client_id" usage:"The oauth client ID."`
	ClientSecret string `yaml:"client_secret" json:"client_secret" usage:"The oauth client secret."`
	Slug         string `yaml:"slug" json:"slug" usage:"The slug of this OIDC Provider."`
}
type SAMLConfig struct {
	CertFile string `yaml:"cert_file" usage:"Path to a PEM encoded certificate file used for SAML auth."`
	KeyFile  string `yaml:"key_file" usage:"Path to a PEM encoded certificate key file used for SAML auth."`
}

type SSLConfig struct {
	CertFile         string   `yaml:"cert_file" usage:"Path to a PEM encoded certificate file to use for TLS if not using ACME."`
	KeyFile          string   `yaml:"key_file" usage:"Path to a PEM encoded key file to use for TLS if not using ACME."`
	ClientCACertFile string   `yaml:"client_ca_cert_file" usage:"Path to a PEM encoded certificate authority file used to issue client certificates for mTLS auth."`
	ClientCAKeyFile  string   `yaml:"client_ca_key_file" usage:"Path to a PEM encoded certificate authority key file used to issue client certificates for mTLS auth."`
	HostWhitelist    []string `yaml:"host_whitelist" usage:"Cloud-Only"`
	EnableSSL        bool     `yaml:"enable_ssl" usage:"Whether or not to enable SSL/TLS on gRPC connections (gRPCS)."`
	UpgradeInsecure  bool     `yaml:"upgrade_insecure" usage:"True if http requests should be redirected to https"`
	UseACME          bool     `yaml:"use_acme" usage:"Whether or not to automatically configure SSL certs using ACME. If ACME is enabled, cert_file and key_file should not be set."`
	DefaultHost      string   `yaml:"default_host" usage:"Host name to use for ACME generated cert if TLS request does not contain SNI."`
}

type ShardedRedisConfig struct {
	Shards   []string `yaml:"shards" usage:"Ordered list of Redis shard addresses."`
	Username string   `yaml:"username" usage:"Redis username"`
	Password string   `yaml:"password" usage:"Redis password"`
}

type RemoteExecutionConfig struct {
	DefaultPoolName                   string             `yaml:"default_pool_name" usage:"The default executor pool to use if one is not specified."`
	EnableWorkflows                   bool               `yaml:"enable_workflows" usage:"Whether to enable BuildBuddy workflows."`
	WorkflowsPoolName                 string             `yaml:"workflows_pool_name" usage:"The executor pool to use for workflow actions. Defaults to the default executor pool if not specified."`
	WorkflowsDefaultImage             string             `yaml:"workflows_default_image" usage:"The default docker image to use for running workflows."`
	WorkflowsCIRunnerDebug            bool               `yaml:"workflows_ci_runner_debug" usage:"Whether to run the CI runner in debug mode."`
	WorkflowsCIRunnerBazelCommand     string             `yaml:"workflows_ci_runner_bazel_command" usage:"Bazel command to be used by the CI runner."`
	WorkflowsEnableFirecracker        bool               `yaml:"workflows_enable_firecracker" usage:"Whether to enable firecracker for Linux workflow actions."`
	RedisTarget                       string             `yaml:"redis_target" usage:"A Redis target for storing remote execution state. Falls back to app.default_redis_target if unspecified. Required for remote execution. To ease migration, the redis target from the cache config will be used if neither this value nor app.default_redis_target are specified."`
	ShardedRedis                      ShardedRedisConfig `yaml:"sharded_redis" usage:"Optional configuration for sharding execution data across multiple Redis instances. Mutually exclusive with the redis_target option."`
	SharedExecutorPoolGroupID         string             `yaml:"shared_executor_pool_group_id" usage:"Group ID that owns the shared executor pool."`
	RedisPubSubPoolSize               int                `yaml:"redis_pubsub_pool_size" usage:"Maximum number of connections used for waiting for execution updates."`
	EnableRemoteExec                  bool               `yaml:"enable_remote_exec" usage:"If true, enable remote-exec. ** Enterprise only **"`
	RequireExecutorAuthorization      bool               `yaml:"require_executor_authorization" usage:"If true, executors connecting to this server must provide a valid executor API key."`
	EnableUserOwnedExecutors          bool               `yaml:"enable_user_owned_executors" usage:"If enabled, users can register their own executors with the scheduler."`
	ForceUserOwnedDarwinExecutors     bool               `yaml:"force_user_owned_darwin_executors" usage:"If enabled, darwin actions will always run on user-owned executors."`
	EnableExecutorKeyCreation         bool               `yaml:"enable_executor_key_creation" usage:"If enabled, UI will allow executor keys to be created."`
	EnableRedisAvailabilityMonitoring bool               `yaml:"enable_redis_availability_monitoring" usage:"If enabled, the execution server will detect if Redis has lost state and will ask Bazel to retry executions."`
}

type ExecutorConfig struct {
	AppTarget                     string                    `yaml:"app_target" usage:"The GRPC url of a buildbuddy app server."`
	Pool                          string                    `yaml:"pool" usage:"Executor pool name. Only one of this config option or the MY_POOL environment variable should be specified."`
	RootDirectory                 string                    `yaml:"root_directory" usage:"The root directory to use for build files."`
	HostRootDirectory             string                    `yaml:"host_root_directory" usage:"Path on the host where the executor container root directory is mounted."`
	LocalCacheDirectory           string                    `yaml:"local_cache_directory" usage:"A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result."`
	LocalCacheSizeBytes           int64                     `yaml:"local_cache_size_bytes" usage:"The maximum size, in bytes, to use for the local on-disk cache"`
	DisableLocalCache             bool                      `yaml:"disable_local_cache" usage:"If true, a local file cache will not be used."`
	DockerSocket                  string                    `yaml:"docker_socket" usage:"If set, run execution commands in docker using the provided socket."`
	APIKey                        string                    `yaml:"api_key" usage:"API Key used to authorize the executor with the BuildBuddy app server."`
	DockerMountMode               string                    `yaml:"docker_mount_mode" usage:"Sets the mount mode of volumes mounted to docker images. Useful if running on SELinux https://www.projectatomic.io/blog/2015/06/using-volumes-with-docker-can-cause-problems-with-selinux/"`
	RunnerPool                    RunnerPoolConfig          `yaml:"runner_pool"`
	DockerNetHost                 bool                      `yaml:"docker_net_host" usage:"Sets --net=host on the docker command. Intended for local development only."`
	DockerCapAdd                  string                    `yaml:"docker_cap_add" usage:"Sets --cap-add= on the docker command. Comma separated."`
	DockerSiblingContainers       bool                      `yaml:"docker_sibling_containers" usage:"If set, mount the configured Docker socket to containers spawned for each action, to enable Docker-out-of-Docker (DooD). Takes effect only if docker_socket is also set. Should not be set by executors that can run untrusted code."`
	DockerInheritUserIDs          bool                      `yaml:"docker_inherit_user_ids" usage:"If set, run docker containers using the same uid and gid as the user running the executor process."`
	DefaultXcodeVersion           string                    `yaml:"default_xcode_version" usage:"Sets the default Xcode version number to use if an action doesn't specify one. If not set, /Applications/Xcode.app/ is used."`
	DefaultIsolationType          string                    `yaml:"default_isolation_type" usage:"The default workload isolation type when no type is specified in an action. If not set, we use the first of the following that is set: docker, firecracker, podman, or barerunner"`
	EnableBareRunner              bool                      `yaml:"enable_bare_runner" usage:"Enables running execution commands directly on the host without isolation."`
	EnablePodman                  bool                      `yaml:"enable_podman" usage:"Enables running execution commands inside podman container."`
	PodmanRuntime                 string                    `yaml:"podman_runtime" usage:"Enables running podman with other runtimes, like gVisor (runsc)."`
	EnableFirecracker             bool                      `yaml:"enable_firecracker" usage:"Enables running execution commands inside of firecracker VMs"`
	FirecrackerMountWorkspaceFile bool                      `yaml:"firecracker_mount_workspace_file" usage:"Enables mounting workspace filesystem to improve performance of copying action outputs."`
	ContainerRegistries           []ContainerRegistryConfig `yaml:"container_registries"`
	EnableVFS                     bool                      `yaml:"enable_vfs" usage:"Whether FUSE based filesystem is enabled."`
	DefaultImage                  string                    `yaml:"default_image" usage:"The default docker image to use to warm up executors or if no platform property is set. Ex: gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4"`
	WarmupTimeoutSecs             int64                     `yaml:"warmup_timeout_secs" usage:"The default time (in seconds) to wait for an executor to warm up i.e. download the default docker image. Default is 120s"`
	StartupWarmupMaxWaitSecs      int64                     `yaml:"startup_warmup_max_wait_secs" usage:"Maximum time to block startup while waiting for default image to be pulled. Default is no wait."`
	ExclusiveTaskScheduling       bool                      `yaml:"exclusive_task_scheduling" usage:"If true, only one task will be scheduled at a time. Default is false"`
	MemoryBytes                   int64                     `yaml:"memory_bytes" usage:"Optional maximum memory to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MEMORY_BYTES env var."`
	MilliCPU                      int64                     `yaml:"millicpu" usage:"Optional maximum CPU milliseconds to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MILLICPU env var."`
}

type ContainerRegistryConfig struct {
	Hostnames []string `yaml:"hostnames" json:"hostnames"`
	Username  string   `yaml:"username" json:"username"`
	Password  string   `yaml:"password" json:"password"`
}

func (c *ExecutorConfig) GetAppTarget() string {
	if c.AppTarget == "" {
		return "grpcs://remote.buildbuddy.io"
	}
	return c.AppTarget
}

func (c *ExecutorConfig) GetRootDirectory() string {
	if c.RootDirectory == "" {
		return "/tmp/buildbuddy/remote_build"
	}
	return c.RootDirectory
}

func (c *ExecutorConfig) GetLocalCacheDirectory() string {
	if c.DisableLocalCache {
		return ""
	}
	if c.LocalCacheDirectory == "" {
		return "/tmp/buildbuddy/filecache"
	}
	return c.LocalCacheDirectory
}

func (c *ExecutorConfig) GetLocalCacheSizeBytes() int64 {
	if c.DisableLocalCache {
		return 0
	}
	if c.LocalCacheSizeBytes == 0 {
		return 1_000_000_000 // 1 GB
	}
	return c.LocalCacheSizeBytes
}

type RunnerPoolConfig struct {
	MaxRunnerCount            int   `yaml:"max_runner_count" usage:"Maximum number of recycled RBE runners that can be pooled at once. Defaults to a value derived from estimated CPU usage, max RAM, allocated CPU, and allocated memory."`
	MaxRunnerDiskSizeBytes    int64 `yaml:"max_runner_disk_size_bytes" usage:"Maximum disk size for a recycled runner; runners exceeding this threshold are not recycled. Defaults to 16GB."`
	MaxRunnerMemoryUsageBytes int64 `yaml:"max_runner_memory_usage_bytes" usage:"Maximum memory usage for a recycled runner; runners exceeding this threshold are not recycled. Defaults to 1/10 of total RAM allocated to the executor. (Only supported for Docker-based executors)."`
}

type APIConfig struct {
	APIKey    string `yaml:"api_key" usage:"The default API key to use for on-prem enterprise deploys with a single organization/group."`
	EnableAPI bool   `yaml:"enable_api" usage:"Whether or not to enable the BuildBuddy API."`
}

type GithubConfig struct {
	ClientID            string `yaml:"client_id" usage:"The client ID of your GitHub Oauth App. ** Enterprise only **"`
	ClientSecret        string `yaml:"client_secret" usage:"The client secret of your GitHub Oauth App. ** Enterprise only **"`
	AccessToken         string `yaml:"access_token" usage:"The GitHub access token used to post GitHub commit statuses. ** Enterprise only **"`
	StatusNameSuffix    string `yaml:"status_name_suffix" usage:"Suffix to be appended to all reported GitHub status names. Useful for differentiating BuildBuddy deployments. For example: '(dev)' ** Enterprise only **"`
	StatusPerTestTarget bool   `yaml:"status_per_test_target" usage:"If true, report status per test target. ** Enterprise only **"`
}

type OrgConfig struct {
	Name   string `yaml:"name" usage:"The name of your organization, which is displayed on your organization's build history."`
	Domain string `yaml:"domain" usage:"Your organization's email domain. If this is set, only users with email addresses in this domain will be able to register for a BuildBuddy account."`
}

var (
	sharedConfigurator = &Configurator{gc: &generalConfig{}}

	// Contains the string slices originally defined by the flags
	originalSliceLens = make(map[string]int)
	defineFlagsOnce   sync.Once

	// Set of the flags that were explicitly set on the command line
	originalSetFlags = make(map[string]struct{})
)

func RegisterAndParseFlags() {
	defineFlagsOnce.Do(func() {
		defineFlagsForMembers([]string{}, reflect.ValueOf(&generalConfig{}).Elem(), flag.CommandLine)
		flag.Parse()
		flag.Visit(func(flg *flag.Flag) {
			originalSetFlags[flg.Name] = struct{}{}
		})
	})
}

// FOR TESTING PURPOSES ONLY!!!
func TestOnlySetFlag(flagName string, set bool) bool {
	_, wasSet := originalSetFlags[flagName]
	if set {
		originalSetFlags[flagName] = struct{}{}
	} else {
		delete(originalSetFlags, flagName)
	}
	return wasSet
}

// If configSet is nil, defines all global flags present in the config that have
// not been elsewhere defined. If configSet isn't nil, defines flags in the
// provided FlagSet for the config.
func defineFlagsForMembers(parentStructNames []string, T reflect.Value, flagSet *flag.FlagSet) {
	typeOfT := T.Type()
	for i := 0; i < T.NumField(); i++ {
		f := T.Field(i)
		fieldName := typeOfT.Field(i).Tag.Get("yaml")
		fqFieldName := strings.ToLower(strings.Join(append(parentStructNames, fieldName), "."))
		docString := typeOfT.Field(i).Tag.Get("usage")

		// Only define missing flags
		if flagSet.Lookup(fqFieldName) == nil {
			switch f.Type().Kind() {
			case reflect.Ptr:
				log.Fatal("The config should not contain pointers!")
			case reflect.Struct:
				defineFlagsForMembers(append(parentStructNames, fieldName), f, flagSet)
			case reflect.Bool:
				flagSet.BoolVar(f.Addr().Convert(reflect.TypeOf((*bool)(nil))).Interface().(*bool), fqFieldName, f.Bool(), docString)
			case reflect.String:
				flagSet.StringVar(f.Addr().Convert(reflect.TypeOf((*string)(nil))).Interface().(*string), fqFieldName, f.String(), docString)
			case reflect.Int:
				flagSet.IntVar(f.Addr().Convert(reflect.TypeOf((*int)(nil))).Interface().(*int), fqFieldName, int(f.Int()), docString)
			case reflect.Int64:
				if f.Addr().Type() == reflect.TypeOf((*time.Duration)(nil)) {
					flagSet.DurationVar(f.Addr().Interface().(*time.Duration), fqFieldName, time.Duration(f.Int()), docString)
					continue
				}
				flagSet.Int64Var(f.Addr().Convert(reflect.TypeOf((*int64)(nil))).Interface().(*int64), fqFieldName, int64(f.Int()), docString)
			case reflect.Float64:
				flagSet.Float64Var(f.Addr().Convert(reflect.TypeOf((*float64)(nil))).Interface().(*float64), fqFieldName, f.Float(), docString)
			case reflect.Slice:
				if sf, err := flagutil.NewSliceFlag(f.Addr().Interface()); err == nil {
					flagSet.Var(sf, fqFieldName, docString)
					continue
				}
				fallthrough
			default:
				log.Warningf("Skipping flag: --%s, kind: %s", fqFieldName, f.Type().Kind())
			}
		}
	}
}

func populateYamlPresenceMap(yamlMap map[interface{}]interface{}) map[string]struct{} {
	var check func(key []string, value interface{})
	presenceMap := make(map[string]struct{})
	check = func(key []string, value interface{}) {
		if m, ok := value.(map[interface{}]interface{}); ok {
			for k, v := range m {
				if newKey, ok := k.(string); ok {
					check(append(key, strings.ToLower(newKey)), v)
					continue
				}
				log.Warningf("Non-string key encountered in yaml map in field: %s", key)
			}
			return
		}
		presenceMap[strings.Join(key, ".")] = struct{}{}
	}
	check([]string{}, yamlMap)
	return presenceMap
}

type Configurator struct {
	gc          *generalConfig
	presenceMap map[string]struct{}
	reconciled  bool
}

func NewConfiguratorFromData(data []byte) (*Configurator, error) {
	// expand environment variables
	expandedData := []byte(os.ExpandEnv(string(data)))

	// Unmarshal in strict mode once and warn about invalid fields.
	var syntaxValidationConfig generalConfig
	if err := yaml.UnmarshalStrict([]byte(expandedData), &syntaxValidationConfig); err != nil {
		log.Warningf("Unknown fields in config: %s", err)
	}

	gc := &generalConfig{}
	if err := yaml.Unmarshal([]byte(expandedData), gc); err != nil {
		return nil, fmt.Errorf("Error parsing config file: %s", err)
	}

	generalConfigMap := make(map[interface{}]interface{})
	if err := yaml.Unmarshal([]byte(expandedData), &generalConfigMap); err != nil {
		return nil, fmt.Errorf("Error parsing config file: %s", err)
	}

	// The shared config caches the last config parsed from a file so that a call to
	// `readConfig` with an empty `configFile` can return the cached config.
	sharedConfigurator = &Configurator{gc, populateYamlPresenceMap(generalConfigMap), false}

	return sharedConfigurator, nil
}

func NewConfigurator(configFilePath string) (*Configurator, error) {
	if configFilePath == "" {
		return sharedConfigurator, nil
	}
	log.Infof("Reading buildbuddy config from '%s'", configFilePath)

	_, err := os.Stat(configFilePath)

	// If the file does not exist then we are SOL.
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Config file %s not found", configFilePath)
	}

	fileBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	return NewConfiguratorFromData(fileBytes)
}

func ParseAndReconcileFlagsAndConfig(configFilePath string) (*Configurator, error) {
	RegisterAndParseFlags()
	if configFilePath == "" {
		configFilePath = *configFile
	}
	if configFilePath == "" {
		_, err := os.Stat("/config.yaml")
		if err == nil {
			configFilePath = "/config.yaml"
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, status.UnknownErrorf("could not load config file: %s", err)
		}
	}
	configurator, err := NewConfigurator(configFilePath)
	if err != nil {
		return nil, err
	}
	configurator.ReconcileFlagsAndConfig()
	return configurator, nil
}

type RedisClientConfig struct {
	SimpleTarget  string
	ShardedConfig *ShardedRedisConfig
}

func (cc *RedisClientConfig) String() string {
	if cc.ShardedConfig != nil {
		return "sharded across " + strings.Join(cc.ShardedConfig.Shards, ", ")
	}
	return cc.SimpleTarget
}

// After calling this function, the generalConfig in the Configurator and the
// flags in the default flag set (flag.Commandline) with a corresponding config
// value will be consistent.
func (c *Configurator) ReconcileFlagsAndConfig() {
	c.GenerateFlagSet().VisitAll(func(flg *flag.Flag) {
		if configSlice, ok := flg.Value.(flagutil.SliceFlag); ok {
			if flagSlice, ok := flag.Lookup(flg.Name).Value.(flagutil.SliceFlag); ok {
				if originalSliceLen, ok := originalSliceLens[flg.Name]; ok {
					// reset flagSlice if it has been modified
					// in this case, flagSliceLen is the sum of the length of the slices
					// defined in the config and those defined on the command line, and
					// the config slice comes first.
					flagSlice.SetTo(flagSlice.Slice(flagSlice.Len()-originalSliceLen, flagSlice.Len()))
					if c.reconciled {
						// reset configSlice if it has been modified
						configSlice.SetTo(configSlice.Slice(0, configSlice.Len()-originalSliceLen))
					}
				} else {
					originalSliceLens[flg.Name] = flagSlice.Len()
				}
				// Slices from flags are appended to the values in the config, as
				// opposed to one overriding the other, so no conflict check is needed.
				concatSlice := configSlice.AppendSlice(flagSlice.UnderlyingSlice())
				configSlice.SetTo(concatSlice)
				flagSlice.SetTo(concatSlice)
				return
			}
			log.Warningf("yaml defines %s as %T, but flags do not.", flg.Name, configSlice.UnderlyingSlice())
		}
		_, setInFlags := originalSetFlags[flg.Name]
		_, presentInYaml := c.presenceMap[flg.Name]
		if !setInFlags {
			// reset flag to default value if it was not initially set
			flag.Set(flg.Name, flag.Lookup(flg.Name).DefValue)
		}
		// If the flag was set on the command line or there is no value specified in
		// the config, we use the value defined by the flags and update the config
		// with that value. Otherwise (which is to say, if there was no flag
		// explicitly set on the command line and there is a value present in the
		// config), use the config value and update the flags with that value.
		if setInFlags || !presentInYaml {
			flg.Value.Set(flag.Lookup(flg.Name).Value.String())
			return
		}
		flag.Set(flg.Name, flg.Value.String())
	})
	c.reconciled = true
}

func (c *Configurator) GenerateFlagSet() *flag.FlagSet {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	defineFlagsForMembers([]string{}, reflect.ValueOf(c.gc).Elem(), flagSet)
	return flagSet
}

func (c *Configurator) GetCacheMaxSizeBytes() int64 {
	return c.gc.Cache.MaxSizeBytes
}

func (c *Configurator) GetCacheDiskConfig() *DiskConfig {
	if c.gc.Cache.Disk.RootDirectory != "" {
		return &c.gc.Cache.Disk
	}
	return nil
}

func (c *Configurator) GetCacheGCSConfig() *GCSCacheConfig {
	if c.gc.Cache.GCS.Bucket != "" {
		return &c.gc.Cache.GCS
	}
	return nil
}

func (c *Configurator) GetCacheS3Config() *S3CacheConfig {
	if c.gc.Cache.S3.Bucket != "" {
		return &c.gc.Cache.S3
	}
	return nil
}

func (c *Configurator) GetDistributedCacheConfig() *DistributedCacheConfig {
	if c.gc.Cache.DistributedCache.ListenAddr != "" {
		return &c.gc.Cache.DistributedCache
	}
	return nil
}

func (c *Configurator) GetRaftCacheConfig() *RaftCacheConfig {
	if c.gc.Cache.RaftCache.ListenAddr != "" {
		return &c.gc.Cache.RaftCache
	}
	return nil
}

func (c *Configurator) GetCacheMemcacheTargets() []string {
	return c.gc.Cache.MemcacheTargets
}

func (c *Configurator) GetCacheRedisClientConfig() *RedisClientConfig {
	// Prefer the client configs from Redis sub-config, is present.
	if len(c.gc.Cache.Redis.Sharded.Shards) > 0 {
		return &RedisClientConfig{ShardedConfig: &c.gc.Cache.Redis.Sharded}
	}
	if c.gc.Cache.Redis.RedisTarget != "" {
		return &RedisClientConfig{SimpleTarget: c.gc.Cache.Redis.RedisTarget}
	}

	if c.gc.Cache.RedisTarget != "" {
		return &RedisClientConfig{SimpleTarget: c.gc.Cache.RedisTarget}
	}

	return nil
}

func (c *Configurator) GetCacheRedisConfig() *RedisCacheConfig {
	if c.gc.Cache.Redis.RedisTarget != "" {
		return &c.gc.Cache.Redis
	}
	return nil
}

func (c *Configurator) GetCacheInMemory() bool {
	return c.gc.Cache.InMemory
}

func (c *Configurator) GetCacheZstdTranscodingEnabled() bool {
	return c.gc.Cache.ZstdTranscodingEnabled
}

func (c *Configurator) GetAnonymousUsageEnabled() bool {
	numOauthProviders := len(c.gc.Auth.OauthProviders)
	if c.GetSelfAuthEnabled() {
		// SelfAuth is considered an Oauth Provider
		numOauthProviders++
	}
	return numOauthProviders == 0 || c.gc.Auth.EnableAnonymousUsage
}

func (c *Configurator) GetAuthJWTKey() string {
	return c.gc.Auth.JWTKey
}

func (c *Configurator) GetAuthOauthProviders() []OauthProvider {
	op := c.gc.Auth.OauthProviders
	if len(c.gc.Auth.OauthProviders) == 1 {
		if cs := os.Getenv("BB_OAUTH_CLIENT_SECRET"); cs != "" {
			op[0].ClientSecret = cs
		}
	}
	return op
}

func (c *Configurator) GetAuthAPIKeyGroupCacheTTL() string {
	return c.gc.Auth.APIKeyGroupCacheTTL
}

func (c *Configurator) GetAuthAdminGroupID() string {
	return c.gc.Auth.AdminGroupID
}

func (c *Configurator) GetSAMLConfig() *SAMLConfig {
	return &c.gc.Auth.SAMLConfig
}

func (c *Configurator) GetSelfAuthEnabled() bool {
	return c.gc.Auth.EnableSelfAuth
}

func (c *Configurator) GetHttpsOnlyCookies() bool {
	return c.gc.Auth.HttpsOnlyCookies
}

func (c *Configurator) GetDisableRefreshToken() bool {
	return c.gc.Auth.DisableRefreshToken
}

func (c *Configurator) GetRemoteExecutionConfig() *RemoteExecutionConfig {
	if c.gc.RemoteExecution.EnableRemoteExec {
		return &c.gc.RemoteExecution
	}
	return nil
}

func (c *Configurator) GetRemoteExecutionRedisClientConfig() *RedisClientConfig {
	if rec := c.GetRemoteExecutionConfig(); rec != nil {
		if len(rec.ShardedRedis.Shards) > 0 {
			return &RedisClientConfig{ShardedConfig: &rec.ShardedRedis}
		}
		if rec.RedisTarget != "" {
			return &RedisClientConfig{SimpleTarget: rec.RedisTarget}
		}
	}

	// If no remote execution target is defined, use the default.
	if len(c.gc.App.DefaultShardedRedis.Shards) > 0 {
		return &RedisClientConfig{ShardedConfig: &c.gc.App.DefaultShardedRedis}
	}
	if c.gc.App.DefaultRedisTarget != "" {
		return &RedisClientConfig{SimpleTarget: c.gc.App.DefaultRedisTarget}
	}

	if c.GetCacheRedisClientConfig() != nil {
		// Fall back to the cache redis config if redis config is not specified in remote execution config or app config.
		// Historically we did not have a separate redis target for remote execution.
		return c.GetCacheRedisClientConfig()
	}

	return nil
}

func (c *Configurator) GetExecutorConfig() *ExecutorConfig {
	return &c.gc.Executor
}

func (c *Configurator) GetAPIConfig() *APIConfig {
	if c.gc.API.EnableAPI {
		return &c.gc.API
	}
	return nil
}
