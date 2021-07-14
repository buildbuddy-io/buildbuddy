package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"gopkg.in/yaml.v2"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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
	BuildBuddyURL             string   `yaml:"build_buddy_url" usage:"The external URL where your BuildBuddy instance can be found."`
	EventsAPIURL              string   `yaml:"events_api_url" usage:"Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen."`
	CacheAPIURL               string   `yaml:"cache_api_url" usage:"Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen."`
	RemoteExecutionAPIURL     string   `yaml:"remote_execution_api_url" usage:"Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen."`
	LogLevel                  string   `yaml:"log_level" usage:"The desired log level. Logs with a level >= this level will be emitted. One of {'fatal', 'error', 'warn', 'info', 'debug'}"`
	GRPCMaxRecvMsgSizeBytes   int      `yaml:"grpc_max_recv_msg_size_bytes" usage:"Configures the max GRPC receive message size [bytes]"`
	GRPCOverHTTPPortEnabled   bool     `yaml:"grpc_over_http_port_enabled" usage:"Cloud-Only"`
	AddUserToDomainGroup      bool     `yaml:"add_user_to_domain_group" usage:"Cloud-Only"`
	DefaultToDenseMode        bool     `yaml:"default_to_dense_mode" usage:"Enables the dense UI mode by default."`
	CreateGroupPerUser        bool     `yaml:"create_group_per_user" usage:"Cloud-Only"`
	EnableTargetTracking      bool     `yaml:"enable_target_tracking" usage:"Cloud-Only"`
	EnableStructuredLogging   bool     `yaml:"enable_structured_logging" usage:"If true, log messages will be json-formatted."`
	LogIncludeShortFileName   bool     `yaml:"log_include_short_file_name" usage:"If true, log messages will include shortened originating file name."`
	NoDefaultUserGroup        bool     `yaml:"no_default_user_group" usage:"Cloud-Only"`
	LogEnableGCPLoggingFormat bool     `yaml:"log_enable_gcp_logging_format" usage:"If true, the output structured logs will be compatible with format expected by GCP Logging."`
	LogErrorStackTraces       bool     `yaml:"log_error_stack_traces" usage:"If true, stack traces will be printed for errors that have them."`
	TraceProjectID            string   `yaml:"trace_project_id" usage:"Optional GCP project ID to export traces to. If not specified, determined from default credentials or metadata server if running on GCP."`
	TraceJaegerCollector      string   `yaml:"trace_jaeger_collector" usage:"Address of the Jager collector endpoint where traces will be sent."`
	TraceServiceName          string   `yaml:"trace_service_name" usage:"Name of the service to associate with traces."`
	TraceFraction             float64  `yaml:"trace_fraction" usage:"Fraction of requests to sample for tracing."`
	TraceFractionOverrides    []string `yaml:"trace_fraction_overrides" usage:"Tracing fraction override based on name in format name=fraction."`
	IgnoreForcedTracingHeader bool     `yaml:"ignore_forced_tracing_header" usage:"If set, we will not honor the forced tracing header."`
	CodeEditorEnabled         bool     `yaml:"code_editor_enabled" usage:"If set, code editor functionality will be enabled."`
}

type buildEventProxy struct {
	Hosts      []string `yaml:"hosts" usage:"The list of hosts to pass build events onto."`
	BufferSize int      `yaml:"buffer_size" usage:"The number of build events to buffer locally when proxying build events."`
}

type DatabaseConfig struct {
	DataSource             string `yaml:"data_source" usage:"The SQL database to connect to, specified as a connection string."`
	ReadReplica            string `yaml:"read_replica" usage:"A secondary, read-only SQL database to connect to, specified as a connection string."`
	StatsPollInterval      string `yaml:"stats_poll_interval" usage:"How often to poll the DB client for connection stats (default: '5s')."`
	MaxOpenConns           int    `yaml:"max_open_conns" usage:"The maximum number of open connections to maintain to the db"`
	MaxIdleConns           int    `yaml:"max_idle_conns" usage:"The maximum number of idle connections to maintain to the db"`
	ConnMaxLifetimeSeconds int    `yaml:"conn_max_lifetime_seconds" usage:"The maximum lifetime of a connection to the db"`
	LogQueries             bool   `yaml:"log_queries" usage:"If true, log all queries"`
}

type storageConfig struct {
	Disk                   DiskConfig  `yaml:"disk"`
	GCS                    GCSConfig   `yaml:"gcs"`
	AwsS3                  AwsS3Config `yaml:"aws_s3"`
	TTLSeconds             int         `yaml:"ttl_seconds" usage:"The time, in seconds, to keep invocations before deletion"`
	ChunkFileSizeBytes     int         `yaml:"chunk_file_size_bytes" usage:"How many bytes to buffer in memory before flushing a chunk of build protocol data to disk."`
	EnableChunkedEventLogs bool        `yaml:"enable_chunked_event_logs" usage:"If true, Event logs will be stored separately from the invocation proto in chunks."`
}

type DiskCachePartition struct {
	ID           string `yaml:"id" json:"id" usage:"The ID of the partition."`
	MaxSizeBytes int64  `yaml:"max_size" json:"max_size_bytes" usage:"Maximum size of the partition."`
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

	// Useful for configuring MinIO: https://docs.min.io/docs/how-to-use-aws-sdk-for-go-with-minio-server.html
	Endpoint                string `yaml:"endpoint" usage:"The AWS endpoint to use, useful for configuring the use of MinIO."`
	StaticCredentialsID     string `yaml:"static_credentials_id" usage:"Static credentials ID to use, useful for configuring the use of MinIO."`
	StaticCredentialsSecret string `yaml:"static_credentials_secret" usage:"Static credentials secret to use, useful for configuring the use of MinIO."`
	StaticCredentialsToken  string `yaml:"static_credentials_token" usage:"Static credentials token to use, useful for configuring the use of MinIO."`
	DisableSSL              bool   `yaml:"disable_ssl" usage:"Disables the use of SSL, useful for configuring the use of MinIO."`
	S3ForcePathStyle        bool   `yaml:"s3_force_path_style" usage:"Force path style urls for objects, useful for configuring the use of MinIO."`
}

type integrationsConfig struct {
	Slack SlackConfig `yaml:"slack"`
}

type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url" usage:"A Slack webhook url to post build update messages to."`
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

type RedisCacheConfig struct {
	RedisTarget       string `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **"`
	MaxValueSizeBytes int64  `yaml:"max_value_size_bytes" usage:"The maximum value size to cache in redis (in bytes)."`
}

type cacheConfig struct {
	Disk             DiskConfig             `yaml:"disk"`
	RedisTarget      string                 `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **"`
	S3               S3CacheConfig          `yaml:"s3"`
	GCS              GCSCacheConfig         `yaml:"gcs"`
	MemcacheTargets  []string               `yaml:"memcache_targets" usage:"Deprecated. Use Redis Target instead."`
	Redis            RedisCacheConfig       `yaml:"redis"`
	DistributedCache DistributedCacheConfig `yaml:"distributed_cache"`
	MaxSizeBytes     int64                  `yaml:"max_size_bytes" usage:"How big to allow the cache to be (in bytes)."`
	InMemory         bool                   `yaml:"in_memory" usage:"Whether or not to use the in_memory cache."`
}

type authConfig struct {
	JWTKey               string          `yaml:"jwt_key" usage:"The key to use when signing JWT tokens."`
	APIKeyGroupCacheTTL  string          `yaml:"api_key_group_cache_ttl" usage:"Override for the TTL for API Key to Group caching. Set to '0' to disable cache."`
	OauthProviders       []OauthProvider `yaml:"oauth_providers"`
	EnableAnonymousUsage bool            `yaml:"enable_anonymous_usage" usage:"If true, unauthenticated build uploads will still be allowed but won't be associated with your organization."`
}

type OauthProvider struct {
	IssuerURL    string `yaml:"issuer_url" usage:"The issuer URL of this OIDC Provider."`
	ClientID     string `yaml:"client_id" usage:"The oauth client ID."`
	ClientSecret string `yaml:"client_secret" usage:"The oauth client secret."`
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

type RemoteExecutionConfig struct {
	DefaultPoolName               string `yaml:"default_pool_name" usage:"The default executor pool to use if one is not specified."`
	EnableWorkflows               bool   `yaml:"enable_workflows" usage:"Whether to enable BuildBuddy workflows."`
	WorkflowsPoolName             string `yaml:"workflows_pool_name" usage:"The executor pool to use for workflow actions. Defaults to the default executor pool if not specified."`
	WorkflowsDefaultImage         string `yaml:"workflows_default_image" usage:"The default docker image to use for running workflows."`
	WorkflowsCIRunnerDebug        bool   `yaml:"workflows_ci_runner_debug" usage:"Whether to run the CI runner in debug mode."`
	WorkflowsCIRunnerBazelCommand string `yaml:"workflows_ci_runner_bazel_command" usage:"Bazel command to be used by the CI runner."`
	RedisTarget                   string `yaml:"redis_target" usage:"A Redis target for storing remote execution state. Required for remote execution. To ease migration, the redis target from the cache config will be used if this value is not specified."`
	SharedExecutorPoolGroupID     string `yaml:"shared_executor_pool_group_id" usage:"Group ID that owns the shared executor pool."`
	RedisPubSubPoolSize           int    `yaml:"redis_pubsub_pool_size" usage:"Maximum number of connections used for waiting for execution updates."`
	EnableRemoteExec              bool   `yaml:"enable_remote_exec" usage:"If true, enable remote-exec. ** Enterprise only **"`
	RequireExecutorAuthorization  bool   `yaml:"require_executor_authorization" usage:"If true, executors connecting to this server must provide a valid executor API key."`
	EnableUserOwnedExecutors      bool   `yaml:"enable_user_owned_executors" usage:"If enabled, users can register their own executors with the scheduler."`
	EnableExecutorKeyCreation     bool   `yaml:"enable_executor_key_creation" usage:"If enabled, UI will allow executor keys to be created."`
}

type ExecutorConfig struct {
	AppTarget               string           `yaml:"app_target" usage:"The GRPC url of a buildbuddy app server."`
	RootDirectory           string           `yaml:"root_directory" usage:"The root directory to use for build files."`
	LocalCacheDirectory     string           `yaml:"local_cache_directory" usage:"A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result."`
	LocalCacheSizeBytes     int64            `yaml:"local_cache_size_bytes" usage:"The maximum size, in bytes, to use for the local on-disk cache"`
	DisableLocalCache       bool             `yaml:"disable_local_cache" usage:"If true, a local file cache will not be used."`
	DockerSocket            string           `yaml:"docker_socket" usage:"If set, run execution commands in docker using the provided socket."`
	APIKey                  string           `yaml:"api_key" usage:"API Key used to authorize the executor with the BuildBuddy app server."`
	ContainerdSocket        string           `yaml:"containerd_socket" usage:"(UNSTABLE) If set, run execution commands in containerd using the provided socket."`
	DockerMountMode         string           `yaml:"docker_mount_mode" usage:"Sets the mount mode of volumes mounted to docker images. Useful if running on SELinux https://www.projectatomic.io/blog/2015/06/using-volumes-with-docker-can-cause-problems-with-selinux/"`
	RunnerPool              RunnerPoolConfig `yaml:"runner_pool"`
	DockerNetHost           bool             `yaml:"docker_net_host" usage:"Sets --net=host on the docker command. Intended for local development only."`
	DockerSiblingContainers bool             `yaml:"docker_sibling_containers" usage:"If set, mount the configured Docker socket to containers spawned for each action, to enable Docker-out-of-Docker (DooD). Takes effect only if docker_socket is also set. Should not be set by executors that can run untrusted code."`
	DefaultXCodeVersion     string           `yaml:"default_xcode_version" usage:"Sets the default XCode version number to use if an action doesn't specify one. If not set, /Applications/Xcode.app/ is used."`
}

func (c *ExecutorConfig) GetAppTarget() string {
	if c.AppTarget == "" {
		return "grpcs://cloud.buildbuddy.io"
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

var sharedGeneralConfig generalConfig

type stringSliceFlag []string

func (i *stringSliceFlag) String() string {
	return strings.Join(*i, ",")
}

// NOTE: string slice flags are *appended* to the values in the YAML,
// instead of overriding them completely.

func (i *stringSliceFlag) Set(values string) error {
	for _, val := range strings.Split(values, ",") {
		*i = append(*i, val)
	}
	return nil
}

type structSliceFlag struct {
	dstSlice   reflect.Value
	structType reflect.Type
}

func (f *structSliceFlag) String() string {
	if *f == (structSliceFlag{}) {
		return "[]"
	}

	var l []string
	for i := 0; i < f.dstSlice.Len(); i++ {
		b, err := json.Marshal(f.dstSlice.Index(i).Interface())
		if err != nil {
			alert.UnexpectedEvent("config_cannot_marshal_struct", "err: %s", err)
			continue
		}
		l = append(l, string(b))
	}
	return "[" + strings.Join(l, ",") + "]"
}

func (f *structSliceFlag) Set(value string) error {
	dst := reflect.New(f.structType)
	if err := json.Unmarshal([]byte(value), dst.Interface()); err != nil {
		return err
	}
	f.dstSlice.Set(reflect.Append(f.dstSlice, dst.Elem()))
	return nil
}

func defineFlagsForMembers(parentStructNames []string, T reflect.Value) {
	typeOfT := T.Type()
	for i := 0; i < T.NumField(); i++ {
		f := T.Field(i)
		fieldName := typeOfT.Field(i).Tag.Get("yaml")
		docString := typeOfT.Field(i).Tag.Get("usage")
		fqFieldName := strings.ToLower(strings.Join(append(parentStructNames, fieldName), "."))

		switch f.Type().Kind() {
		case reflect.Ptr:
			log.Fatal("The config should not contain pointers!")
		case reflect.Struct:
			defineFlagsForMembers(append(parentStructNames, fieldName), f)
			continue
		case reflect.Bool:
			flag.BoolVar(f.Addr().Interface().(*bool), fqFieldName, f.Bool(), docString)
		case reflect.String:
			flag.StringVar(f.Addr().Interface().(*string), fqFieldName, f.String(), docString)
		case reflect.Int:
			flag.IntVar(f.Addr().Interface().(*int), fqFieldName, int(f.Int()), docString)
		case reflect.Int64:
			flag.Int64Var(f.Addr().Interface().(*int64), fqFieldName, int64(f.Int()), docString)
		case reflect.Float64:
			flag.Float64Var(f.Addr().Interface().(*float64), fqFieldName, f.Float(), docString)
		case reflect.Slice:
			if f.Type().Elem().Kind() == reflect.String {
				if slice, ok := f.Interface().([]string); ok {
					sf := stringSliceFlag(slice)
					flag.Var(&sf, fqFieldName, docString)
				}
				continue
			} else if f.Type().Elem().Kind() == reflect.Struct {
				sf := structSliceFlag{f, f.Type().Elem()}
				flag.Var(&sf, fqFieldName, docString)
				continue
			}
			fallthrough
		default:
			// We know this is not flag compatible and it's here for
			// long-term support reasons, so don't warn about it.
			if fqFieldName == "auth.oauth_providers" {
				continue
			}
			log.Warningf("Skipping flag: --%s, kind: %s", fqFieldName, f.Type().Kind())
		}
	}
}

// Register flags too.
func init() {
	defineFlagsForMembers([]string{}, reflect.ValueOf(&sharedGeneralConfig).Elem())
}

func readConfig(fullConfigPath string) (*generalConfig, error) {
	if fullConfigPath == "" {
		return &sharedGeneralConfig, nil
	}
	log.Infof("Reading buildbuddy config from '%s'", fullConfigPath)

	_, err := os.Stat(fullConfigPath)

	// If the file does not exist then we are SOL.
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Config file %s not found", fullConfigPath)
	}

	fileBytes, err := os.ReadFile(fullConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	// expand environment variables
	expandedFileBytes := []byte(os.ExpandEnv(string(fileBytes)))

	if err := yaml.Unmarshal([]byte(expandedFileBytes), &sharedGeneralConfig); err != nil {
		return nil, fmt.Errorf("Error parsing config file: %s", err)
	}

	// Re-parse flags so that they override the YAML config values.
	flag.Parse()

	return &sharedGeneralConfig, nil
}

type Configurator struct {
	gc             *generalConfig
	fullConfigPath string
}

func NewConfigurator(configFilePath string) (*Configurator, error) {
	conf, err := readConfig(configFilePath)
	if err != nil {
		return nil, err
	}
	return &Configurator{
		fullConfigPath: configFilePath,
		gc:             conf,
	}, nil
}

func (c *Configurator) GetStorageEnableChunkedEventLogs() bool {
	return c.gc.Storage.EnableChunkedEventLogs
}

func (c *Configurator) GetStorageTTLSeconds() int {
	return c.gc.Storage.TTLSeconds
}

func (c *Configurator) GetStorageChunkFileSizeBytes() int {
	return c.gc.Storage.ChunkFileSizeBytes
}

func (c *Configurator) GetStorageDiskRootDir() string {
	return c.gc.Storage.Disk.RootDirectory
}

func (c *Configurator) GetStorageGCSConfig() *GCSConfig {
	return &c.gc.Storage.GCS
}

func (c *Configurator) GetStorageAWSS3Config() *AwsS3Config {
	return &c.gc.Storage.AwsS3
}

func (c *Configurator) GetDatabaseConfig() *DatabaseConfig {
	return &c.gc.Database
}

func (c *Configurator) GetDBDataSource() string {
	return c.gc.Database.DataSource
}

func (c *Configurator) GetDBReadReplica() string {
	return c.gc.Database.ReadReplica
}

func (c *Configurator) GetAppBuildBuddyURL() string {
	return c.gc.App.BuildBuddyURL
}

func (c *Configurator) GetAppEventsAPIURL() string {
	return c.gc.App.EventsAPIURL
}

func (c *Configurator) GetAppCacheAPIURL() string {
	return c.gc.App.CacheAPIURL
}

func (c *Configurator) GetAppRemoteExecutionAPIURL() string {
	return c.gc.App.RemoteExecutionAPIURL
}

func (c *Configurator) GetAppNoDefaultUserGroup() bool {
	return c.gc.App.NoDefaultUserGroup
}

func (c *Configurator) GetAppCreateGroupPerUser() bool {
	return c.gc.App.CreateGroupPerUser
}

func (c *Configurator) GetAppAddUserToDomainGroup() bool {
	return c.gc.App.AddUserToDomainGroup
}

func (c *Configurator) GetAppLogIncludeShortFileName() bool {
	return c.gc.App.LogIncludeShortFileName
}

func (c *Configurator) GetAppLogErrorStackTraces() bool {
	return c.gc.App.LogErrorStackTraces
}

func (c *Configurator) GetAppEnableStructuredLogging() bool {
	return c.gc.App.EnableStructuredLogging
}

func (c *Configurator) GetAppLogLevel() string {
	return c.gc.App.LogLevel
}

func (c *Configurator) GetAppLogEnableGCPLoggingFormat() bool {
	return c.gc.App.LogEnableGCPLoggingFormat
}

func (c *Configurator) GetGRPCOverHTTPPortEnabled() bool {
	return c.gc.App.GRPCOverHTTPPortEnabled
}

func (c *Configurator) GetDefaultToDenseMode() bool {
	return c.gc.App.DefaultToDenseMode
}

func (c *Configurator) GetCodeEditorEnabled() bool {
	return c.gc.App.CodeEditorEnabled
}

func (c *Configurator) GetGRPCMaxRecvMsgSizeBytes() int {
	n := c.gc.App.GRPCMaxRecvMsgSizeBytes
	if n == 0 {
		// Support large BEP messages: https://github.com/bazelbuild/bazel/issues/12050
		return 50000000
	}
	return n
}

func (c *Configurator) EnableTargetTracking() bool {
	return c.gc.App.EnableTargetTracking
}

func (c *Configurator) GetIntegrationsSlackConfig() *SlackConfig {
	return &c.gc.Integrations.Slack
}

func (c *Configurator) GetBuildEventProxyHosts() []string {
	return c.gc.BuildEventProxy.Hosts
}

func (c *Configurator) GetBuildEventProxyBufferSize() int {
	return c.gc.BuildEventProxy.BufferSize
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

func (c *Configurator) GetCacheMemcacheTargets() []string {
	return c.gc.Cache.MemcacheTargets
}

func (c *Configurator) GetCacheRedisTarget() string {
	// Prefer the target from Redis sub-config, is present.
	if redisConfig := c.GetCacheRedisConfig(); redisConfig != nil {
		return redisConfig.RedisTarget
	}
	return c.gc.Cache.RedisTarget
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

func (c *Configurator) GetAnonymousUsageEnabled() bool {
	return len(c.gc.Auth.OauthProviders) == 0 || c.gc.Auth.EnableAnonymousUsage
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

func (c *Configurator) GetSSLConfig() *SSLConfig {
	if c.gc.SSL.EnableSSL {
		return &c.gc.SSL
	}
	return nil
}

func (c *Configurator) GetRemoteExecutionConfig() *RemoteExecutionConfig {
	if c.gc.RemoteExecution.EnableRemoteExec {
		return &c.gc.RemoteExecution
	}
	return nil
}

func (c *Configurator) GetRemoteExecutionRedisTarget() string {
	if rec := c.GetRemoteExecutionConfig(); rec != nil && rec.RedisTarget != "" {
		return rec.RedisTarget
	}
	// Fall back to the cache redis target if redis target is not specified in remote execution config.
	// Historically we did not have a separate redis target for remote execution.
	return c.GetCacheRedisTarget()
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

func (c *Configurator) GetGithubConfig() *GithubConfig {
	if c.gc.Github == (GithubConfig{}) {
		return nil
	}
	ghc := c.gc.Github
	if cs := os.Getenv("BB_GITHUB_CLIENT_SECRET"); cs != "" {
		ghc.ClientSecret = cs
	}
	return &ghc
}

func (c *Configurator) GetOrgConfig() *OrgConfig {
	if c.gc.Org.Name != "" || c.gc.Org.Domain != "" {
		return &c.gc.Org
	}
	return nil
}

func (c *Configurator) GetTraceJaegerCollector() string {
	return c.gc.App.TraceJaegerCollector
}

func (c *Configurator) GetTraceServiceName() string {
	return c.gc.App.TraceServiceName
}

func (c *Configurator) GetTraceFraction() float64 {
	return c.gc.App.TraceFraction
}

func (c *Configurator) GetTraceFractionOverrides() []string {
	return c.gc.App.TraceFractionOverrides
}

func (c *Configurator) GetIgnoreForcedTracingHeader() bool {
	return c.gc.App.IgnoreForcedTracingHeader
}
