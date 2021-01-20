package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"

	"gopkg.in/yaml.v2"
)

// When adding new storage fields, always be explicit about their yaml field
// name.
type generalConfig struct {
	App             appConfig             `yaml:"app"`
	BuildEventProxy buildEventProxy       `yaml:"build_event_proxy"`
	Database        DatabaseConfig        `yaml:"database"`
	Storage         storageConfig         `yaml:"storage"`
	Integrations    integrationsConfig    `yaml:"integrations"`
	Cache           cacheConfig           `yaml:"cache"`
	Auth            authConfig            `yaml:"auth"`
	SSL             SSLConfig             `yaml:"ssl"`
	RemoteExecution RemoteExecutionConfig `yaml:"remote_execution"`
	Executor        ExecutorConfig        `yaml:"executor"`
	API             APIConfig             `yaml:"api"`
	Github          GithubConfig          `yaml:"github"`
	Org             OrgConfig             `yaml:"org"`
}

type appConfig struct {
	BuildBuddyURL           string `yaml:"build_buddy_url" usage:"The external URL where your BuildBuddy instance can be found."`
	EventsAPIURL            string `yaml:"events_api_url" usage:"Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen."`
	CacheAPIURL             string `yaml:"cache_api_url" usage:"Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen."`
	RemoteExecutionAPIURL   string `yaml:"remote_execution_api_url" usage:"Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen."`
	NoDefaultUserGroup      bool   `yaml:"no_default_user_group" usage:"Cloud-Only"`
	CreateGroupPerUser      bool   `yaml:"create_group_per_user" usage:"Cloud-Only"`
	AddUserToDomainGroup    bool   `yaml:"add_user_to_domain_group" usage:"Cloud-Only"`
	GRPCOverHTTPPortEnabled bool   `yaml:"grpc_over_http_port_enabled" usage:"Cloud-Only"`
	DefaultToDenseMode      bool   `yaml:"default_to_dense_mode" usage:"Enables the dense UI mode by default."`
	GRPCMaxRecvMsgSizeBytes int    `yaml:"grpc_max_recv_msg_size_bytes" usage:"Configures the max GRPC receive message size [bytes]"`
	EnableTargetTracking    bool   `yaml:"enable_target_tracking" usage:"Cloud-Only"`
}

type buildEventProxy struct {
	Hosts []string `yaml:"hosts" usage:"The list of hosts to pass build events onto."`
}

type DatabaseConfig struct {
	DataSource             string `yaml:"data_source" usage:"The SQL database to connect to, specified as a connection string."`
	ReadReplica            string `yaml:"read_replica" usage:"A secondary, read-only SQL database to connect to, specified as a connection string."`
	MaxOpenConns           int    `yaml:"max_open_conns" usage:"The maximum number of open connections to maintain to the db"`
	MaxIdleConns           int    `yaml:"max_idle_conns" usage:"The maximum number of idle connections to maintain to the db"`
	ConnMaxLifetimeSeconds int    `yaml:"conn_max_lifetime_seconds" usage:"The maximum lifetime of a connection to the db"`
	LogQueries             bool   `yaml:"log_queries" usage:"If true, log all queries"`
	StatsPollInterval      string `yaml:"stats_poll_interval" usage:"How often to poll the DB client for connection stats (default: "5s")."`
}

type storageConfig struct {
	Disk               DiskConfig  `yaml:"disk"`
	GCS                GCSConfig   `yaml:"gcs"`
	AwsS3              AwsS3Config `yaml:"aws_s3"`
	TTLSeconds         int         `yaml:"ttl_seconds" usage:"The time, in seconds, to keep invocations before deletion"`
	ChunkFileSizeBytes int         `yaml:"chunk_file_size_bytes" usage:"How many bytes to buffer in memory before flushing a chunk of build protocol data to disk."`
}

type DiskConfig struct {
	RootDirectory string `yaml:"root_directory" usage:"The root directory to store all blobs in, if using disk based storage."`
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
}

type DistributedDiskConfig struct {
	RootDirectory string `yaml:"root_directory" usage:"The root directory to store all blobs in, if using disk based storage."`
	ListenAddr    string `yaml:"listen_addr" usage:"The address to listen for local BuildBuddy distributed cache traffic on."`
	RedisTarget   string `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. ** Enterprise only **"`
	GroupName     string `yaml:"group_name" usage:"A unique name for this distributed cache group. ** Enterprise only **"`
}

type cacheConfig struct {
	Disk            DiskConfig            `yaml:"disk"`
	GCS             GCSCacheConfig        `yaml:"gcs"`
	S3              S3CacheConfig         `yaml:"s3"`
	DistributedDisk DistributedDiskConfig `yaml:"distributed_disk"`
	InMemory        bool                  `yaml:"in_memory" usage:"Whether or not to use the in_memory cache."`
	MaxSizeBytes    int64                 `yaml:"max_size_bytes" usage:"How big to allow the cache to be (in bytes)."`
	MemcacheTargets []string              `yaml:"memcache_targets" usage:"Deprecated. Use Redis Target instead."`
	RedisTarget     string                `yaml:"redis_target" usage:"A redis target for improved Caching/RBE performance. ** Enterprise only **"`
}

type authConfig struct {
	OauthProviders       []OauthProvider `yaml:"oauth_providers"`
	EnableAnonymousUsage bool            `yaml:"enable_anonymous_usage" usage:"If true, unauthenticated build uploads will still be allowed but won't be associated with your organization."`
	JWTKey               string          `yaml:"jwt_key" usage:"The key to use when signing JWT tokens."`
}

type OauthProvider struct {
	IssuerURL    string `yaml:"issuer_url" usage:"The issuer URL of this OIDC Provider."`
	ClientID     string `yaml:"client_id" usage:"The oauth client ID."`
	ClientSecret string `yaml:"client_secret" usage:""The oauth client secret.`
}

type SSLConfig struct {
	EnableSSL        bool     `yaml:"enable_ssl" usage:"Whether or not to enable SSL/TLS on gRPC connections (gRPCS)."`
	UseACME          bool     `yaml:"use_acme" usage:"Whether or not to automatically configure SSL certs using ACME. If ACME is enabled, cert_file and key_file should not be set."`
	CertFile         string   `yaml:"cert_file" usage:"Path to a PEM encoded certificate file to use for TLS if not using ACME."`
	KeyFile          string   `yaml:"key_file" usage:"Path to a PEM encoded key file to use for TLS if not using ACME."`
	ClientCACertFile string   `yaml:"client_ca_cert_file" usage:"Path to a PEM encoded certificate authority file used to issue client certificates for mTLS auth."`
	ClientCAKeyFile  string   `yaml:"client_ca_key_file" usage:"Path to a PEM encoded certificate authority key file used to issue client certificates for mTLS auth."`
	UpgradeInsecure  bool     `yaml:"upgrade_insecure" usage:"True if http requests should be redirected to https"`
	HostWhitelist    []string `yaml:"host_whitelist" usage:"Cloud-Only"`
}

type RemoteExecutionConfig struct {
	EnableRemoteExec bool   `yaml:"enable_remote_exec" usage:"If true, enable remote-exec. ** Enterprise only **"`
	DefaultPoolName  string `yaml:"default_pool_name" usage:"The default executor pool to use if one is not specified."`
}

type ExecutorConfig struct {
	AppTarget           string `yaml:"app_target" usage:"The GRPC url of a buildbuddy app server."`
	RootDirectory       string `yaml:"root_directory" usage:"The root directory to use for build files."`
	LocalCacheDirectory string `yaml:"local_cache_directory" usage:"A local on-disk cache directory."`
	LocalCacheSizeBytes int64  `yaml:"local_cache_size_bytes" usage:"The maximum size, in bytes, to use for the local on-disk cache"`
	DockerSocket        string `yaml:"docker_socket" usage:"If set, run execution commands in docker using the provided socket."`
	ContainerdSocket    string `yaml:"containerd_socket" usage:"(UNSTABLE) If set, run execution commands in containerd using the provided socket."`
}

type APIConfig struct {
	EnableAPI bool   `yaml:"enable_api" usage:"Whether or not to enable the BuildBuddy API."`
	APIKey    string `yaml:"api_key" usage:"The default API key to use for on-prem enterprise deploys with a single organization/group."`
}

type GithubConfig struct {
	ClientID            string `yaml:"client_id" usage:"The client ID of your GitHub Oauth App. ** Enterprise only **"`
	ClientSecret        string `yaml:"client_secret" usage:"The client secret of your GitHub Oauth App. ** Enterprise only **"`
	AccessToken         string `yaml:"access_token" usage:"The GitHub access token used to post GitHub commit statuses. ** Enterprise only **"`
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

func (i *stringSliceFlag) Set(values string) error {
	for _, val := range strings.Split(values, ",") {
		*i = append(*i, val)
	}
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
		case reflect.Slice:
			if f.Type().Elem().Kind() == reflect.String {
				if slice, ok := f.Interface().([]string); ok {
					sf := stringSliceFlag(slice)
					flag.Var(&sf, fqFieldName, docString)
				}
				continue
			}
			fallthrough
		default:
			// We know this is not flag compatible and it's here for
			// long-term support reasons, so don't warn about it.
			if fqFieldName != "auth.oauth_providers" {
				log.Printf("Skipping flag: --%s, kind: %s", fqFieldName, f.Type().Kind())
			}
			continue
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
	log.Printf("Reading buildbuddy config from '%s'", fullConfigPath)

	_, err := os.Stat(fullConfigPath)

	// If the file does not exist then we are SOL.
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Config file %s not found", fullConfigPath)
	}

	fileBytes, err := ioutil.ReadFile(fullConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	// expand environment variables
	expandedFileBytes := []byte(os.ExpandEnv(string(fileBytes)))

	if err := yaml.Unmarshal([]byte(expandedFileBytes), &sharedGeneralConfig); err != nil {
		return nil, fmt.Errorf("Error parsing config file: %s", err)
	}
	return &sharedGeneralConfig, nil
}

type Configurator struct {
	fullConfigPath string
	gc             *generalConfig
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

func (c *Configurator) GetGRPCOverHTTPPortEnabled() bool {
	return c.gc.App.GRPCOverHTTPPortEnabled
}

func (c *Configurator) GetDefaultToDenseMode() bool {
	return c.gc.App.DefaultToDenseMode
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

func (c *Configurator) GetDistributedDiskConfig() *DistributedDiskConfig {
	if c.gc.Cache.DistributedDisk.RootDirectory != "" {
		return &c.gc.Cache.DistributedDisk
	}
	return nil
}

func (c *Configurator) GetCacheMemcacheTargets() []string {
	return c.gc.Cache.MemcacheTargets
}

func (c *Configurator) GetCacheRedisTarget() string {
	return c.gc.Cache.RedisTarget
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

func (c *Configurator) GetExecutorConfig() *ExecutorConfig {
	if c.gc.Executor.RootDirectory != "" {
		return &c.gc.Executor
	}
	return nil
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
