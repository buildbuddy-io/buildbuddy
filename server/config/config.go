package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// When adding new storage fields, always be explicit about their yaml field
// name.
type generalConfig struct {
	App             appConfig              `yaml:"app"`
	BuildEventProxy buildEventProxy        `yaml:"build_event_proxy"`
	Database        databaseConfig         `yaml:"database"`
	Storage         storageConfig          `yaml:"storage"`
	Integrations    integrationsConfig     `yaml:"integrations"`
	Cache           cacheConfig            `yaml:"cache"`
	Auth            authConfig             `yaml:"auth"`
	SSL             *SSLConfig             `yaml:"ssl"`
	RemoteExecution *RemoteExecutionConfig `yaml:"remote_execution"`
	Executor        *ExecutorConfig        `yaml:"executor"`
	API             *APIConfig             `yaml:"api"`
	Github          *GithubConfig          `yaml:"github"`
	Org             *OrgConfig             `yaml:"org"`
}

type appConfig struct {
	BuildBuddyURL           string `yaml:"build_buddy_url"`
	EventsAPIURL            string `yaml:"events_api_url"`
	CacheAPIURL             string `yaml:"cache_api_url"`
	RemoteExecutionAPIURL   string `yaml:"remote_execution_api_url"`
	NoDefaultUserGroup      bool   `yaml:"no_default_user_group"`
	CreateGroupPerUser      bool   `yaml:"create_group_per_user"`
	AddUserToDomainGroup    bool   `yaml:"add_user_to_domain_group"`
	GRPCOverHTTPPortEnabled bool   `yaml:"grpc_over_http_port_enabled"`
	DefaultToDenseMode      bool   `yaml:"default_to_dense_mode"`
	GRPCMaxRecvMsgSizeBytes int    `yaml:"grpc_max_recv_msg_size_bytes"`
}

type buildEventProxy struct {
	Hosts []string `yaml:"hosts"`
}

type databaseConfig struct {
	DataSource string `yaml:"data_source"`
}

type storageConfig struct {
	Disk               DiskConfig  `yaml:"disk"`
	GCS                GCSConfig   `yaml:"gcs"`
	AwsS3              AwsS3Config `yaml:"aws_s3"`
	TTLSeconds         int         `yaml:"ttl_seconds"`
	ChunkFileSizeBytes int         `yaml:"chunk_file_size_bytes"`
}

type DiskConfig struct {
	RootDirectory string `yaml:"root_directory"`
}

type GCSConfig struct {
	Bucket          string `yaml:"bucket"`
	CredentialsFile string `yaml:"credentials_file"`
	ProjectID       string `yaml:"project_id"`
}

type AwsS3Config struct {
	Region             string `yaml:"region"`
	Bucket             string `yaml:"bucket"`
	CredentialsProfile string `yaml:"credentials_profile"`
}

type integrationsConfig struct {
	Slack SlackConfig `yaml:"slack"`
}

type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url"`
}

type GCSCacheConfig struct {
	Bucket          string `yaml:"bucket"`
	CredentialsFile string `yaml:"credentials_file"`
	ProjectID       string `yaml:"project_id"`
	TTLDays         int64  `yaml:"ttl_days"`
}

type S3CacheConfig struct {
	Region             string `yaml:"region"`
	Bucket             string `yaml:"bucket"`
	CredentialsProfile string `yaml:"credentials_profile"`
	TTLDays            int64  `yaml:"ttl_days"`
}

type cacheConfig struct {
	Disk            *DiskConfig     `yaml:"disk"`
	GCS             *GCSCacheConfig `yaml:"gcs"`
	S3              *S3CacheConfig  `yaml:"s3"`
	InMemory        bool            `yaml:"in_memory"`
	MaxSizeBytes    int64           `yaml:"max_size_bytes"`
	MemcacheTargets []string        `yaml:"memcache_targets"`
}

type authConfig struct {
	OauthProviders       []*OauthProvider `yaml:"oauth_providers"`
	EnableAnonymousUsage bool             `yaml:"enable_anonymous_usage"`
}

type OauthProvider struct {
	IssuerURL    string `yaml:"issuer_url"`
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
}

type SSLConfig struct {
	EnableSSL        bool     `yaml:"enable_ssl"`
	UseACME          bool     `yaml:"use_acme"`
	CertFile         string   `yaml:"cert_file"`
	KeyFile          string   `yaml:"key_file"`
	ClientCACertFile string   `yaml:"client_ca_cert_file"`
	ClientCAKeyFile  string   `yaml:"client_ca_key_file"`
	HostWhitelist    []string `yaml:"host_whitelist"`
}

type RemoteExecutionTarget struct {
	Target                     string            `yaml:"target"`
	Properties                 map[string]string `yaml:"properties"`
	MaxExecutionTimeoutSeconds int64             `yaml:"max_execution_timeout_seconds"`
	DisableStreaming           bool              `yaml:"disable_streaming"`
}

type RemoteExecutionConfig struct {
	RemoteExecutionTargets []RemoteExecutionTarget `yaml:"remote_execution_targets"`
}

type ExecutorConfig struct {
	RootDirectory       string `yaml:"root_directory"`
	LocalCacheDirectory string `yaml:"local_cache_directory"`
	LocalCacheSizeBytes int64  `yaml:"local_cache_size_bytes"`
	CacheTarget         string `yaml:"cache_target"`
	AppTarget           string `yaml:"app_target"`
}

type APIConfig struct {
	EnableAPI bool   `yaml:"enable_api"`
	APIKey    string `yaml:"api_key"`
}

type GithubConfig struct {
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
}

type OrgConfig struct {
	Name   string `yaml:"name"`
	Domain string `yaml:"domain"`
}

func ensureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Printf("Directory '%s' did not exist; creating it.", dir)
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func readConfig(fullConfigPath string) (*generalConfig, error) {
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

	var gc generalConfig
	if err := yaml.Unmarshal([]byte(expandedFileBytes), &gc); err != nil {
		return nil, fmt.Errorf("Error parsing config file: %s", err)
	}
	return &gc, nil
}

func validateConfig(c *generalConfig) error {
	if c.Storage.Disk.RootDirectory != "" {
		if err := ensureDirectoryExists(c.Storage.Disk.RootDirectory); err != nil {
			return err
		}
	}
	return nil
}

type Configurator struct {
	fullConfigPath string
	lastReadTime   time.Time
	gc             *generalConfig
}

func NewConfigurator(configFilePath string) (*Configurator, error) {
	log.Printf("Reading buildbuddy config from '%s'", configFilePath)
	conf, err := readConfig(configFilePath)
	if err != nil {
		return nil, err
	}
	if err := validateConfig(conf); err != nil {
		return nil, err
	}
	return &Configurator{
		fullConfigPath: configFilePath,
		lastReadTime:   time.Now(),
		gc:             conf,
	}, nil
}

func (c *Configurator) rereadIfStale() {
	stat, err := os.Stat(c.fullConfigPath)
	if err != nil {
		log.Printf("Error STATing config file: %s", err)
		return
	}
	// We already read this thing.
	if c.lastReadTime.After(stat.ModTime()) {
		return
	}
	conf, err := readConfig(c.fullConfigPath)
	if err != nil {
		log.Printf("Error rereading config file: %s", err)
		return
	}
	c.gc = conf
}

func (c *Configurator) GetStorageTTLSeconds() int {
	return c.gc.Storage.TTLSeconds
}

func (c *Configurator) GetStorageChunkFileSizeBytes() int {
	return c.gc.Storage.ChunkFileSizeBytes
}

func (c *Configurator) GetStorageDiskRootDir() string {
	c.rereadIfStale()
	return c.gc.Storage.Disk.RootDirectory
}

func (c *Configurator) GetStorageGCSConfig() *GCSConfig {
	c.rereadIfStale()
	return &c.gc.Storage.GCS
}

func (c *Configurator) GetStorageAWSS3Config() *AwsS3Config {
	c.rereadIfStale()
	return &c.gc.Storage.AwsS3
}

func (c *Configurator) GetDBDataSource() string {
	c.rereadIfStale()
	return c.gc.Database.DataSource
}

func (c *Configurator) GetAppBuildBuddyURL() string {
	c.rereadIfStale()
	return c.gc.App.BuildBuddyURL
}

func (c *Configurator) GetAppEventsAPIURL() string {
	c.rereadIfStale()
	return c.gc.App.EventsAPIURL
}

func (c *Configurator) GetAppCacheAPIURL() string {
	c.rereadIfStale()
	return c.gc.App.CacheAPIURL
}

func (c *Configurator) GetAppRemoteExecutionAPIURL() string {
	c.rereadIfStale()
	return c.gc.App.RemoteExecutionAPIURL
}

func (c *Configurator) GetAppNoDefaultUserGroup() bool {
	c.rereadIfStale()
	return c.gc.App.NoDefaultUserGroup
}

func (c *Configurator) GetAppCreateGroupPerUser() bool {
	c.rereadIfStale()
	return c.gc.App.CreateGroupPerUser
}

func (c *Configurator) GetAppAddUserToDomainGroup() bool {
	c.rereadIfStale()
	return c.gc.App.AddUserToDomainGroup
}

func (c *Configurator) GetGRPCOverHTTPPortEnabled() bool {
	c.rereadIfStale()
	return c.gc.App.GRPCOverHTTPPortEnabled
}

func (c *Configurator) GetDefaultToDenseMode() bool {
	c.rereadIfStale()
	return c.gc.App.DefaultToDenseMode
}

func (c *Configurator) GetGRPCMaxRecvMsgSizeBytes() int {
	c.rereadIfStale()
	n := c.gc.App.GRPCMaxRecvMsgSizeBytes
	if n == 0 {
		// Support large BEP messages: https://github.com/bazelbuild/bazel/issues/12050
		return 50000000
	}
	return n
}

func (c *Configurator) GetIntegrationsSlackConfig() *SlackConfig {
	c.rereadIfStale()
	return &c.gc.Integrations.Slack
}

func (c *Configurator) GetBuildEventProxyHosts() []string {
	c.rereadIfStale()
	return c.gc.BuildEventProxy.Hosts
}

func (c *Configurator) GetCacheMaxSizeBytes() int64 {
	c.rereadIfStale()
	return c.gc.Cache.MaxSizeBytes
}

func (c *Configurator) GetCacheDiskConfig() *DiskConfig {
	c.rereadIfStale()
	return c.gc.Cache.Disk
}

func (c *Configurator) GetCacheGCSConfig() *GCSCacheConfig {
	c.rereadIfStale()
	return c.gc.Cache.GCS
}

func (c *Configurator) GetCacheS3Config() *S3CacheConfig {
	c.rereadIfStale()
	return c.gc.Cache.S3
}

func (c *Configurator) GetCacheMemcacheTargets() []string {
	c.rereadIfStale()
	return c.gc.Cache.MemcacheTargets
}

func (c *Configurator) GetCacheInMemory() bool {
	c.rereadIfStale()
	return c.gc.Cache.InMemory
}

func (c *Configurator) GetAnonymousUsageEnabled() bool {
	c.rereadIfStale()
	return len(c.gc.Auth.OauthProviders) == 0 || c.gc.Auth.EnableAnonymousUsage
}

func (c *Configurator) GetAuthOauthProviders() []*OauthProvider {
	c.rereadIfStale()
	op := c.gc.Auth.OauthProviders
	if len(c.gc.Auth.OauthProviders) == 1 {
		if cs := os.Getenv("BB_OAUTH_CLIENT_SECRET"); cs != "" {
			op[0].ClientSecret = cs
		}
	}
	return op
}

func (c *Configurator) GetSSLConfig() *SSLConfig {
	c.rereadIfStale()
	return c.gc.SSL
}

func (c *Configurator) GetRemoteExecutionConfig() *RemoteExecutionConfig {
	c.rereadIfStale()
	return c.gc.RemoteExecution
}

func (c *Configurator) GetExecutorConfig() *ExecutorConfig {
	c.rereadIfStale()
	return c.gc.Executor
}

func (c *Configurator) GetAPIConfig() *APIConfig {
	c.rereadIfStale()
	return c.gc.API
}

func (c *Configurator) GetGithubConfig() *GithubConfig {
	c.rereadIfStale()
	ghc := c.gc.Github
	if cs := os.Getenv("BB_GITHUB_CLIENT_SECRET"); cs != "" {
		ghc.ClientSecret = cs
	}
	return ghc
}

func (c *Configurator) GetOrgConfig() *OrgConfig {
	c.rereadIfStale()
	return c.gc.Org
}
