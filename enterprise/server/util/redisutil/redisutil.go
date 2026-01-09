package redisutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cespare/xxhash/v2"
	"github.com/dgryski/go-rendezvous"
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
)

var (
	commandBufferFlushPeriod = flag.Duration(
		"redis_command_buffer_flush_period", 250*time.Millisecond,
		"How long to wait between flushing buffered redis commands. "+
			"Setting this to 0 will disable buffering at the cost of higher redis QPS.")

	shardConfigReloadInterval = flag.Duration("redis_sharding_config_reload_interval", 1*time.Minute, "How often to re-check the experiment config for redis shard configuration changes.", flag.Internal)
)

const (
	// Timeout for each attempt to dial redis.
	dialTimeout = 5 * time.Second
)

// ConfigureLogging configures the Redis library to use our logger.
func ConfigureLogging() {
	redis.SetLogger(&logger{})
}

type logger struct{}

func (*logger) Printf(ctx context.Context, format string, args ...any) {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		format = "%s:%d: " + format
		args = append([]any{filepath.Base(file), line}, args...)
	}
	log.CtxInfof(ctx, format, args...)
}

func isRedisURI(redisTarget string) bool {
	return strings.HasPrefix(redisTarget, "redis://") ||
		strings.HasPrefix(redisTarget, "rediss://") ||
		strings.HasPrefix(redisTarget, "unix://")
}

func TargetToOptions(redisTarget string) *redis.Options {
	if !isRedisURI(redisTarget) {
		return &redis.Options{
			Addr:     redisTarget,
			Password: "", // no password set
			DB:       0,  // use default DB
		}
	} else if opt, err := redis.ParseURL(redisTarget); err == nil {
		return opt
	} else {
		log.Errorf(
			"Failed to parse redis URI %q: %s - The supported redis URI formats are:\n"+
				"redis[s]://[[USER][:PASSWORD]@][HOST][:PORT]"+
				"[/DATABASE]\nor:\nunix://[[USER][:PASSWORD]@]"+
				"SOCKET_PATH[?db=DATABASE]\nIf using a socket path, "+
				"path must be an absolute unix path.", redisTarget, err)
		return &redis.Options{}
	}
}

func TargetToOpts(redisTarget string) *Opts {
	if redisTarget == "" {
		return nil
	}
	libOpts := TargetToOptions(redisTarget)
	return &Opts{
		Addrs:     []string{libOpts.Addr},
		Network:   libOpts.Network,
		Username:  libOpts.Username,
		Password:  libOpts.Password,
		DB:        libOpts.DB,
		TLSConfig: libOpts.TLSConfig,
		Dialer:    getDialer(libOpts.TLSConfig),
	}
}

func ShardsToOpts(shards []string, username, password string) *Opts {
	if len(shards) == 0 {
		return nil
	}
	var prevShardScheme string
	for i, shard := range shards {
		u, err := url.Parse(shard)
		if err != nil {
			log.Errorf("Failed to parse redis shard %q: %s, ignoring", shard, err)
			continue
		}
		if i > 0 {
			if u.Scheme != prevShardScheme {
				// TODO(sluongng): return an error instead of logging.
				log.Warningf("All redis shards must use the same url scheme, but found %q and %q", prevShardScheme, u.Scheme)
				break
			}
		}
		prevShardScheme = u.Scheme
	}
	opt := TargetToOpts(shards[0])
	opt.Addrs = shards
	opt.Username = username
	opt.Password = password
	return opt
}

func getDialer(tlsConfig *tls.Config) func(ctx context.Context, network, addr string) (net.Conn, error) {
	// Create a dialer which should exactly match go-redis' implementation:
	// https://github.com/redis/go-redis/blob/cae67723092cac2cb441bc87044ab9edacb2484d/options.go#L128-L142
	//
	// TODO: when we upgrade redis, we can switch to redis.NewDialer here.
	dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		netDialer := &net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 5 * time.Minute,
		}
		if tlsConfig == nil {
			return netDialer.DialContext(ctx, network, addr)
		}
		return tls.DialWithDialer(netDialer, network, addr, tlsConfig)
	}
	// Wrap the dialer with retries.
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return retry.Do(ctx, retry.DefaultOptions(), func(ctx context.Context) (net.Conn, error) {
			conn, err := dialer(ctx, network, addr)
			if err != nil {
				metrics.RedisClientDialErrors.Inc()
				return nil, fmt.Errorf("dial %s %s: %w", network, addr, err)
			}
			return conn, nil
		})
	}
}

type HealthChecker struct {
	Rdb redis.UniversalClient
}

func (c *HealthChecker) Check(ctx context.Context) error {
	return c.Rdb.Ping(ctx).Err()
}

// Opts holds configuration options that can be converted to redis.Options used by the "simple" Redis client or to
// redis.RingOptions used by the Ring client. The go-redis library has a "universal" option type which unfortunately
// does not cover the Ring client.
// Refer to the redis.Options struct for documentation of individual options.
type Opts struct {
	// Addrs contains zero or more addresses of Redis instances.
	// When used to create a simple client, this must contain either 0 or 1 addresses.
	// May contain more than 1 address if creating a Ring client.
	Addrs []string
	// Only used when creating a simple client. Ring client does not support non-tcp network types.
	Network string

	Username string
	Password string
	DB       int

	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration

	TLSConfig *tls.Config
	Dialer    func(ctx context.Context, network, addr string) (net.Conn, error)

	// MigrationConfig allows configuring migrations for sharded redis setups.
	// Use [NewMigrationConfig] to get an instance.
	MigrationConfig *MigrationConfig
}

func (o *Opts) toSimpleOpts() (*redis.Options, error) {
	if len(o.Addrs) > 1 {
		return nil, status.FailedPreconditionErrorf("simple Redis client only supports a single address")
	}
	opts := &redis.Options{
		Network:            o.Network,
		Username:           o.Username,
		Password:           o.Password,
		DB:                 o.DB,
		MaxRetries:         o.MaxRetries,
		MinRetryBackoff:    o.MinRetryBackoff,
		MaxRetryBackoff:    o.MaxRetryBackoff,
		PoolSize:           o.PoolSize,
		PoolTimeout:        o.PoolTimeout,
		IdleTimeout:        o.IdleTimeout,
		IdleCheckFrequency: o.IdleCheckFrequency,
		TLSConfig:          o.TLSConfig,
		Dialer:             o.Dialer,
	}
	if len(o.Addrs) > 0 {
		opts.Addr = o.Addrs[0]
	}
	return opts, nil
}

func (o *Opts) toRingOpts() (*redis.RingOptions, error) {
	opts := &redis.RingOptions{
		Addrs:              map[string]string{},
		Username:           o.Username,
		Password:           o.Password,
		DB:                 o.DB,
		MaxRetries:         o.MaxRetries,
		MinRetryBackoff:    o.MinRetryBackoff,
		MaxRetryBackoff:    o.MaxRetryBackoff,
		PoolSize:           o.PoolSize,
		PoolTimeout:        o.PoolTimeout,
		IdleTimeout:        o.IdleTimeout,
		IdleCheckFrequency: o.IdleCheckFrequency,
		TLSConfig:          o.TLSConfig,
		Dialer:             o.Dialer,
	}
	for i, addr := range o.Addrs {
		if !isRedisURI(addr) {
			// Assume tcp if no scheme is provided.
			addr = "redis://" + addr
		}
		opt, err := redis.ParseURL(addr)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("invalid redis shard address %q: %s", addr, err)
		}
		opts.Addrs[fmt.Sprintf("shard%d", i)] = opt.Addr
	}
	return opts, nil
}

type MigrationConfig struct {
	fp interfaces.ExperimentFlagProvider

	// If nonempty, this is the default value for the enabled shards. If empty,
	// all shards will be enabled.
	defaultEnabledAddrs []string

	// experimentName is the experiment that determines which shards are
	// enabled. If the experiment evaluates to an empty slice (e.g. if the
	// experiment is not configured yet), then defaultEnabledAddrs is used. The
	// experiment flag must evaluate to an object matching the shape of
	// [MigrationExperimentConfig].
	experimentName string
}

type MigrationExperimentConfig struct {
	EnabledAddrs []string `json:"enabled_addrs"`
}

func NewMigrationConfig(fp interfaces.ExperimentFlagProvider, allAddrs, defaultEnabledAddrs []string, experimentName string) *MigrationConfig {
	if len(defaultEnabledAddrs) == 0 {
		defaultEnabledAddrs = allAddrs
	}
	return &MigrationConfig{
		fp:                  fp,
		defaultEnabledAddrs: defaultEnabledAddrs,
		experimentName:      experimentName,
	}
}

func (m *MigrationConfig) loadEnabledAddrs() []string {
	expMap := m.fp.Object(context.TODO(), m.experimentName, nil)
	if len(expMap) == 0 {
		return m.defaultEnabledAddrs
	}
	expStruct := &MigrationExperimentConfig{}
	if err := experiments.ObjectToStruct(expMap, expStruct); err != nil {
		log.Errorf("Failed to unmarshal enabled shards experiment object: %s", err)
		return m.defaultEnabledAddrs
	}
	if len(expStruct.EnabledAddrs) == 0 {
		return m.defaultEnabledAddrs
	}
	return expStruct.EnabledAddrs
}

// getConsistentHashProvider returns a redis hash function provider that filters
// to the currently enabled shards in the experiment config.
func getConsistentHashProvider(ctx context.Context, clientName string, migrationConfig *MigrationConfig, shardNameToAddr map[string]string) func(shards []string) redis.ConsistentHash {
	// The consistent hash uses shard names - build a reverse mapping.
	addrToShardName := make(map[string]string, len(shardNameToAddr))
	for k, v := range shardNameToAddr {
		addrToShardName[v] = k
	}
	shardNamesFromAddrs := func(enabledAddrs []string) []string {
		out := make([]string, 0, len(enabledAddrs))
		for _, addr := range enabledAddrs {
			out = append(out, addrToShardName[addr])
		}
		return out
	}

	var enabledShards atomic.Value // *[]string
	{
		s := shardNamesFromAddrs(migrationConfig.loadEnabledAddrs())
		enabledShards.Store(&s)
	}
	log.CtxInfof(ctx, "Configured enabled redis shards for %s: %v", clientName, migrationConfig.defaultEnabledAddrs)

	// In the background, reconfigure the enabled shards whenever the experiment
	// config changes.
	go func() {
		configChanged := make(chan struct{}, 1)
		unsubscribe := migrationConfig.fp.Subscribe(configChanged)
		defer unsubscribe()
		// As a backup, poll as well.
		ticker := time.NewTicker(*shardConfigReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-configChanged:
			}
			enabledAddrs := migrationConfig.loadEnabledAddrs()
			slices.Sort(enabledAddrs)
			s := shardNamesFromAddrs(enabledAddrs)
			// If the slice contents have changed, store a new slice pointer in
			// enabledShards, which will cause the rendezvous hashes below to be
			// invalidated and rebuilt.
			if !slices.Equal(s, *enabledShards.Load().(*[]string)) {
				log.CtxInfof(ctx, "Reconfigured enabled redis shards for %s: %v", clientName, enabledAddrs)
				enabledShards.Store(&s)
			}
		}
	}()
	// Return a hash function provider that filters the shard list to the
	// currently enabled shards, then hashes keys only to the enabled shards.
	// Internally, it rebuilds the consistent hash whenever the enabled shards
	// list changes.
	return func(shards []string) redis.ConsistentHash {
		type hash struct {
			*rendezvous.Rendezvous
			enabledShards *[]string
		}
		var h atomic.Value // *hash
		h.Store(&hash{})
		getHashWithEnabledShardsOnly := func() *hash {
			// Check whether the hash needs to be rebuilt to reflect the
			// enabled shards list.
			enabledShardsPtr, _ := enabledShards.Load().(*[]string)
			hVal := h.Load().(*hash)
			if enabledShardsPtr == hVal.enabledShards {
				// Current hash is up to date; return it.
				return hVal
			}
			// Hash is out of date and needs to be rebuilt. Build a new hash
			// from the shards list, filtering to enabled shards only.
			enabled := *enabledShardsPtr
			shards := slices.Clone(shards)
			shards = slices.DeleteFunc(shards, func(s string) bool {
				return !slices.Contains(enabled, s)
			})
			// NOTE: this hash construction exactly matches what go-redis does
			// in its default NewConsistentHash implementation.
			hVal = &hash{
				Rendezvous:    rendezvous.New(shards, xxhash.Sum64String),
				enabledShards: enabledShardsPtr,
			}
			h.Store(hVal)
			return hVal
		}
		return consistentHashFunc(func(key string) string {
			hash := getHashWithEnabledShardsOnly()
			shardName := hash.Lookup(key)
			return shardName
		})
	}
}

// consistentHashFunc implements [redis.ConsistentHash] by just calling the func.
type consistentHashFunc func(key string) string

func (f consistentHashFunc) Get(key string) string { return f(key) }

func NewClientWithOpts(opts *Opts, checker interfaces.HealthChecker, healthCheckName string) (redis.UniversalClient, error) {
	var redisClient redis.UniversalClient
	if len(opts.Addrs) <= 1 {
		simpleOpts, err := opts.toSimpleOpts()
		if err != nil {
			return nil, err
		}
		redisClient = redis.NewClient(simpleOpts)
	} else {
		ringOpts, err := opts.toRingOpts()
		if err != nil {
			return nil, err
		}
		if opts.MigrationConfig != nil {
			ringOpts.NewConsistentHash = getConsistentHashProvider(context.TODO(), healthCheckName, opts.MigrationConfig, ringOpts.Addrs)
		}
		redisClient = redis.NewRing(ringOpts)
	}
	redisClient.AddHook(redisotel.NewTracingHook())
	checker.AddHealthCheck(healthCheckName, &HealthChecker{Rdb: redisClient})
	return redisClient, nil
}

func NewSimpleClient(redisTarget string, checker interfaces.HealthChecker, healthCheckName string) *redis.Client {
	redisClient := redis.NewClient(TargetToOptions(redisTarget))
	redisClient.AddHook(redisotel.NewTracingHook())
	checker.AddHealthCheck(healthCheckName, &HealthChecker{Rdb: redisClient})
	return redisClient
}

var unlockScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

type redlock struct {
	rdb    redis.UniversalClient
	key    string
	value  string
	expiry time.Duration
}

// NewWeakLock returns a distributed lock implemented using a single Redis
// client. This provides a "weak" implementation of the Redlock algorithm,
// in the sense that:
//
// (a) It is not resilient to Redis nodes failing (even if
//
//	the given Redis client points to a Redis cluster)
//
// (b) It does not retry failed locking attempts.
//
// See https://redis.io/topics/distlock
func NewWeakLock(rdb redis.UniversalClient, key string, expiry time.Duration) (*redlock, error) {
	value, err := random.RandomString(20)
	if err != nil {
		return nil, err
	}
	return &redlock{
		rdb:    rdb,
		key:    key,
		value:  value,
		expiry: expiry,
	}, nil
}

func (r *redlock) Lock(ctx context.Context) error {
	ok, err := r.rdb.SetNX(ctx, r.key, r.value, r.expiry).Result()
	if err != nil {
		return status.UnavailableErrorf("failed to attempt redis lock acquisition: %s", err)
	}
	if !ok {
		return status.ResourceExhaustedErrorf("failed to acquire redis lock: already acquired")
	}
	return nil
}

func (r *redlock) Unlock(ctx context.Context) error {
	_, err := unlockScript.Run(ctx, r.rdb, []string{r.key}, r.value).Result()
	return err
}

type valueExpiration struct {
	value      interface{}
	expiration time.Duration
}

// CommandBuffer buffers and aggregates Redis commands in-memory and allows
// periodically flushing the aggregate results in batch. This is useful for
// reducing the load placed on Redis in cases where a high volume of commands
// are operating on a relatively small subset of Redis keys (for example,
// counter values that are incremented several times over a short time period).
//
// When using this buffer, the caller is responsible for explicitly flushing the
// data. In most cases, flushing should be done at regular intervals, as well as
// after writing the last command to the buffer, if applicable.
//
// There may be a non-negligible delay between the time that the data is written
// to the buffer and the time that data is flushed, so callers may need extra
// synchronization in order to ensure that buffers are flushed (on all nodes, if
// applicable), before relying on the values of any keys updated via this
// buffer.
//
// It is safe for concurrent access.
//
// When the server is shutting down, all commands are issued synchronously
// rather than adding them to the buffer. This is to reduce the likelihood of
// data being lost from the buffer due to the server shutting down. In practice,
// not much new data should be written to the buffer during shutdown since we
// don't accept new RPCs while shutting down.
type CommandBuffer struct {
	rdb       redis.UniversalClient
	stopFlush chan struct{}

	mu sync.Mutex // protects all fields below
	// Buffer for SET commands.
	set map[string]valueExpiration
	// Buffer for INCRBY commands.
	incr map[string]int64
	// Buffer for HINCRBY commands.
	hincr map[string]map[string]int64
	// Buffer for RPUSH commands.
	rpush map[string][]interface{}
	// Buffer for SADD commands.
	sadd map[string]map[interface{}]struct{}
	// Buffer for EXPIRE commands.
	expire map[string]time.Duration
	// Whether the server is shutting down.
	isShuttingDown bool
}

// NewCommandBuffer creates a buffer.
func NewCommandBuffer(rdb redis.UniversalClient) *CommandBuffer {
	c := &CommandBuffer{rdb: rdb}
	c.init()
	return c
}

func (c *CommandBuffer) init() {
	c.set = map[string]valueExpiration{}
	c.incr = map[string]int64{}
	c.hincr = map[string]map[string]int64{}
	c.rpush = map[string][]interface{}{}
	c.sadd = map[string]map[interface{}]struct{}{}
	c.expire = map[string]time.Duration{}
}

func (c *CommandBuffer) shouldFlushSynchronously() bool {
	return c.isShuttingDown || (*commandBufferFlushPeriod == 0)
}

// IncrBy adds an INCRBY operation to the buffer.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) IncrBy(ctx context.Context, key string, increment int64) error {
	c.mu.Lock()
	if c.shouldFlushSynchronously() {
		c.mu.Unlock()
		return c.rdb.IncrBy(ctx, key, increment).Err()
	}
	defer c.mu.Unlock()

	c.incr[key] += increment
	return nil
}

// HIncrBy adds an HINCRBY operation to the buffer.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) HIncrBy(ctx context.Context, key, field string, increment int64) error {
	c.mu.Lock()
	if c.shouldFlushSynchronously() {
		c.mu.Unlock()
		return c.rdb.HIncrBy(ctx, key, field, increment).Err()
	}
	defer c.mu.Unlock()

	h, ok := c.hincr[key]
	if !ok {
		h = map[string]int64{}
		c.hincr[key] = h
	}
	h[field] += increment
	return nil
}

// Set adds a SET operation to the buffer.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	c.mu.Lock()
	if c.shouldFlushSynchronously() {
		c.mu.Unlock()
		return c.rdb.Set(ctx, key, value, expiration).Err()
	}
	defer c.mu.Unlock()

	c.set[key] = valueExpiration{value, expiration}
	return nil
}

// SAdd adds an SADD operation to the buffer. All buffered members of the set
// will be added in a single SADD command.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) SAdd(ctx context.Context, key string, members ...interface{}) error {
	c.mu.Lock()
	if c.shouldFlushSynchronously() {
		c.mu.Unlock()
		return c.rdb.SAdd(ctx, key, members...).Err()
	}
	defer c.mu.Unlock()

	set, ok := c.sadd[key]
	if !ok {
		set = make(map[interface{}]struct{}, len(members))
		c.sadd[key] = set
	}
	for _, m := range members {
		set[m] = struct{}{}
	}
	return nil
}

// RPush adds an RPUSH operation to the buffer.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) RPush(ctx context.Context, key string, values ...interface{}) error {
	c.mu.Lock()
	if c.shouldFlushSynchronously() {
		c.mu.Unlock()
		return c.rdb.RPush(ctx, key, values...).Err()
	}
	defer c.mu.Unlock()

	c.rpush[key] = append(c.rpush[key], values...)
	return nil
}

// Expire adds an EXPIRE operation to the buffer, overwriting any previous
// expiry currently buffered for the given key. The duration is applied
// as-is when flushed to Redis, meaning that any time elapsed until the flush
// does not count towards the expiration duration.
//
// Because the expirations are relative to when the buffer is flushed, the
// buffer does not actively apply expirations to its own keys or delete keys
// from Redis when the buffered expiration times have elapsed. This means there
// are some corner cases where a sequence of buffered commands does not behave
// the same as if those commands were issued directly to Redis. For example,
// consider the following sequence of buffered commands:
//
//	INCRBY key 1
//	EXPIRE key 0
//	EXPIRE key 3600
//
// The `EXPIRE key 0` command is effectively dropped from the buffer since the
// later `EXPIRE` command overwrites it, whereas Redis would delete the key
// immediately upon seeing the `EXPIRE key 0` command.
func (c *CommandBuffer) Expire(ctx context.Context, key string, duration time.Duration) error {
	c.mu.Lock()
	if c.shouldFlushSynchronously() {
		c.mu.Unlock()
		return c.rdb.Expire(ctx, key, duration).Err()
	}
	defer c.mu.Unlock()

	c.expire[key] = duration
	return nil
}

// Flush flushes the buffered commands to Redis. The commands needed to flush
// the buffers are issued in serial. If a command fails, the error is returned
// and the rest of the commands in the series are not executed, and their data
// is dropped from the buffer.
func (c *CommandBuffer) Flush(ctx context.Context) error {
	c.mu.Lock()
	set := c.set
	incr := c.incr
	hincr := c.hincr
	sadd := c.sadd
	rpush := c.rpush
	expire := c.expire
	// Set all fields to fresh values so the current ones can be flushed without
	// keeping a hold on the lock.
	c.init()
	c.mu.Unlock()

	pipe := c.rdb.Pipeline()

	for key, args := range set {
		pipe.Set(ctx, key, args.value, args.expiration)
	}
	for key, increment := range incr {
		pipe.IncrBy(ctx, key, increment)
	}
	for key, h := range hincr {
		for field, increment := range h {
			pipe.HIncrBy(ctx, key, field, increment)
		}
	}
	for key, values := range rpush {
		pipe.RPush(ctx, key, values...)
	}
	for key, set := range sadd {
		members := []interface{}{}
		for member := range set {
			members = append(members, member)
		}
		pipe.SAdd(ctx, key, members...)
	}
	for key, duration := range expire {
		pipe.Expire(ctx, key, duration)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// StartPeriodicFlush starts a loop that periodically flushes buffered commands
// to Redis.
func (c *CommandBuffer) StartPeriodicFlush(ctx context.Context) {
	if *commandBufferFlushPeriod == 0 {
		return
	}

	c.stopFlush = make(chan struct{})
	go func() {
		ticker := time.NewTicker(*commandBufferFlushPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopFlush:
				return
			case <-ticker.C:
				if err := c.Flush(ctx); err != nil {
					log.Errorf("Failed to flush Redis command buffer: %s", err)
				}
			}
		}
	}()
}

// StopPeriodicFlush stops flushing the buffer to Redis. If a flush is currently
// in progress, this will block until the flush is complete.
func (c *CommandBuffer) StopPeriodicFlush(ctx context.Context) error {
	if *commandBufferFlushPeriod == 0 {
		return nil
	}

	c.stopFlush <- struct{}{}

	c.mu.Lock()
	// Set a flag to make any newly written commands to go directly to Redis.
	c.isShuttingDown = true
	c.mu.Unlock()

	// Since all newly written commands will now go directly to Redis and we
	// just stopped the background flusher, we're on the hook for flushing any
	// remaining buffer contents.
	return c.Flush(ctx)
}
