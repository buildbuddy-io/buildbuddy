package redisutil

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
)

var (
	commandBufferFlushPeriod = flag.Duration(
		"redis_command_buffer_flush_period", 250*time.Millisecond,
		"How long to wait between flushing buffered redis commands. "+
			"Setting this to 0 will disable buffering at the cost of higher redis QPS.")
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
	// The name of the experiment that provides the Opts as a JSON object.
	optsExperimentName string
}

func NewMigrationConfig(fp interfaces.ExperimentFlagProvider, optsExperimentName string) *MigrationConfig {
	return &MigrationConfig{
		fp:                 fp,
		optsExperimentName: optsExperimentName,
	}
}

func NewClientWithOpts(opts *Opts, checker interfaces.HealthChecker, healthCheckName string) (redis.UniversalClient, error) {
	redisClient, err := newClient(opts)
	if err != nil {
		return nil, err
	}
	if opts.MigrationConfig != nil {
		redisClient = withMigrationConfig(redisClient, opts.MigrationConfig, checker)
	}
	redisClient.AddHook(redisotel.NewTracingHook())
	checker.AddHealthCheck(healthCheckName, &HealthChecker{Rdb: redisClient})
	return redisClient, nil
}

func newClient(opts *Opts) (redis.UniversalClient, error) {
	if len(opts.Addrs) <= 1 {
		simpleOpts, err := opts.toSimpleOpts()
		if err != nil {
			return nil, err
		}
		return redis.NewClient(simpleOpts), nil
	}
	ringOpts, err := opts.toRingOpts()
	if err != nil {
		return nil, err
	}
	return redis.NewRing(ringOpts), nil
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

func withMigrationConfig(oldClient redis.UniversalClient, config *MigrationConfig, hc interfaces.HealthChecker) redis.UniversalClient {
	var mu sync.RWMutex
	var newClient redis.UniversalClient

	// Listen to config changes and read the client from the experiment config.
	// Also trigger an initial read.
	reloadConfig := make(chan struct{}, 1)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			// Reconfigure the client whenever flagd config changes, along with
			// a periodic check as a backup.
			// TODO: Ensure Subscribe() reliably delivers updates and remove
			// this extra check.
			case <-reloadConfig:
			case <-time.After(1 * time.Minute):
			}
			mu.Lock()
			newClient = newClientFromExperiment(config.fp, config.optsExperimentName)
			mu.Unlock()
		}
	})
	unsubscribe := config.fp.Subscribe(reloadConfig)
	// Unsubscribe on shutdown
	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		unsubscribe()
		cancel()
		wg.Wait()
		return nil
	})
	return &dynamicClient{
		GetClient: func() redis.UniversalClient {
			mu.RLock()
			c := newClient
			mu.RUnlock()
			if c != nil {
				return c
			}
			return oldClient
		},
	}
}

func newClientFromExperiment(fp interfaces.ExperimentFlagProvider, experimentName string) redis.UniversalClient {
	obj := fp.Object(context.Background(), experimentName, nil)
	if len(obj) == 0 {
		return nil
	}
	opts := &Opts{}
	if err := experiments.ObjectToStruct(obj, opts); err != nil {
		log.Errorf("Failed to convert experiment object to Opts struct: %s", err)
		return nil
	}
	log.Infof("Loaded redis client config from experiment %q: %+#v", experimentName, opts)
	client, err := newClient(opts)
	if err != nil {
		log.Errorf("Failed to create Redis client from experiment %q: %s", experimentName, err)
		return nil
	}
	return client
}

// dynamicClient implements [redis.UniversalClient]. For every redis operation,
// it evaluates a function to get the client to use for the operation, then
// invokes the same operation on the returned client. This is useful for
// migrations.
//
// Note that any long-lived operations such as stream subscriptions will use the
// client originally returned by the given func.
type dynamicClient struct {
	GetClient func() redis.UniversalClient
}

var _ redis.UniversalClient = (*dynamicClient)(nil)

func (c *dynamicClient) Context() context.Context { return c.GetClient().Context() }
func (c *dynamicClient) AddHook(hook redis.Hook)  { c.GetClient().AddHook(hook) }
func (c *dynamicClient) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return c.GetClient().Watch(ctx, fn, keys...)
}
func (c *dynamicClient) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	return c.GetClient().Do(ctx, args...)
}
func (c *dynamicClient) Process(ctx context.Context, cmd redis.Cmder) error {
	return c.GetClient().Process(ctx, cmd)
}
func (c *dynamicClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.GetClient().Subscribe(ctx, channels...)
}
func (c *dynamicClient) PSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.GetClient().PSubscribe(ctx, channels...)
}
func (c *dynamicClient) Close() error                { return c.GetClient().Close() }
func (c *dynamicClient) PoolStats() *redis.PoolStats { return c.GetClient().PoolStats() }
func (c *dynamicClient) Pipeline() redis.Pipeliner   { return c.GetClient().Pipeline() }
func (c *dynamicClient) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return c.GetClient().Pipelined(ctx, fn)
}
func (c *dynamicClient) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return c.GetClient().TxPipelined(ctx, fn)
}
func (c *dynamicClient) TxPipeline() redis.Pipeliner {
	return c.GetClient().TxPipeline()
}
func (c *dynamicClient) Command(ctx context.Context) *redis.CommandsInfoCmd {
	return c.GetClient().Command(ctx)
}
func (c *dynamicClient) ClientGetName(ctx context.Context) *redis.StringCmd {
	return c.GetClient().ClientGetName(ctx)
}
func (c *dynamicClient) Echo(ctx context.Context, message interface{}) *redis.StringCmd {
	return c.GetClient().Echo(ctx, message)
}
func (c *dynamicClient) Ping(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().Ping(ctx)
}
func (c *dynamicClient) Quit(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().Quit(ctx)
}
func (c *dynamicClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.GetClient().Del(ctx, keys...)
}
func (c *dynamicClient) Unlink(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.GetClient().Unlink(ctx, keys...)
}
func (c *dynamicClient) Dump(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().Dump(ctx, key)
}
func (c *dynamicClient) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.GetClient().Exists(ctx, keys...)
}
func (c *dynamicClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().Expire(ctx, key, expiration)
}
func (c *dynamicClient) ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd {
	return c.GetClient().ExpireAt(ctx, key, tm)
}
func (c *dynamicClient) ExpireNX(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().ExpireNX(ctx, key, expiration)
}
func (c *dynamicClient) ExpireXX(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().ExpireXX(ctx, key, expiration)
}
func (c *dynamicClient) ExpireGT(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().ExpireGT(ctx, key, expiration)
}
func (c *dynamicClient) ExpireLT(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().ExpireLT(ctx, key, expiration)
}
func (c *dynamicClient) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return c.GetClient().Keys(ctx, pattern)
}
func (c *dynamicClient) Migrate(ctx context.Context, host, port, key string, db int, timeout time.Duration) *redis.StatusCmd {
	return c.GetClient().Migrate(ctx, host, port, key, db, timeout)
}
func (c *dynamicClient) Move(ctx context.Context, key string, db int) *redis.BoolCmd {
	return c.GetClient().Move(ctx, key, db)
}
func (c *dynamicClient) ObjectRefCount(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().ObjectRefCount(ctx, key)
}
func (c *dynamicClient) ObjectEncoding(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().ObjectEncoding(ctx, key)
}
func (c *dynamicClient) ObjectIdleTime(ctx context.Context, key string) *redis.DurationCmd {
	return c.GetClient().ObjectIdleTime(ctx, key)
}
func (c *dynamicClient) Persist(ctx context.Context, key string) *redis.BoolCmd {
	return c.GetClient().Persist(ctx, key)
}
func (c *dynamicClient) PExpire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().PExpire(ctx, key, expiration)
}
func (c *dynamicClient) PExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd {
	return c.GetClient().PExpireAt(ctx, key, tm)
}
func (c *dynamicClient) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return c.GetClient().PTTL(ctx, key)
}
func (c *dynamicClient) RandomKey(ctx context.Context) *redis.StringCmd {
	return c.GetClient().RandomKey(ctx)
}
func (c *dynamicClient) Rename(ctx context.Context, key, newkey string) *redis.StatusCmd {
	return c.GetClient().Rename(ctx, key, newkey)
}
func (c *dynamicClient) RenameNX(ctx context.Context, key, newkey string) *redis.BoolCmd {
	return c.GetClient().RenameNX(ctx, key, newkey)
}
func (c *dynamicClient) Restore(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	return c.GetClient().Restore(ctx, key, ttl, value)
}
func (c *dynamicClient) RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	return c.GetClient().RestoreReplace(ctx, key, ttl, value)
}
func (c *dynamicClient) Sort(ctx context.Context, key string, sort *redis.Sort) *redis.StringSliceCmd {
	return c.GetClient().Sort(ctx, key, sort)
}
func (c *dynamicClient) SortStore(ctx context.Context, key, store string, sort *redis.Sort) *redis.IntCmd {
	return c.GetClient().SortStore(ctx, key, store, sort)
}
func (c *dynamicClient) SortInterfaces(ctx context.Context, key string, sort *redis.Sort) *redis.SliceCmd {
	return c.GetClient().SortInterfaces(ctx, key, sort)
}
func (c *dynamicClient) Touch(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.GetClient().Touch(ctx, keys...)
}
func (c *dynamicClient) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return c.GetClient().TTL(ctx, key)
}
func (c *dynamicClient) Type(ctx context.Context, key string) *redis.StatusCmd {
	return c.GetClient().Type(ctx, key)
}
func (c *dynamicClient) Append(ctx context.Context, key, value string) *redis.IntCmd {
	return c.GetClient().Append(ctx, key, value)
}
func (c *dynamicClient) Decr(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().Decr(ctx, key)
}
func (c *dynamicClient) DecrBy(ctx context.Context, key string, decrement int64) *redis.IntCmd {
	return c.GetClient().DecrBy(ctx, key, decrement)
}
func (c *dynamicClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().Get(ctx, key)
}
func (c *dynamicClient) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	return c.GetClient().GetRange(ctx, key, start, end)
}
func (c *dynamicClient) GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	return c.GetClient().GetSet(ctx, key, value)
}
func (c *dynamicClient) GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd {
	return c.GetClient().GetEx(ctx, key, expiration)
}
func (c *dynamicClient) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().GetDel(ctx, key)
}
func (c *dynamicClient) Incr(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().Incr(ctx, key)
}
func (c *dynamicClient) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return c.GetClient().IncrBy(ctx, key, value)
}
func (c *dynamicClient) IncrByFloat(ctx context.Context, key string, value float64) *redis.FloatCmd {
	return c.GetClient().IncrByFloat(ctx, key, value)
}
func (c *dynamicClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return c.GetClient().MGet(ctx, keys...)
}
func (c *dynamicClient) MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd {
	return c.GetClient().MSet(ctx, values...)
}
func (c *dynamicClient) MSetNX(ctx context.Context, values ...interface{}) *redis.BoolCmd {
	return c.GetClient().MSetNX(ctx, values...)
}
func (c *dynamicClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return c.GetClient().Set(ctx, key, value, expiration)
}
func (c *dynamicClient) SetArgs(ctx context.Context, key string, value interface{}, a redis.SetArgs) *redis.StatusCmd {
	return c.GetClient().SetArgs(ctx, key, value, a)
}
func (c *dynamicClient) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return c.GetClient().SetEX(ctx, key, value, expiration)
}
func (c *dynamicClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().SetNX(ctx, key, value, expiration)
}
func (c *dynamicClient) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return c.GetClient().SetXX(ctx, key, value, expiration)
}
func (c *dynamicClient) SetRange(ctx context.Context, key string, offset int64, value string) *redis.IntCmd {
	return c.GetClient().SetRange(ctx, key, offset, value)
}
func (c *dynamicClient) StrLen(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().StrLen(ctx, key)
}
func (c *dynamicClient) Copy(ctx context.Context, sourceKey string, destKey string, db int, replace bool) *redis.IntCmd {
	return c.GetClient().Copy(ctx, sourceKey, destKey, db, replace)
}
func (c *dynamicClient) GetBit(ctx context.Context, key string, offset int64) *redis.IntCmd {
	return c.GetClient().GetBit(ctx, key, offset)
}
func (c *dynamicClient) SetBit(ctx context.Context, key string, offset int64, value int) *redis.IntCmd {
	return c.GetClient().SetBit(ctx, key, offset, value)
}
func (c *dynamicClient) BitCount(ctx context.Context, key string, bitCount *redis.BitCount) *redis.IntCmd {
	return c.GetClient().BitCount(ctx, key, bitCount)
}
func (c *dynamicClient) BitOpAnd(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return c.GetClient().BitOpAnd(ctx, destKey, keys...)
}
func (c *dynamicClient) BitOpOr(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return c.GetClient().BitOpOr(ctx, destKey, keys...)
}
func (c *dynamicClient) BitOpXor(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return c.GetClient().BitOpXor(ctx, destKey, keys...)
}
func (c *dynamicClient) BitOpNot(ctx context.Context, destKey string, key string) *redis.IntCmd {
	return c.GetClient().BitOpNot(ctx, destKey, key)
}
func (c *dynamicClient) BitPos(ctx context.Context, key string, bit int64, pos ...int64) *redis.IntCmd {
	return c.GetClient().BitPos(ctx, key, bit, pos...)
}
func (c *dynamicClient) BitField(ctx context.Context, key string, args ...interface{}) *redis.IntSliceCmd {
	return c.GetClient().BitField(ctx, key, args...)
}
func (c *dynamicClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return c.GetClient().Scan(ctx, cursor, match, count)
}
func (c *dynamicClient) ScanType(ctx context.Context, cursor uint64, match string, count int64, keyType string) *redis.ScanCmd {
	return c.GetClient().ScanType(ctx, cursor, match, count, keyType)
}
func (c *dynamicClient) SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return c.GetClient().SScan(ctx, key, cursor, match, count)
}
func (c *dynamicClient) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return c.GetClient().HScan(ctx, key, cursor, match, count)
}
func (c *dynamicClient) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return c.GetClient().ZScan(ctx, key, cursor, match, count)
}
func (c *dynamicClient) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	return c.GetClient().HDel(ctx, key, fields...)
}
func (c *dynamicClient) HExists(ctx context.Context, key, field string) *redis.BoolCmd {
	return c.GetClient().HExists(ctx, key, field)
}
func (c *dynamicClient) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return c.GetClient().HGet(ctx, key, field)
}
func (c *dynamicClient) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	return c.GetClient().HGetAll(ctx, key)
}
func (c *dynamicClient) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	return c.GetClient().HIncrBy(ctx, key, field, incr)
}
func (c *dynamicClient) HIncrByFloat(ctx context.Context, key, field string, incr float64) *redis.FloatCmd {
	return c.GetClient().HIncrByFloat(ctx, key, field, incr)
}
func (c *dynamicClient) HKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	return c.GetClient().HKeys(ctx, key)
}
func (c *dynamicClient) HLen(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().HLen(ctx, key)
}
func (c *dynamicClient) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	return c.GetClient().HMGet(ctx, key, fields...)
}
func (c *dynamicClient) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.GetClient().HSet(ctx, key, values...)
}
func (c *dynamicClient) HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
	return c.GetClient().HMSet(ctx, key, values...)
}
func (c *dynamicClient) HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd {
	return c.GetClient().HSetNX(ctx, key, field, value)
}
func (c *dynamicClient) HVals(ctx context.Context, key string) *redis.StringSliceCmd {
	return c.GetClient().HVals(ctx, key)
}
func (c *dynamicClient) HRandField(ctx context.Context, key string, count int, withValues bool) *redis.StringSliceCmd {
	return c.GetClient().HRandField(ctx, key, count, withValues)
}
func (c *dynamicClient) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return c.GetClient().BLPop(ctx, timeout, keys...)
}
func (c *dynamicClient) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	return c.GetClient().BRPop(ctx, timeout, keys...)
}
func (c *dynamicClient) BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *redis.StringCmd {
	return c.GetClient().BRPopLPush(ctx, source, destination, timeout)
}
func (c *dynamicClient) LIndex(ctx context.Context, key string, index int64) *redis.StringCmd {
	return c.GetClient().LIndex(ctx, key, index)
}
func (c *dynamicClient) LInsert(ctx context.Context, key, op string, pivot, value interface{}) *redis.IntCmd {
	return c.GetClient().LInsert(ctx, key, op, pivot, value)
}
func (c *dynamicClient) LInsertBefore(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd {
	return c.GetClient().LInsertBefore(ctx, key, pivot, value)
}
func (c *dynamicClient) LInsertAfter(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd {
	return c.GetClient().LInsertAfter(ctx, key, pivot, value)
}
func (c *dynamicClient) LLen(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().LLen(ctx, key)
}
func (c *dynamicClient) LPop(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().LPop(ctx, key)
}
func (c *dynamicClient) LPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd {
	return c.GetClient().LPopCount(ctx, key, count)
}
func (c *dynamicClient) LPos(ctx context.Context, key string, value string, args redis.LPosArgs) *redis.IntCmd {
	return c.GetClient().LPos(ctx, key, value, args)
}
func (c *dynamicClient) LPosCount(ctx context.Context, key string, value string, count int64, args redis.LPosArgs) *redis.IntSliceCmd {
	return c.GetClient().LPosCount(ctx, key, value, count, args)
}
func (c *dynamicClient) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.GetClient().LPush(ctx, key, values...)
}
func (c *dynamicClient) LPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.GetClient().LPushX(ctx, key, values...)
}
func (c *dynamicClient) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.GetClient().LRange(ctx, key, start, stop)
}
func (c *dynamicClient) LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd {
	return c.GetClient().LRem(ctx, key, count, value)
}
func (c *dynamicClient) LSet(ctx context.Context, key string, index int64, value interface{}) *redis.StatusCmd {
	return c.GetClient().LSet(ctx, key, index, value)
}
func (c *dynamicClient) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	return c.GetClient().LTrim(ctx, key, start, stop)
}
func (c *dynamicClient) RPop(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().RPop(ctx, key)
}
func (c *dynamicClient) RPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd {
	return c.GetClient().RPopCount(ctx, key, count)
}
func (c *dynamicClient) RPopLPush(ctx context.Context, source, destination string) *redis.StringCmd {
	return c.GetClient().RPopLPush(ctx, source, destination)
}
func (c *dynamicClient) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.GetClient().RPush(ctx, key, values...)
}
func (c *dynamicClient) RPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.GetClient().RPushX(ctx, key, values...)
}
func (c *dynamicClient) LMove(ctx context.Context, source, destination, srcpos, destpos string) *redis.StringCmd {
	return c.GetClient().LMove(ctx, source, destination, srcpos, destpos)
}
func (c *dynamicClient) BLMove(ctx context.Context, source, destination, srcpos, destpos string, timeout time.Duration) *redis.StringCmd {
	return c.GetClient().BLMove(ctx, source, destination, srcpos, destpos, timeout)
}
func (c *dynamicClient) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return c.GetClient().SAdd(ctx, key, members...)
}
func (c *dynamicClient) SCard(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().SCard(ctx, key)
}
func (c *dynamicClient) SDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return c.GetClient().SDiff(ctx, keys...)
}
func (c *dynamicClient) SDiffStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return c.GetClient().SDiffStore(ctx, destination, keys...)
}
func (c *dynamicClient) SInter(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return c.GetClient().SInter(ctx, keys...)
}
func (c *dynamicClient) SInterStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return c.GetClient().SInterStore(ctx, destination, keys...)
}
func (c *dynamicClient) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	return c.GetClient().SIsMember(ctx, key, member)
}
func (c *dynamicClient) SMIsMember(ctx context.Context, key string, members ...interface{}) *redis.BoolSliceCmd {
	return c.GetClient().SMIsMember(ctx, key, members...)
}
func (c *dynamicClient) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return c.GetClient().SMembers(ctx, key)
}
func (c *dynamicClient) SMembersMap(ctx context.Context, key string) *redis.StringStructMapCmd {
	return c.GetClient().SMembersMap(ctx, key)
}
func (c *dynamicClient) SMove(ctx context.Context, source, destination string, member interface{}) *redis.BoolCmd {
	return c.GetClient().SMove(ctx, source, destination, member)
}
func (c *dynamicClient) SPop(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().SPop(ctx, key)
}
func (c *dynamicClient) SPopN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	return c.GetClient().SPopN(ctx, key, count)
}
func (c *dynamicClient) SRandMember(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().SRandMember(ctx, key)
}
func (c *dynamicClient) SRandMemberN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	return c.GetClient().SRandMemberN(ctx, key, count)
}
func (c *dynamicClient) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return c.GetClient().SRem(ctx, key, members...)
}
func (c *dynamicClient) SUnion(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return c.GetClient().SUnion(ctx, keys...)
}
func (c *dynamicClient) SUnionStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return c.GetClient().SUnionStore(ctx, destination, keys...)
}
func (c *dynamicClient) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	return c.GetClient().XAdd(ctx, a)
}
func (c *dynamicClient) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	return c.GetClient().XDel(ctx, stream, ids...)
}
func (c *dynamicClient) XLen(ctx context.Context, stream string) *redis.IntCmd {
	return c.GetClient().XLen(ctx, stream)
}
func (c *dynamicClient) XRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd {
	return c.GetClient().XRange(ctx, stream, start, stop)
}
func (c *dynamicClient) XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	return c.GetClient().XRangeN(ctx, stream, start, stop, count)
}
func (c *dynamicClient) XRevRange(ctx context.Context, stream string, start, stop string) *redis.XMessageSliceCmd {
	return c.GetClient().XRevRange(ctx, stream, start, stop)
}
func (c *dynamicClient) XRevRangeN(ctx context.Context, stream string, start, stop string, count int64) *redis.XMessageSliceCmd {
	return c.GetClient().XRevRangeN(ctx, stream, start, stop, count)
}
func (c *dynamicClient) XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd {
	return c.GetClient().XRead(ctx, a)
}
func (c *dynamicClient) XReadStreams(ctx context.Context, streams ...string) *redis.XStreamSliceCmd {
	return c.GetClient().XReadStreams(ctx, streams...)
}
func (c *dynamicClient) XGroupCreate(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return c.GetClient().XGroupCreate(ctx, stream, group, start)
}
func (c *dynamicClient) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return c.GetClient().XGroupCreateMkStream(ctx, stream, group, start)
}
func (c *dynamicClient) XGroupSetID(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return c.GetClient().XGroupSetID(ctx, stream, group, start)
}
func (c *dynamicClient) XGroupDestroy(ctx context.Context, stream, group string) *redis.IntCmd {
	return c.GetClient().XGroupDestroy(ctx, stream, group)
}
func (c *dynamicClient) XGroupCreateConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	return c.GetClient().XGroupCreateConsumer(ctx, stream, group, consumer)
}
func (c *dynamicClient) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	return c.GetClient().XGroupDelConsumer(ctx, stream, group, consumer)
}
func (c *dynamicClient) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return c.GetClient().XReadGroup(ctx, a)
}
func (c *dynamicClient) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	return c.GetClient().XAck(ctx, stream, group, ids...)
}
func (c *dynamicClient) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	return c.GetClient().XPending(ctx, stream, group)
}
func (c *dynamicClient) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return c.GetClient().XPendingExt(ctx, a)
}
func (c *dynamicClient) XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
	return c.GetClient().XClaim(ctx, a)
}
func (c *dynamicClient) XClaimJustID(ctx context.Context, a *redis.XClaimArgs) *redis.StringSliceCmd {
	return c.GetClient().XClaimJustID(ctx, a)
}
func (c *dynamicClient) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	return c.GetClient().XAutoClaim(ctx, a)
}
func (c *dynamicClient) XAutoClaimJustID(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimJustIDCmd {
	return c.GetClient().XAutoClaimJustID(ctx, a)
}
func (c *dynamicClient) XTrim(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	return c.GetClient().XTrim(ctx, key, maxLen)
}
func (c *dynamicClient) XTrimApprox(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	return c.GetClient().XTrimApprox(ctx, key, maxLen)
}
func (c *dynamicClient) XTrimMaxLen(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	return c.GetClient().XTrimMaxLen(ctx, key, maxLen)
}
func (c *dynamicClient) XTrimMaxLenApprox(ctx context.Context, key string, maxLen, limit int64) *redis.IntCmd {
	return c.GetClient().XTrimMaxLenApprox(ctx, key, maxLen, limit)
}
func (c *dynamicClient) XTrimMinID(ctx context.Context, key string, minID string) *redis.IntCmd {
	return c.GetClient().XTrimMinID(ctx, key, minID)
}
func (c *dynamicClient) XTrimMinIDApprox(ctx context.Context, key string, minID string, limit int64) *redis.IntCmd {
	return c.GetClient().XTrimMinIDApprox(ctx, key, minID, limit)
}
func (c *dynamicClient) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	return c.GetClient().XInfoGroups(ctx, key)
}
func (c *dynamicClient) XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd {
	return c.GetClient().XInfoStream(ctx, key)
}
func (c *dynamicClient) XInfoStreamFull(ctx context.Context, key string, count int) *redis.XInfoStreamFullCmd {
	return c.GetClient().XInfoStreamFull(ctx, key, count)
}
func (c *dynamicClient) XInfoConsumers(ctx context.Context, key string, group string) *redis.XInfoConsumersCmd {
	return c.GetClient().XInfoConsumers(ctx, key, group)
}
func (c *dynamicClient) BZPopMax(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	return c.GetClient().BZPopMax(ctx, timeout, keys...)
}
func (c *dynamicClient) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	return c.GetClient().BZPopMin(ctx, timeout, keys...)
}
func (c *dynamicClient) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return c.GetClient().ZAdd(ctx, key, members...)
}
func (c *dynamicClient) ZAddNX(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return c.GetClient().ZAddNX(ctx, key, members...)
}
func (c *dynamicClient) ZAddXX(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return c.GetClient().ZAddXX(ctx, key, members...)
}
func (c *dynamicClient) ZAddCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return c.GetClient().ZAddCh(ctx, key, members...)
}
func (c *dynamicClient) ZAddNXCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return c.GetClient().ZAddNXCh(ctx, key, members...)
}
func (c *dynamicClient) ZAddXXCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return c.GetClient().ZAddXXCh(ctx, key, members...)
}
func (c *dynamicClient) ZAddArgs(ctx context.Context, key string, args redis.ZAddArgs) *redis.IntCmd {
	return c.GetClient().ZAddArgs(ctx, key, args)
}
func (c *dynamicClient) ZAddArgsIncr(ctx context.Context, key string, args redis.ZAddArgs) *redis.FloatCmd {
	return c.GetClient().ZAddArgsIncr(ctx, key, args)
}
func (c *dynamicClient) ZIncr(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	return c.GetClient().ZIncr(ctx, key, member)
}
func (c *dynamicClient) ZIncrNX(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	return c.GetClient().ZIncrNX(ctx, key, member)
}
func (c *dynamicClient) ZIncrXX(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	return c.GetClient().ZIncrXX(ctx, key, member)
}
func (c *dynamicClient) ZCard(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().ZCard(ctx, key)
}
func (c *dynamicClient) ZCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	return c.GetClient().ZCount(ctx, key, min, max)
}
func (c *dynamicClient) ZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	return c.GetClient().ZLexCount(ctx, key, min, max)
}
func (c *dynamicClient) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	return c.GetClient().ZIncrBy(ctx, key, increment, member)
}
func (c *dynamicClient) ZInter(ctx context.Context, store *redis.ZStore) *redis.StringSliceCmd {
	return c.GetClient().ZInter(ctx, store)
}
func (c *dynamicClient) ZInterWithScores(ctx context.Context, store *redis.ZStore) *redis.ZSliceCmd {
	return c.GetClient().ZInterWithScores(ctx, store)
}
func (c *dynamicClient) ZInterStore(ctx context.Context, destination string, store *redis.ZStore) *redis.IntCmd {
	return c.GetClient().ZInterStore(ctx, destination, store)
}
func (c *dynamicClient) ZMScore(ctx context.Context, key string, members ...string) *redis.FloatSliceCmd {
	return c.GetClient().ZMScore(ctx, key, members...)
}
func (c *dynamicClient) ZPopMax(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	return c.GetClient().ZPopMax(ctx, key, count...)
}
func (c *dynamicClient) ZPopMin(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	return c.GetClient().ZPopMin(ctx, key, count...)
}
func (c *dynamicClient) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.GetClient().ZRange(ctx, key, start, stop)
}
func (c *dynamicClient) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return c.GetClient().ZRangeWithScores(ctx, key, start, stop)
}
func (c *dynamicClient) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.GetClient().ZRangeByScore(ctx, key, opt)
}
func (c *dynamicClient) ZRangeByLex(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.GetClient().ZRangeByLex(ctx, key, opt)
}
func (c *dynamicClient) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return c.GetClient().ZRangeByScoreWithScores(ctx, key, opt)
}
func (c *dynamicClient) ZRangeArgs(ctx context.Context, z redis.ZRangeArgs) *redis.StringSliceCmd {
	return c.GetClient().ZRangeArgs(ctx, z)
}
func (c *dynamicClient) ZRangeArgsWithScores(ctx context.Context, z redis.ZRangeArgs) *redis.ZSliceCmd {
	return c.GetClient().ZRangeArgsWithScores(ctx, z)
}
func (c *dynamicClient) ZRangeStore(ctx context.Context, dst string, z redis.ZRangeArgs) *redis.IntCmd {
	return c.GetClient().ZRangeStore(ctx, dst, z)
}
func (c *dynamicClient) ZRank(ctx context.Context, key, member string) *redis.IntCmd {
	return c.GetClient().ZRank(ctx, key, member)
}
func (c *dynamicClient) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return c.GetClient().ZRem(ctx, key, members...)
}
func (c *dynamicClient) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	return c.GetClient().ZRemRangeByRank(ctx, key, start, stop)
}
func (c *dynamicClient) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	return c.GetClient().ZRemRangeByScore(ctx, key, min, max)
}
func (c *dynamicClient) ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd {
	return c.GetClient().ZRemRangeByLex(ctx, key, min, max)
}
func (c *dynamicClient) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.GetClient().ZRevRange(ctx, key, start, stop)
}
func (c *dynamicClient) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return c.GetClient().ZRevRangeWithScores(ctx, key, start, stop)
}
func (c *dynamicClient) ZRevRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.GetClient().ZRevRangeByScore(ctx, key, opt)
}
func (c *dynamicClient) ZRevRangeByLex(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.GetClient().ZRevRangeByLex(ctx, key, opt)
}
func (c *dynamicClient) ZRevRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return c.GetClient().ZRevRangeByScoreWithScores(ctx, key, opt)
}
func (c *dynamicClient) ZRevRank(ctx context.Context, key, member string) *redis.IntCmd {
	return c.GetClient().ZRevRank(ctx, key, member)
}
func (c *dynamicClient) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	return c.GetClient().ZScore(ctx, key, member)
}
func (c *dynamicClient) ZUnionStore(ctx context.Context, dest string, store *redis.ZStore) *redis.IntCmd {
	return c.GetClient().ZUnionStore(ctx, dest, store)
}
func (c *dynamicClient) ZUnion(ctx context.Context, store redis.ZStore) *redis.StringSliceCmd {
	return c.GetClient().ZUnion(ctx, store)
}
func (c *dynamicClient) ZUnionWithScores(ctx context.Context, store redis.ZStore) *redis.ZSliceCmd {
	return c.GetClient().ZUnionWithScores(ctx, store)
}
func (c *dynamicClient) ZRandMember(ctx context.Context, key string, count int, withScores bool) *redis.StringSliceCmd {
	return c.GetClient().ZRandMember(ctx, key, count, withScores)
}
func (c *dynamicClient) ZDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	return c.GetClient().ZDiff(ctx, keys...)
}
func (c *dynamicClient) ZDiffWithScores(ctx context.Context, keys ...string) *redis.ZSliceCmd {
	return c.GetClient().ZDiffWithScores(ctx, keys...)
}
func (c *dynamicClient) ZDiffStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	return c.GetClient().ZDiffStore(ctx, destination, keys...)
}
func (c *dynamicClient) PFAdd(ctx context.Context, key string, els ...interface{}) *redis.IntCmd {
	return c.GetClient().PFAdd(ctx, key, els...)
}
func (c *dynamicClient) PFCount(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.GetClient().PFCount(ctx, keys...)
}
func (c *dynamicClient) PFMerge(ctx context.Context, dest string, keys ...string) *redis.StatusCmd {
	return c.GetClient().PFMerge(ctx, dest, keys...)
}
func (c *dynamicClient) BgRewriteAOF(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().BgRewriteAOF(ctx)
}
func (c *dynamicClient) BgSave(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().BgSave(ctx)
}
func (c *dynamicClient) ClientKill(ctx context.Context, ipPort string) *redis.StatusCmd {
	return c.GetClient().ClientKill(ctx, ipPort)
}
func (c *dynamicClient) ClientKillByFilter(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.GetClient().ClientKillByFilter(ctx, keys...)
}
func (c *dynamicClient) ClientList(ctx context.Context) *redis.StringCmd {
	return c.GetClient().ClientList(ctx)
}
func (c *dynamicClient) ClientPause(ctx context.Context, dur time.Duration) *redis.BoolCmd {
	return c.GetClient().ClientPause(ctx, dur)
}
func (c *dynamicClient) ClientID(ctx context.Context) *redis.IntCmd {
	return c.GetClient().ClientID(ctx)
}
func (c *dynamicClient) ConfigGet(ctx context.Context, parameter string) *redis.SliceCmd {
	return c.GetClient().ConfigGet(ctx, parameter)
}
func (c *dynamicClient) ConfigResetStat(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ConfigResetStat(ctx)
}
func (c *dynamicClient) ConfigSet(ctx context.Context, parameter, value string) *redis.StatusCmd {
	return c.GetClient().ConfigSet(ctx, parameter, value)
}
func (c *dynamicClient) ConfigRewrite(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ConfigRewrite(ctx)
}
func (c *dynamicClient) DBSize(ctx context.Context) *redis.IntCmd {
	return c.GetClient().DBSize(ctx)
}
func (c *dynamicClient) FlushAll(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().FlushAll(ctx)
}
func (c *dynamicClient) FlushAllAsync(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().FlushAllAsync(ctx)
}
func (c *dynamicClient) FlushDB(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().FlushDB(ctx)
}
func (c *dynamicClient) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().FlushDBAsync(ctx)
}
func (c *dynamicClient) Info(ctx context.Context, section ...string) *redis.StringCmd {
	return c.GetClient().Info(ctx, section...)
}
func (c *dynamicClient) LastSave(ctx context.Context) *redis.IntCmd {
	return c.GetClient().LastSave(ctx)
}
func (c *dynamicClient) Save(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().Save(ctx)
}
func (c *dynamicClient) Shutdown(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().Shutdown(ctx)
}
func (c *dynamicClient) ShutdownSave(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ShutdownSave(ctx)
}
func (c *dynamicClient) ShutdownNoSave(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ShutdownNoSave(ctx)
}
func (c *dynamicClient) SlaveOf(ctx context.Context, host, port string) *redis.StatusCmd {
	return c.GetClient().SlaveOf(ctx, host, port)
}
func (c *dynamicClient) Time(ctx context.Context) *redis.TimeCmd {
	return c.GetClient().Time(ctx)
}
func (c *dynamicClient) DebugObject(ctx context.Context, key string) *redis.StringCmd {
	return c.GetClient().DebugObject(ctx, key)
}
func (c *dynamicClient) ReadOnly(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ReadOnly(ctx)
}
func (c *dynamicClient) ReadWrite(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ReadWrite(ctx)
}
func (c *dynamicClient) MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd {
	return c.GetClient().MemoryUsage(ctx, key, samples...)
}
func (c *dynamicClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return c.GetClient().Eval(ctx, script, keys, args...)
}
func (c *dynamicClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return c.GetClient().EvalSha(ctx, sha1, keys, args...)
}
func (c *dynamicClient) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return c.GetClient().ScriptExists(ctx, hashes...)
}
func (c *dynamicClient) ScriptFlush(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ScriptFlush(ctx)
}
func (c *dynamicClient) ScriptKill(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ScriptKill(ctx)
}
func (c *dynamicClient) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return c.GetClient().ScriptLoad(ctx, script)
}
func (c *dynamicClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	return c.GetClient().Publish(ctx, channel, message)
}
func (c *dynamicClient) PubSubChannels(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return c.GetClient().PubSubChannels(ctx, pattern)
}
func (c *dynamicClient) PubSubNumSub(ctx context.Context, channels ...string) *redis.StringIntMapCmd {
	return c.GetClient().PubSubNumSub(ctx, channels...)
}
func (c *dynamicClient) PubSubNumPat(ctx context.Context) *redis.IntCmd {
	return c.GetClient().PubSubNumPat(ctx)
}
func (c *dynamicClient) ClusterSlots(ctx context.Context) *redis.ClusterSlotsCmd {
	return c.GetClient().ClusterSlots(ctx)
}
func (c *dynamicClient) ClusterNodes(ctx context.Context) *redis.StringCmd {
	return c.GetClient().ClusterNodes(ctx)
}
func (c *dynamicClient) ClusterMeet(ctx context.Context, host, port string) *redis.StatusCmd {
	return c.GetClient().ClusterMeet(ctx, host, port)
}
func (c *dynamicClient) ClusterForget(ctx context.Context, nodeID string) *redis.StatusCmd {
	return c.GetClient().ClusterForget(ctx, nodeID)
}
func (c *dynamicClient) ClusterReplicate(ctx context.Context, nodeID string) *redis.StatusCmd {
	return c.GetClient().ClusterReplicate(ctx, nodeID)
}
func (c *dynamicClient) ClusterResetSoft(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ClusterResetSoft(ctx)
}
func (c *dynamicClient) ClusterResetHard(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ClusterResetHard(ctx)
}
func (c *dynamicClient) ClusterInfo(ctx context.Context) *redis.StringCmd {
	return c.GetClient().ClusterInfo(ctx)
}
func (c *dynamicClient) ClusterKeySlot(ctx context.Context, key string) *redis.IntCmd {
	return c.GetClient().ClusterKeySlot(ctx, key)
}
func (c *dynamicClient) ClusterGetKeysInSlot(ctx context.Context, slot int, count int) *redis.StringSliceCmd {
	return c.GetClient().ClusterGetKeysInSlot(ctx, slot, count)
}
func (c *dynamicClient) ClusterCountFailureReports(ctx context.Context, nodeID string) *redis.IntCmd {
	return c.GetClient().ClusterCountFailureReports(ctx, nodeID)
}
func (c *dynamicClient) ClusterCountKeysInSlot(ctx context.Context, slot int) *redis.IntCmd {
	return c.GetClient().ClusterCountKeysInSlot(ctx, slot)
}
func (c *dynamicClient) ClusterDelSlots(ctx context.Context, slots ...int) *redis.StatusCmd {
	return c.GetClient().ClusterDelSlots(ctx, slots...)
}
func (c *dynamicClient) ClusterDelSlotsRange(ctx context.Context, min, max int) *redis.StatusCmd {
	return c.GetClient().ClusterDelSlotsRange(ctx, min, max)
}
func (c *dynamicClient) ClusterSaveConfig(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ClusterSaveConfig(ctx)
}
func (c *dynamicClient) ClusterSlaves(ctx context.Context, nodeID string) *redis.StringSliceCmd {
	return c.GetClient().ClusterSlaves(ctx, nodeID)
}
func (c *dynamicClient) ClusterFailover(ctx context.Context) *redis.StatusCmd {
	return c.GetClient().ClusterFailover(ctx)
}
func (c *dynamicClient) ClusterAddSlots(ctx context.Context, slots ...int) *redis.StatusCmd {
	return c.GetClient().ClusterAddSlots(ctx, slots...)
}
func (c *dynamicClient) ClusterAddSlotsRange(ctx context.Context, min, max int) *redis.StatusCmd {
	return c.GetClient().ClusterAddSlotsRange(ctx, min, max)
}
func (c *dynamicClient) GeoAdd(ctx context.Context, key string, geoLocation ...*redis.GeoLocation) *redis.IntCmd {
	return c.GetClient().GeoAdd(ctx, key, geoLocation...)
}
func (c *dynamicClient) GeoPos(ctx context.Context, key string, members ...string) *redis.GeoPosCmd {
	return c.GetClient().GeoPos(ctx, key, members...)
}
func (c *dynamicClient) GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	return c.GetClient().GeoRadius(ctx, key, longitude, latitude, query)
}
func (c *dynamicClient) GeoRadiusStore(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.IntCmd {
	return c.GetClient().GeoRadiusStore(ctx, key, longitude, latitude, query)
}
func (c *dynamicClient) GeoRadiusByMember(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	return c.GetClient().GeoRadiusByMember(ctx, key, member, query)
}
func (c *dynamicClient) GeoRadiusByMemberStore(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.IntCmd {
	return c.GetClient().GeoRadiusByMemberStore(ctx, key, member, query)
}
func (c *dynamicClient) GeoSearch(ctx context.Context, key string, q *redis.GeoSearchQuery) *redis.StringSliceCmd {
	return c.GetClient().GeoSearch(ctx, key, q)
}
func (c *dynamicClient) GeoSearchLocation(ctx context.Context, key string, q *redis.GeoSearchLocationQuery) *redis.GeoSearchLocationCmd {
	return c.GetClient().GeoSearchLocation(ctx, key, q)
}
func (c *dynamicClient) GeoSearchStore(ctx context.Context, key, store string, q *redis.GeoSearchStoreQuery) *redis.IntCmd {
	return c.GetClient().GeoSearchStore(ctx, key, store, q)
}
func (c *dynamicClient) GeoDist(ctx context.Context, key string, member1, member2, unit string) *redis.FloatCmd {
	return c.GetClient().GeoDist(ctx, key, member1, member2, unit)
}
func (c *dynamicClient) GeoHash(ctx context.Context, key string, members ...string) *redis.StringSliceCmd {
	return c.GetClient().GeoHash(ctx, key, members...)
}
