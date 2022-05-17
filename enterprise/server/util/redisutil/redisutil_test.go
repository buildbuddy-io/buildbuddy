package redisutil_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// noExpiration is a duration constant used to indicate to redis that a value
	// should not expire.
	noExpiration time.Duration = 0
)

func genOptions(scheme, user, password, addr, database string) *redis.Options {
	if scheme != "redis" && scheme != "rediss" && scheme != "unix" {
		return nil
	}
	if addr == "" {
		return &redis.Options{}
	}
	database_num, err := strconv.Atoi(database)
	if database != "" && err != nil {
		return &redis.Options{}
	}
	network := "unix"
	var tls_config *tls.Config
	if strings.HasPrefix(scheme, "redis") {
		network = "tcp"
		if host, port, err := net.SplitHostPort(addr); err == nil {
			if host == "" {
				host = "localhost"
			}
			if port == "" {
				port = "6379"
			}
			addr = net.JoinHostPort(host, port)
			if scheme == "rediss" {
				tls_config = &tls.Config{ServerName: host}
			}
		} else {
			return &redis.Options{}
		}
	}
	return &redis.Options{
		Network:   network,
		Username:  user,
		Password:  password,
		Addr:      addr,
		DB:        database_num,
		TLSConfig: tls_config,
	}
}

func genURI(scheme, user, password, addr, database string) string {
	if scheme != "redis" && scheme != "rediss" && scheme != "unix" {
		return ""
	}
	at := ""
	if user != "" {
		at = "@"
	}
	if password != "" {
		password = ":" + password
		at = "@"
	}

	hostport := ""
	path := ""
	query := ""
	if scheme == "unix" {
		path = addr
		if database != "" {
			query = "?db=" + database
		}
	}

	if strings.HasPrefix(scheme, "redis") {
		if host, port, err := net.SplitHostPort(addr); err == nil {
			hostport = host
			if port != "" {
				hostport = net.JoinHostPort(host, port)
			}
		} else {
			return ""
		}
		if database != "" {
			path = "/" + database
		}
	}

	return fmt.Sprintf(
		"%s://%s%s%s%s%s%s",
		scheme,
		user,
		password,
		at,
		hostport,
		path,
		query,
	)
}

func genRedisURIOptionsMap(schemes, users, passwords, hostnames, ports, databases []string) map[string]*redis.Options {
	product := make(map[string]*redis.Options)
	for _, scheme := range schemes {
		for _, user := range users {
			for _, password := range passwords {
				for _, hostname := range hostnames {
					for _, port := range ports {
						for _, db := range databases {
							uri := genURI(
								scheme,
								user,
								password,
								net.JoinHostPort(hostname, port),
								db,
							)
							product[uri] =
								genOptions(
									scheme,
									user,
									password,
									net.JoinHostPort(hostname, port),
									db,
								)
						}
					}
				}
			}
		}
	}
	return product
}

func genUnixURIOptionsMap(users, passwords, socket_paths, databases []string) map[string]*redis.Options {
	product := make(map[string]*redis.Options)
	for _, user := range users {
		for _, password := range passwords {
			for _, socket_path := range socket_paths {
				for _, database := range databases {
					uri := genURI(
						"unix",
						user,
						password,
						socket_path,
						database,
					)
					product[uri] = genOptions(
						"unix",
						user,
						password,
						socket_path,
						database,
					)
				}
			}
		}
	}
	return product
}

func assertOptionsEqual(
	t *testing.T,
	opt1 *redis.Options,
	opt2 *redis.Options,
	target string,
	error_msg string,
) {
	ignore_opts := cmpopts.IgnoreFields(
		redis.Options{},
		"readOnly",
		"TLSConfig.mutex",
		"TLSConfig.sessionTicketKeys",
		"TLSConfig.autoSessionTicketKeys",
	)
	if !cmp.Equal(
		opt1,
		opt2,
		ignore_opts,
	) {
		t.Errorf(
			fmt.Sprintf(
				"%s\nFailing string: %s\nDiff:\n%s",
				error_msg,
				target,
				cmp.Diff(
					opt1,
					opt2,
					ignore_opts,
				),
			),
		)
	}
}

func TestTargetToOptionsWithoutRedisConnectionURI(t *testing.T) {

	targets := []string{
		"",
		":",
		":6379",
		"hostname:6379",
		"redis.example.com:90",
		"redis",
		"redis:100",
		"rediss",
		"rediss:100",
		"http://tam",
	}

	for _, target := range targets {
		assertOptionsEqual(
			t,
			&redis.Options{Addr: target},
			redisutil.TargetToOptions(target),
			target,
			"Non-redis URI strings should produce "+
				"options containing only an Addr with that "+
				"string.",
		)
	}
}

func TestTargetToOptionsWithConnectionURI(t *testing.T) {

	test_map := func(target_option_map map[string]*redis.Options) {
		for target, opt := range target_option_map {
			test_opt := redisutil.TargetToOptions(target)
			assertOptionsEqual(
				t,
				opt,
				test_opt,
				target,
				"Connection URI string did "+
					"not match expectation.",
			)
		}
	}

	redis_targets_with_options := genRedisURIOptionsMap(
		[]string{"redis", "rediss"},
		[]string{"", "user", "redis"},
		[]string{"", "password", "hunter2"},
		[]string{"", "remotehost", "redis.example.com", "::1"},
		[]string{"", "1000", "5", "6379"},
		[]string{"", "0", "5", "500", "100/2", "200.1", "bad", "05a"},
	)
	test_map(redis_targets_with_options)

	unix_targets_with_options := genUnixURIOptionsMap(
		[]string{"", "user", "redis"},
		[]string{"", "password", "hunter2"},
		[]string{"", "/", "/path", "/path/to", "/path/to/socket"},
		[]string{"", "0", "5", "500", "100/2", "200.1", "bad", "05a"},
	)
	test_map(unix_targets_with_options)

	empty := &redis.Options{}
	bad_targets_with_options := map[string]*redis.Options{
		"redis://u:p@h:2/9?jam=tam":        empty,
		"rediss://u:p@h:2?database=0":      empty,
		"rediss://u@h:2/0?password=pw":     empty,
		"rediss://u:p@::1?database=0":      empty,
		"rediss://u@[::1]:5/0?password=pw": empty,
	}
	test_map(bad_targets_with_options)

}

func TestCommandBuffer(t *testing.T) {
	addr := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(addr))
	ctx := context.Background()

	buf := redisutil.NewCommandBuffer(rdb)

	err := buf.Set(ctx, "key1", "val1", noExpiration)
	require.NoError(t, err)
	err = buf.Set(ctx, "key1", "val1_override", noExpiration)
	require.NoError(t, err)
	err = buf.Set(ctx, "key2", "val2", noExpiration)
	require.NoError(t, err)

	err = buf.HIncrBy(ctx, "hash1", "h1_field1", 1)
	require.NoError(t, err)
	err = buf.HIncrBy(ctx, "hash1", "h1_field1", 10)
	require.NoError(t, err)
	err = buf.HIncrBy(ctx, "hash1", "h1_field2", 100)
	require.NoError(t, err)
	err = buf.HIncrBy(ctx, "hash2", "h2_field1", 1000)
	require.NoError(t, err)

	err = buf.IncrBy(ctx, "counter1", 1)
	require.NoError(t, err)
	err = buf.IncrBy(ctx, "counter1", 10)
	require.NoError(t, err)
	err = buf.IncrBy(ctx, "counter2", 100)
	require.NoError(t, err)

	err = buf.SAdd(ctx, "set1", "1", "2")
	require.NoError(t, err)
	err = buf.SAdd(ctx, "set1", 2, 3)
	require.NoError(t, err)
	err = buf.SAdd(ctx, "set2", "1")
	require.NoError(t, err)

	err = buf.RPush(ctx, "list1", "1", "2")
	require.NoError(t, err)
	err = buf.RPush(ctx, "list1", "3")
	require.NoError(t, err)
	err = buf.RPush(ctx, "list2", "A")
	require.NoError(t, err)

	// Create 2 keys that don't expire (initially).
	_, err = rdb.Set(ctx, "expiring1", "value1", 0).Result()
	require.NoError(t, err)
	_, err = rdb.Set(ctx, "expiring2", "value2", 0).Result()
	require.NoError(t, err)
	// Now add 2 EXPIRE commands to the buffer: the first key should expire
	// immediately but the second should expire long after the test completes.
	err = buf.Expire(ctx, "expiring1", 0)
	require.NoError(t, err)
	err = buf.Expire(ctx, "expiring2", 24*time.Hour)
	require.NoError(t, err)

	// Flush all buffered values.
	err = buf.Flush(ctx)
	require.NoError(t, err)

	// Assert Redis has the values we're expecting.

	val1, err := rdb.Get(ctx, "key1").Result()
	require.NoError(t, err)
	assert.Equal(t, "val1_override", val1)
	val2, err := rdb.Get(ctx, "key2").Result()
	require.NoError(t, err)
	assert.Equal(t, "val2", val2)

	h1, err := rdb.HGetAll(ctx, "hash1").Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"h1_field1": "11",
		"h1_field2": "100",
	}, h1, "HIncrBy not working as expected")
	h2, err := rdb.HGetAll(ctx, "hash2").Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"h2_field1": "1000",
	}, h2, "HIncrBy not working as expected")

	c1, err := rdb.Get(ctx, "counter1").Result()
	require.NoError(t, err)
	assert.Equal(t, "11", c1, "IncrBy not working as expected")
	c2, err := rdb.Get(ctx, "counter2").Result()
	require.NoError(t, err)
	assert.Equal(t, "100", c2, "IncrBy not working as expected")

	s1, err := rdb.SMembers(ctx, "set1").Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, s1, "SAdd not working as expected")
	s2, err := rdb.SMembers(ctx, "set2").Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1"}, s2, "SAdd not working as expected")

	list1, err := rdb.LRange(ctx, "list1", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, list1)
	list2, err := rdb.LRange(ctx, "list2", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"A"}, list2)

	_, err = rdb.Get(ctx, "expiring1").Result()
	assert.Equal(t, redis.Nil, err)
	e2, err := rdb.Get(ctx, "expiring2").Result()
	require.NoError(t, err)
	assert.Equal(t, "value2", e2)
}

func TestCommandBuffer_PostShutdown(t *testing.T) {
	addr := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(addr))
	ctx := context.Background()

	buf := redisutil.NewCommandBuffer(rdb)

	// Start the background flush then immediately stop it, which puts the buffer
	// into a state where all new commands get forwarded directly to Redis.
	buf.StartPeriodicFlush(ctx)
	err := buf.StopPeriodicFlush(ctx)
	require.NoError(t, err)

	err = buf.HIncrBy(ctx, "hash1", "h1_field1", 1)
	require.NoError(t, err)
	err = buf.HIncrBy(ctx, "hash1", "h1_field1", 10)
	require.NoError(t, err)
	err = buf.HIncrBy(ctx, "hash1", "h1_field2", 100)
	require.NoError(t, err)
	err = buf.HIncrBy(ctx, "hash2", "h2_field1", 1000)
	require.NoError(t, err)

	err = buf.IncrBy(ctx, "counter1", 1)
	require.NoError(t, err)
	err = buf.IncrBy(ctx, "counter1", 10)
	require.NoError(t, err)
	err = buf.IncrBy(ctx, "counter2", 100)
	require.NoError(t, err)

	err = buf.SAdd(ctx, "set1", "1", "2")
	require.NoError(t, err)
	err = buf.SAdd(ctx, "set1", 2, 3)
	require.NoError(t, err)
	err = buf.SAdd(ctx, "set2", "1")
	require.NoError(t, err)

	// Create 2 keys that don't expire (initially).
	_, err = rdb.Set(ctx, "expiring1", "value1", 0).Result()
	require.NoError(t, err)
	_, err = rdb.Set(ctx, "expiring2", "value2", 0).Result()
	require.NoError(t, err)
	// Now add 2 EXPIRE commands to the buffer: the first key should expire
	// immediately but the second should expire long after the test completes.
	err = buf.Expire(ctx, "expiring1", 0)
	err = buf.Expire(ctx, "expiring2", 24*time.Hour)

	// Note: No need to flush, since all commands should have gone directly to
	// Redis.

	// Assert Redis has the values we're expecting.
	h1, err := rdb.HGetAll(ctx, "hash1").Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"h1_field1": "11",
		"h1_field2": "100",
	}, h1, "HIncrBy not working as expected")
	h2, err := rdb.HGetAll(ctx, "hash2").Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"h2_field1": "1000",
	}, h2, "HIncrBy not working as expected")

	c1, err := rdb.Get(ctx, "counter1").Result()
	require.NoError(t, err)
	assert.Equal(t, "11", c1, "IncrBy not working as expected")
	c2, err := rdb.Get(ctx, "counter2").Result()
	require.NoError(t, err)
	assert.Equal(t, "100", c2, "IncrBy not working as expected")

	s1, err := rdb.SMembers(ctx, "set1").Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, s1, "SAdd not working as expected")
	s2, err := rdb.SMembers(ctx, "set2").Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1"}, s2, "SAdd not working as expected")

	_, err = rdb.Get(ctx, "expiring1").Result()
	assert.Equal(t, redis.Nil, err)
	e2, err := rdb.Get(ctx, "expiring2").Result()
	require.NoError(t, err)
	assert.Equal(t, "value2", e2)
}

func BenchmarkCommandBuffer_Flush_HIncrBy(b *testing.B) {
	addr := testredis.Start(b).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(addr))
	ctx := context.Background()
	buf := redisutil.NewCommandBuffer(rdb)

	for _, p := range []struct {
		nRedisKeys int
		nHashKeys  int
	}{
		{1, 1},
		{10, 1},
		{1, 10},
		{10, 10},
		{100, 100},
	} {
		p := p
		b.Run(fmt.Sprintf("Keys=%d,Fields=%d,", p.nRedisKeys, p.nHashKeys), func(b *testing.B) {
			for i := 0; i < b.N; i++ {

				for r := 0; r < p.nRedisKeys; r++ {
					for h := 0; h < p.nHashKeys; h++ {
						err := buf.HIncrBy(ctx, fmt.Sprintf("test_redis_key_%d", r), fmt.Sprintf("test_hash_key_%d", h), 1)
						require.NoError(b, err)
					}
				}
				err := buf.Flush(ctx)
				require.NoError(b, err)
			}
		})
	}
}
