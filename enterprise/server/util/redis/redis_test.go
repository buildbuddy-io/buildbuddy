package redis_test

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	redisutil "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redis"
	redis "github.com/go-redis/redis/v8"
)

func genRedisOptions(
	protocol string,
	user string,
	password string,
	hostname string,
	port string,
	database string,
) *redis.Options {
	database_num, err := strconv.Atoi(database)
	if database != "" && err != nil {
		return &redis.Options{}
	}
	if hostname == "" {
		hostname = "localhost"
	}
	if port == "" {
		port = "6379"
	}
	opt := &redis.Options{
		Network:  "tcp",
		Username: user,
		Password: password,
		Addr:     net.JoinHostPort(hostname, port),
		DB:       database_num,
	}
	if protocol == "rediss" {
		opt.TLSConfig = &tls.Config{ServerName: hostname}
	}
	return opt
}

func genRedisURI(
	protocol string,
	user string,
	password string,
	hostname string,
	port string,
	database string,
) string {
	at := ""
	if user != "" {
		at = "@"
	}
	if password != "" {
		password = ":" + password
		at = "@"
	}
	hostport := hostname
	if port != "" {
		hostport = net.JoinHostPort(hostname, port)
	}
	if database != "" {
		database = "/" + database
	}

	return fmt.Sprintf(
		"%s://%s%s%s%s%s",
		protocol,
		user,
		password,
		at,
		hostport,
		database,
	)
}

func genRedisURIOptionsMap(
	protocols []string,
	users []string,
	passwords []string,
	hostnames []string,
	ports []string,
	databases []string,
) map[string]*redis.Options {
	product := make(map[string]*redis.Options)
	for _, protocol := range protocols {
		for _, user := range users {
			for _, password := range passwords {
				for _, hostname := range hostnames {
					for _, port := range ports {
						for _, db := range databases {
							uri := genRedisURI(
								protocol,
								user,
								password,
								hostname,
								port,
								db,
							)
							product[uri] =
								genRedisOptions(
									protocol,
									user,
									password,
									hostname,
									port,
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

func genUnixOptions(
	user string,
	password string,
	socket_path string,
	database string,
) *redis.Options {
	if socket_path == "" {
		return &redis.Options{}
	}
	database_num, err := strconv.Atoi(database)
	if database != "" && err != nil {
		return &redis.Options{}
	}
	return &redis.Options{
		Network:  "unix",
		Username: user,
		Password: password,
		Addr:     socket_path,
		DB:       database_num,
	}
}

func genUnixURI(
	user string,
	password string,
	socket_path string,
	database string,
) string {
	at := ""
	if user != "" {
		at = "@"
	}
	if password != "" {
		password = ":" + password
		at = "@"
	}
	if database != "" {
		database = "?db=" + database
	}
	return fmt.Sprintf(
		"unix://%s%s%s%s%s",
		user,
		password,
		at,
		socket_path,
		database,
	)
}

func genUnixURIOptionsMap(
	users []string,
	passwords []string,
	socket_paths []string,
	databases []string,
) map[string]*redis.Options {
	product := make(map[string]*redis.Options)
	for _, user := range users {
		for _, password := range passwords {
			for _, socket_path := range socket_paths {
				for _, database := range databases {
					uri := genUnixURI(
						user,
						password,
						socket_path,
						database,
					)
					product[uri] = genUnixOptions(
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
