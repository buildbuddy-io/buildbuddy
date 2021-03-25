package redisutil_test

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
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
