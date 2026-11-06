// Run a local replicated ClickHouse cluster using a Bazel-provisioned binary.
//
// Example usage:
//
// [terminal-1]$ bb run enterprise/tools/clickhouse_cluster
// [terminal-2]$ bb run enterprise/server -- $(bb run -- enterprise/tools/clickhouse_cluster flags)

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"html"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

var (
	replicas       = flag.Int("replicas", 2, "Number of ClickHouse servers to run. Each server will store a copy of the DB.")
	serverLogLevel = flag.String("server_log_level", "information", "ClickHouse server log level: trace, debug, information, warning, error")
	configFiles    = flag.Slice("config_file", []string{}, "Additional ClickHouse XML config files to merge into each server's configuration. Paths inside XML refer to the host filesystem; relative paths are resolved from each server's temporary directory.")
	envFiles       = flag.Slice("env_file", []string{}, "Docker-style KEY=VALUE files to make available to each server. Useful for populating env vars in XML config files.")
)

// Set via x_defs in the BUILD file.
var clickhouseRlocationpath string

const (
	clusterName                  = "bb_clickhouse_cluster_local"
	clickhouseBaseTCPPortNumber  = 9200
	clickhouseBaseHTTPPortNumber = 8224
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if len(flag.Args()) > 0 && flag.Args()[0] == "flags" {
		fmt.Println("--olap_database.data_source=clickhouse://default:@127.0.0.1:9201/default --olap_database.enable_data_replication=true")
		return nil
	}
	if *replicas < 1 {
		return fmt.Errorf("replicas must be positive")
	}
	var configs [][]byte
	for _, path := range *configFiles {
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read config_file: %w", err)
		}
		configs = append(configs, content)
	}
	envValues, err := readEnvFiles(*envFiles)
	if err != nil {
		return err
	}
	env := os.Environ()
	for _, key := range slices.Sorted(maps.Keys(envValues)) {
		env = append(env, key+"="+envValues[key])
	}
	// Supervise the server directly, without ClickHouse's watchdog subprocess.
	env = append(env, "CLICKHOUSE_WATCHDOG_ENABLE=0")
	binary, err := runfiles.Rlocation(clickhouseRlocationpath)
	if err != nil {
		return err
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp("", "buildbuddy-clickhouse-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	group, ctx := errgroup.WithContext(ctx)
	for r := 1; r <= *replicas; r++ {
		group.Go(func() error {
			name := fmt.Sprintf("clickhouse%d", r)
			dir := filepath.Join(tmp, name)
			if err := os.MkdirAll(filepath.Join(dir, "config.d"), 0700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(dir, "config.xml"), []byte(getConfigXML(r)), 0600); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(dir, "users.xml"), []byte(usersConfigXML), 0600); err != nil {
				return err
			}
			for i, content := range configs {
				if err := os.WriteFile(filepath.Join(dir, "config.d", fmt.Sprintf("%04d.xml", i)), content, 0600); err != nil {
					return err
				}
			}
			return runProcess(ctx, binary, name, dir, env)
		})
	}
	// Reap every child before removing its data, including after partial startup.
	return group.Wait()
}

func runProcess(ctx context.Context, binary, name, dir string, env []string) error {
	cmd := exec.CommandContext(ctx, binary, "server", "--config-file="+filepath.Join(dir, "config.xml"))
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 30 * time.Second
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("start %s: %w", name, err)
	}
	err := cmd.Wait()
	if ctx.Err() != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("%s exited: %w", name, err)
	}
	return fmt.Errorf("%s exited unexpectedly", name)
}

// readEnvFiles reads Docker-style env files. A key without '=' inherits its
// value from the host environment; later files override earlier files.
func readEnvFiles(files []string) (map[string]string, error) {
	env := make(map[string]string)
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimLeft(scanner.Text(), " \t\ufeff")
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, value, hasValue := strings.Cut(line, "=")
			if key == "" || strings.ContainsAny(key, " \t\x00") {
				f.Close()
				return nil, fmt.Errorf("invalid environment key in %s", path)
			}
			if !hasValue {
				var ok bool
				value, ok = os.LookupEnv(key)
				if !ok {
					continue
				}
			}
			env[key] = value
		}
		err = scanner.Err()
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("read env file %s: %w", path, err)
		}
	}
	return env, nil
}

func getConfigXML(r int) string {
	var xml strings.Builder
	fmt.Fprintf(&xml, `
<clickhouse>
 <logger><level>%s</level><console>1</console></logger>
 <listen_host>127.0.0.1</listen_host>
 <interserver_listen_host>127.0.0.1</interserver_listen_host>
 <tcp_port>%d</tcp_port>
 <http_port>%d</http_port>
 <interserver_http_host>127.0.0.1</interserver_http_host>
 <interserver_http_port>%d</interserver_http_port>
 <path>./data/</path>
 <tmp_path>./tmp/</tmp_path>
 <user_files_path>./user_files/</user_files_path>
 <format_schema_path>./format_schemas/</format_schema_path>
 <user_directories>
  <users_xml><path>users.xml</path></users_xml>
  <local_directory><path>./access/</path></local_directory>
 </user_directories>
 <background_schedule_pool_size>16</background_schedule_pool_size>
 <remote_servers>
  <%s>
   <shard>
`, html.EscapeString(*serverLogLevel), clickhouseTCPPortNumber(r), clickhouseHTTPPortNumber(r), 9300+r, clusterName)
	for r := 1; r <= *replicas; r++ {
		fmt.Fprintf(&xml, `
    <replica><host>127.0.0.1</host><port>%d</port></replica>
`, clickhouseTCPPortNumber(r))
	}
	fmt.Fprintf(&xml, `
   </shard>
  </%s>
 </remote_servers>
 <zookeeper><node><host>127.0.0.1</host><port>2181</port></node></zookeeper>
 <distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>
 <macros>
  <installation>local</installation>
  <cluster>%s</cluster>
  <shard>shard1</shard>
  <replica>replica%d</replica>
 </macros>
`, clusterName, clusterName, r)
	if r == 1 {
		// The first server waits for its embedded Keeper before accepting clients.
		xml.WriteString(keeperConfigXML)
	}
	xml.WriteString("</clickhouse>\n")
	return xml.String()
}

const usersConfigXML = `
<clickhouse>
 <profiles><default/></profiles>
 <users>
  <default>
   <password/>
   <networks><ip>127.0.0.1</ip></networks>
   <profile>default</profile>
   <quota>default</quota>
   <access_management>1</access_management>
  </default>
 </users>
 <quotas><default/></quotas>
</clickhouse>
`

const keeperConfigXML = `
 <keeper_server>
  <tcp_port>2181</tcp_port>
  <server_id>1</server_id>
  <log_storage_path>./keeper/logs/</log_storage_path>
  <snapshot_storage_path>./keeper/snapshots/</snapshot_storage_path>
  <raft_configuration>
   <server><id>1</id><hostname>127.0.0.1</hostname><port>9234</port></server>
  </raft_configuration>
 </keeper_server>
`

func clickhouseHTTPPortNumber(r int) int {
	return clickhouseBaseHTTPPortNumber + r
}

func clickhouseTCPPortNumber(r int) int {
	return clickhouseBaseTCPPortNumber + r
}
