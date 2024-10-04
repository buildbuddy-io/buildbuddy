// Run a ClickHouse cluster locally using docker-compose.
//
// Example usage:
//
// [terminal-1]$ bb run enterprise/tools/clickhouse_cluster
// [terminal-2]$ bb run enterprise/server -- $(bb run -- enterprise/tools/clickhouse_cluster flags)

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	// TODO: shards?

	replicas       = flag.Int("replicas", 2, "Number of ClickHouse servers to run. Each server will store a copy of the DB.")
	serverLogLevel = flag.String("server_log_level", "information", "ClickHouse server log level: trace, debug, information, warning, error")
)

const (
	clusterName = "bb_clickhouse_cluster_local"

	zookeeperImage        = "docker.io/zookeeper:3.8.0"
	clickhouseServerImage = "clickhouse/clickhouse-server:23.8"

	// Base port number for client connections.
	// The replica number is added to this, starting from 1.
	// So replica1 = port 9201, replica2 = port 9202, ...
	clickhouseBaseTCPPortNumber = 9200

	// Base port number for HTTP API, e.g. for making curl requests.
	// The replica number is added to this, starting from 1.
	// So replica1 = port 8225, replica2 = port 8226, ...
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
		// Print BuildBuddy server flags and exit.
		args := []string{
			"--olap_database.data_source=clickhouse://default:@127.0.0.1:9201/default",
			"--olap_database.enable_data_replication=true",
		}
		fmt.Println(strings.Join(args, " "))
		return nil
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	tmp, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("make temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	// Populate the config directory for each clickhouse server
	for r := 1; r <= *replicas; r++ {
		serverDir := filepath.Join(tmp, fmt.Sprintf("clickhouse%d", r))
		if err := os.MkdirAll(serverDir, 0755); err != nil {
			return fmt.Errorf("make clickhouse server config dir: %w", err)
		}
		configXML := getConfigXML(r)
		if err := os.WriteFile(filepath.Join(serverDir, "config.xml"), []byte(configXML), 0644); err != nil {
			return fmt.Errorf("write config.xml: %w", err)
		}
	}

	yml := getDockerComposeConfig(tmp)
	dockerComposeYAMLPath := filepath.Join(tmp, "docker-compose.clickhouse-cluster.yaml")
	if err := os.WriteFile(dockerComposeYAMLPath, []byte(yml), 0644); err != nil {
		return fmt.Errorf("write docker compose YAML: %w", err)
	}

	// Run docker-compose.
	// (Don't use CommandContext since we want graceful shutdown.)
	cmd := exec.Command("docker-compose", "--file="+dockerComposeYAMLPath, "up", "--abort-on-container-exit")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start docker-compose: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = cmd.Process.Signal(os.Interrupt)
	}()

	_ = cmd.Wait()

	{
		cmd := exec.Command("docker-compose", "--file="+dockerComposeYAMLPath, "down")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		_ = cmd.Run()
	}

	return nil
}

func getConfigXML(r int) string {
	xml := `
<clickhouse>
	<logger>
		<level>` + *serverLogLevel + `</level>
		<console>1</console>
		<log>/var/log/clickhouse-server/clickhouse-server.log</log>
		<errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
		<size>1000M</size>
		<count>10</count>
	</logger>

	<remote_servers>
		<` + clusterName + `>
			<shard>
`
	for r := 1; r <= *replicas; r++ {
		xml += `
				<replica>
					<host>clickhouse` + fmt.Sprint(r) + `</host>
					<port>9000</port>
				</replica>
`
	}
	xml += `
			</shard>
		</` + clusterName + `>
	</remote_servers>

	<zookeeper>
		<node>
			<host>zookeeper</host>
			<port>2181</port>
		</node>
	</zookeeper>

	<macros>
		<installation>local</installation>
		<cluster>` + clusterName + `</cluster>
		<shard>shard1</shard>
		<replica>replica` + fmt.Sprint(r) + `</replica>
	</macros>
</clickhouse>
`
	return xml
}

func getDockerComposeConfig(tmp string) string {
	user := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	yml := `
version: "3.3"
services:
  zookeeper:
    image: "` + zookeeperImage + `"
    user: "` + user + `"
    container_name: "zookeeper"
    ports:
      - "2181:2181"
`
	for r := 1; r <= *replicas; r++ {
		configDir := filepath.Join(tmp, fmt.Sprintf("clickhouse%d", r))
		yml += `
  clickhouse` + fmt.Sprint(r) + `:
    image: "` + clickhouseServerImage + `"
    ulimits: { nofile: { soft: 262144, hard: 262144 } }
    cap_add: [ "NET_ADMIN", "SYS_NICE", "IPC_LOCK" ]
    user: "` + user + `"
    container_name: "clickhouse` + fmt.Sprint(r) + `"
    user: "` + user + `"
    volumes:
      - "` + configDir + `/config.xml:/etc/clickhouse-server/config.d/config.xml:ro"
    depends_on:
      - zookeeper
    ports:
      - "` + fmt.Sprint(clickhouseTCPPortNumber(r)) + `:9000"
      - "` + fmt.Sprint(clickhouseHTTPPortNumber(r)) + `:8123"
`
	}
	return yml
}

func clickhouseHTTPPortNumber(r int) int {
	return clickhouseBaseHTTPPortNumber + r
}

func clickhouseTCPPortNumber(r int) int {
	return clickhouseBaseTCPPortNumber + r
}
