// Run a ClickHouse cluster locally using the Docker API.
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
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"golang.org/x/sync/errgroup"

	units "github.com/docker/go-units"
	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
)

var (
	// TODO: shards?

	replicas       = flag.Int("replicas", 2, "Number of ClickHouse servers to run. Each server will store a copy of the DB.")
	serverLogLevel = flag.String("server_log_level", "information", "ClickHouse server log level: trace, debug, information, warning, error")
	configFiles    = flag.Slice("config_file", []string{}, "Additional ClickHouse XML config file paths on the host to merge into the default and generated config files.")
	envFiles       = flag.Slice("env_file", []string{}, "Docker-style KEY=VALUE files to make available to each server. Useful for populating env vars in XML config files.")
	volumes        = flag.Slice("volume", []string{}, "Volume to mount into each clickhouse server container (can be specified multiple times).")
)

const (
	clusterName = "bb_clickhouse_cluster_local"

	zookeeperImage        = "docker.io/zookeeper:3.8"
	clickhouseServerImage = "clickhouse/clickhouse-server:25.3"

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

func run() (retErr error) {
	if len(flag.Args()) > 0 && flag.Args()[0] == "flags" {
		// Print BuildBuddy server flags and exit.
		args := []string{
			"--olap_database.data_source=clickhouse://default:@127.0.0.1:9201/default",
			"--olap_database.enable_data_replication=true",
		}
		fmt.Println(strings.Join(args, " "))
		return nil
	}

	if *replicas < 1 {
		return fmt.Errorf("replicas must be positive")
	}
	// Resolve host paths before passing them to the Docker daemon.
	var configs []string
	for _, configFile := range *configFiles {
		configFile, err := filepath.Abs(configFile)
		if err != nil {
			return err
		}
		if _, err := os.Stat(configFile); err != nil {
			return fmt.Errorf("validate config_file: %w", err)
		}
		configs = append(configs, configFile)
	}
	mounts, err := resolveVolumes(*volumes)
	if err != nil {
		return err
	}
	envValues, err := readEnvFiles(*envFiles)
	if err != nil {
		return err
	}
	// Match the former Compose environment override.
	envValues["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] = "1"
	var env []string
	for _, key := range slices.Sorted(maps.Keys(envValues)) {
		env = append(env, key+"="+envValues[key])
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	tmp, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("make temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	dc, err := dockerclient.New(dockerclient.FromEnv)
	if err != nil {
		return err
	}
	defer dc.Close()
	network, err := dc.NetworkCreate(ctx, clusterName+"-"+filepath.Base(tmp), dockerclient.NetworkCreateOptions{})
	if err != nil {
		return fmt.Errorf("create network: %w", err)
	}
	c := &cluster{client: dc, networkID: network.ID}
	// Clean up partial startup too, preserving the original error if cleanup fails.
	defer func() { retErr = errors.Join(retErr, c.cleanup()) }()
	group, groupCtx := errgroup.WithContext(ctx)
	// Cancel and join monitors before cleanup, even after partial startup.
	defer func() {
		cancel()
		if errors.Is(retErr, context.Canceled) {
			retErr = nil
		}
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			retErr = errors.Join(retErr, err)
		}
	}()

	zkPort := dockernetwork.MustParsePort("2181/tcp")
	if err := c.startContainer(groupCtx, group, "zookeeper", &dockercontainer.Config{
		Image:        zookeeperImage,
		ExposedPorts: dockernetwork.PortSet{zkPort: {}},
	}, &dockercontainer.HostConfig{
		PortBindings: dockernetwork.PortMap{zkPort: {{HostPort: "2181"}}},
	}); err != nil {
		return fmt.Errorf("start ZooKeeper: %w", err)
	}
	for r := 1; r <= *replicas; r++ {
		configPath := filepath.Join(tmp, fmt.Sprintf("clickhouse%d.xml", r))
		if err := os.WriteFile(configPath, []byte(getConfigXML(r)), 0644); err != nil {
			return err
		}
		binds := []string{configPath + ":/etc/clickhouse-server/config.d/config.xml:ro"}
		for i, configFile := range configs {
			binds = append(binds, fmt.Sprintf("%s:/etc/clickhouse-server/config.d/%d_%s:ro", configFile, i, filepath.Base(configFile)))
		}
		binds = append(binds, mounts...)
		tcpPort := dockernetwork.MustParsePort("9000/tcp")
		httpPort := dockernetwork.MustParsePort("8123/tcp")
		if err := c.startContainer(groupCtx, group, fmt.Sprintf("clickhouse%d", r), &dockercontainer.Config{
			Image:        clickhouseServerImage,
			Env:          env,
			ExposedPorts: dockernetwork.PortSet{tcpPort: {}, httpPort: {}},
		}, &dockercontainer.HostConfig{
			Binds:   binds,
			CapAdd:  []string{"NET_ADMIN", "SYS_NICE", "IPC_LOCK"},
			Ulimits: []*units.Ulimit{{Name: "nofile", Soft: 262144, Hard: 262144}},
			PortBindings: dockernetwork.PortMap{
				tcpPort:  {{HostPort: fmt.Sprint(clickhouseTCPPortNumber(r))}},
				httpPort: {{HostPort: fmt.Sprint(clickhouseHTTPPortNumber(r))}},
			},
		}); err != nil {
			return fmt.Errorf("start ClickHouse replica %d: %w", r, err)
		}
	}
	// The deferred join reports monitor failures; signals shut down normally.
	<-groupCtx.Done()
	return nil
}

// cluster tracks Docker resources so partial startup can be cleaned up too.
// Container creation and cleanup run sequentially; only logs and exit monitoring
// run in the background.
type cluster struct {
	client       *dockerclient.Client
	networkID    string
	containerIDs []string
}

func (c *cluster) startContainer(ctx context.Context, group *errgroup.Group, name string, config *dockercontainer.Config, hostConfig *dockercontainer.HostConfig) error {
	if _, err := c.client.ImageInspect(ctx, config.Image); errdefs.IsNotFound(err) {
		log.Infof("Pulling %s", config.Image)
		pull, err := c.client.ImagePull(ctx, config.Image, dockerclient.ImagePullOptions{})
		if err != nil {
			return err
		}
		defer pull.Close()
		if err := pull.Wait(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	config.User = fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	created, err := c.client.ContainerCreate(ctx, dockerclient.ContainerCreateOptions{
		Name:       name,
		Config:     config,
		HostConfig: hostConfig,
		NetworkingConfig: &dockernetwork.NetworkingConfig{
			EndpointsConfig: map[string]*dockernetwork.EndpointSettings{
				c.networkID: {Aliases: []string{name}},
			},
		},
	})
	if err != nil {
		return err
	}
	c.containerIDs = append(c.containerIDs, created.ID)
	if _, err := c.client.ContainerStart(ctx, created.ID, dockerclient.ContainerStartOptions{}); err != nil {
		return err
	}
	group.Go(func() error { return c.streamLogs(ctx, created.ID) })
	group.Go(func() error { return c.waitForExit(ctx, created.ID, name) })
	return nil
}

func (c *cluster) streamLogs(ctx context.Context, id string) error {
	// Allow final logs to drain after a container exits, but bound the wait for
	// siblings that are still running or a daemon that stops responding.
	logCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() {
		select {
		case <-time.After(5 * time.Second):
			cancel()
		case <-logCtx.Done():
		}
	})
	defer stop()
	logs, err := c.client.ContainerLogs(logCtx, id, dockerclient.ContainerLogsOptions{Follow: true, ShowStdout: true, ShowStderr: true})
	if err != nil {
		return err
	}
	defer logs.Close()
	_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, logs)
	return err
}

func (c *cluster) waitForExit(ctx context.Context, id, name string) error {
	wait := c.client.ContainerWait(ctx, id, dockerclient.ContainerWaitOptions{Condition: dockercontainer.WaitConditionNotRunning})
	select {
	case result := <-wait.Result:
		if result.Error != nil {
			return fmt.Errorf("%s: %s", name, result.Error.Message)
		}
		return fmt.Errorf("%s exited with status %d", name, result.StatusCode)
	case err := <-wait.Error:
		return err
	}
}

func (c *cluster) cleanup() (retErr error) {
	// Stop replicas before ZooKeeper, even after partial startup.
	for _, id := range slices.Backward(c.containerIDs) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, stopErr := c.client.ContainerStop(cleanupCtx, id, dockerclient.ContainerStopOptions{})
		cleanupCancel()
		cleanupCtx, cleanupCancel = context.WithTimeout(context.Background(), 30*time.Second)
		_, removeErr := c.client.ContainerRemove(cleanupCtx, id, dockerclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		cleanupCancel()
		retErr = errors.Join(retErr, stopErr, removeErr)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	_, err := c.client.NetworkRemove(cleanupCtx, c.networkID, dockerclient.NetworkRemoveOptions{})
	retErr = errors.Join(retErr, err)
	return retErr
}

func resolveVolumes(volumes []string) ([]string, error) {
	var resolved []string
	for _, volume := range volumes {
		source, target, hasSource := strings.Cut(volume, ":")
		// Preserve named and anonymous volumes; only resolve host paths.
		if hasSource && (strings.HasPrefix(source, ".") || strings.Contains(source, "/")) {
			abs, err := filepath.Abs(source)
			if err != nil {
				return nil, fmt.Errorf("resolve volume %q: %w", volume, err)
			}
			volume = abs + ":" + target
		}
		resolved = append(resolved, volume)
	}
	return resolved, nil
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
	<logger>
		<level>%s</level>
		<console>1</console>
		<log>/var/log/clickhouse-server/clickhouse-server.log</log>
		<errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
		<size>1000M</size>
		<count>10</count>
	</logger>

	<remote_servers>
		<%s>
			<shard>
`, *serverLogLevel, clusterName)
	for r := 1; r <= *replicas; r++ {
		fmt.Fprintf(&xml, `
				<replica>
					<host>clickhouse%d</host>
					<port>9000</port>
				</replica>
`, r)
	}
	fmt.Fprintf(&xml, `
			</shard>
		</%s>
	</remote_servers>

	<zookeeper>
		<node>
			<host>zookeeper</host>
			<port>2181</port>
		</node>
	</zookeeper>

	<macros>
		<installation>local</installation>
		<cluster>%s</cluster>
		<shard>shard1</shard>
		<replica>replica%d</replica>
	</macros>
</clickhouse>
`, clusterName, clusterName, r)
	return xml.String()
}

func clickhouseHTTPPortNumber(r int) int {
	return clickhouseBaseHTTPPortNumber + r
}

func clickhouseTCPPortNumber(r int) int {
	return clickhouseBaseTCPPortNumber + r
}
