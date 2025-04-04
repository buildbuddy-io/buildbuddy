// This tool is used to view and edit Grafana dashboards locally.
// See tools/metrics/README.md for more info.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	kube = flag.Bool("kube", false, "Use kubectl port-forward to point Grafana at real data.")

	// Note: these flags only take effect when setting -kube=true:
	namespace = flag.String("namespace", "monitor-dev", "k8s namespace")
	service   = flag.String("service", "victoria-metrics-cluster-global-vmselect", "k8s VictoriaMetrics service name")

	clickhouse = flag.String("clickhouse", "", "Optional clickhouse environment to connect to. If set, must be one of (local, dev, prod)")
)

const (
	dockerComposeDir = "tools/metrics"
	dashboardsDir    = "tools/metrics/grafana/dashboards"
	normalizeScript  = "tools/metrics/process_dashboard.py"
	dashboardsBzl    = "tools/metrics/grafana/dashboards.bzl"

	grafanaAPIURL = "http://admin:admin@localhost:4500/api"
	grafanaUIURL  = "http://localhost:4500/d/1rsE5yoGz?orgId=1&refresh=5s"

	victoriaMetricsLocalURL   = "http://localhost:8428"
	victoriaMetricsClusterURL = "http://localhost:8481/select/0/prometheus"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()
	// cd to the repo root
	workspaceRoot := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
	if err := os.Chdir(workspaceRoot); err != nil {
		return err
	}
	switch *clickhouse {
	case "":
	case "local":
		os.Setenv("CLICKHOUSE_PORT", "9000") // Assume the default from tools/clickhouse
	case "dev", "prod":
		os.Setenv("CLICKHOUSE_PORT", "9001")
		os.Setenv("CLICKHOUSE_USERNAME", "buildbuddy_"+*clickhouse+"_readonly")
		pw, err := getSecret("BB_" + strings.ToUpper(*clickhouse) + "_CLICKHOUSE_READONLY_PASSWORD")
		if err != nil {
			return err
		}
		os.Setenv("CLICKHOUSE_PASSWORD", string(pw))
	default:
		return fmt.Errorf("Invalid value for --clickhouse: %v", *clickhouse)
	}
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer cancel() // stop everything else even when docker exits cleanly
		// Start docker-compose
		os.Setenv("DASHBOARDS_DIR", filepath.Join(workspaceRoot, dashboardsDir))
		os.Setenv("GF_DATASOURCE_URL", strings.Replace(datasourceURL(), "localhost", "host.docker.internal", 1))

		commandName := "docker-compose"
		var args []string
		if _, err := exec.LookPath("docker-compose"); err != nil {
			commandName = "docker"
			args = append(args, "compose")
		}

		args = append(args, "--file", "docker-compose.grafana.yml")
		if !*kube {
			args = append(args, "--file", "docker-compose.redis-exporter.yml")
			args = append(args, "--file", "docker-compose.victoria-metrics.yml")
		}
		args = append(args, "up")
		// Note: CommandContext kills with SIGKILL - we don't want that since it
		// doesn't give docker a chance to clean up.
		cmd := exec.Command(commandName, args...)
		cmd.Dir = dockerComposeDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	})
	eg.Go(func() error {
		if !*kube {
			return nil
		}
		// Start kubectl port-forward for victoria metrics
		cmd := exec.CommandContext(
			ctx, "kubectl", "--namespace", *namespace,
			"port-forward", "service/"+*service,
			"--address=0.0.0.0", "8481:8481")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			log.Printf("*********** Failed to port forward victoria metrics connection: %s", err)
		}
		return err
	})
	eg.Go(func() error {
		var context string
		if *clickhouse == "dev" {
			context = "gke_flame-build_us-west1_dev-nv8eh"
		} else if *clickhouse == "prod" {
			context = "gke_flame-build_us-west1_prod-hs6in"
		} else {
			return nil
		}
		namespace := "clickhouse-operator-" + *clickhouse
		service := "chi-repl-" + *clickhouse + "-replicated-0-0-0"
		// Start kubectl port-forward for clickhouse
		cmd := exec.CommandContext(
			ctx, "kubectl", "--namespace", namespace, "port-forward", service,
			"--context="+context, "--address=0.0.0.0", "9001:9000")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			log.Printf("*********** Failed to port forward clickhouse connection: %s", err)
		}
		return err
	})
	eg.Go(func() error {
		// Periodically export dashboards
		for {
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return nil
			}
			b, err := httpGetBody(grafanaAPIURL + "/search")
			if err != nil {
				log.Printf("Failed to list grafana dashboards: %s", err)
				continue
			}
			var dashboards []DashboardMetadata
			if err := json.Unmarshal(b, &dashboards); err != nil {
				log.Printf("Invalid JSON in grafana response: %s", err)
				continue
			}
			var fileNames []string
			var exportErr error
			for _, d := range dashboards {
				fileName, err := exportNormalizedDashboard(d)
				if err != nil {
					log.Printf("Failed to export dashboard: %s", err)
					exportErr = err
					continue
				}
				fileNames = append(fileNames, fileName)
			}
			if exportErr == nil {
				slices.Sort(fileNames)
				if err := writeDashboardsBzl(fileNames); err != nil {
					log.Printf("Failed to write %s: %s", dashboardsBzl, err)
				}
			}
		}
	})
	eg.Go(func() error {
		// Open grafana when both prometheus and grafana are ready.
		for {
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return nil
			}
			if _, err := http.Head(grafanaUIURL); err != nil {
				continue
			}
			if _, err := http.Head(datasourceURL()); err != nil {
				continue
			}
			_ = openInBrowser(grafanaUIURL)
			return nil
		}
	})
	return eg.Wait()
}

func getSecret(secret string) ([]byte, error) {
	return exec.Command("gcloud", "secrets", "versions", "access", "latest", "--secret="+secret).CombinedOutput()
}

func datasourceURL() string {
	if *kube {
		return victoriaMetricsClusterURL
	}
	return victoriaMetricsLocalURL
}

type DashboardMetadata map[string]any

type Dashboard map[string]any

type DashboardResponse struct {
	Dashboard Dashboard `json:"dashboard"`
}

func exportNormalizedDashboard(md DashboardMetadata) (fileName string, _ error) {
	b, err := httpGetBody(grafanaAPIURL + "/dashboards/uid/" + md["uid"].(string))
	if err != nil {
		return "", err
	}
	// Unmarshal the response to get the slug field (this is how we determine
	// the JSON file name)
	rsp := &DashboardResponse{}
	if err := json.Unmarshal(b, rsp); err != nil {
		return "", err
	}
	d := rsp.Dashboard
	fileName = path.Base(md["url"].(string)) + ".json"
	for _, t := range d["tags"].([]any) {
		// Use the existing "file:" tag as the file name if it exists, this way
		// the file name does not have to match the dashboard title, and so we
		// can rename dashboards without changing the file name.
		tag := t.(string)
		if strings.HasPrefix(tag, "file:") {
			fileName = strings.TrimPrefix(tag, "file:")
			break
		}
	}
	outPath := filepath.Join(dashboardsDir, fileName)
	dashboardJSON, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	processedJSON, err := processDashboard(dashboardJSON, fileName)
	if err != nil {
		return "", err
	}
	if err := ensureFileContents(outPath, processedJSON); err != nil {
		return "", err
	}
	return fileName, nil
}

func processDashboard(dashboardJSON []byte, fileName string) ([]byte, error) {
	cmd := exec.Command(normalizeScript, "--name", fileName)
	cmd.Stdin = bytes.NewReader(dashboardJSON)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeDashboardsBzl(fileNames []string) error {
	var lines []string
	lines = append(lines, "# This file is automatically updated by grafana.go - DO NOT EDIT")
	lines = append(lines, "DASHBOARD_NAMES = [")
	for _, name := range fileNames {
		lines = append(lines, fmt.Sprintf(`    %q,`, strings.TrimSuffix(name, ".json")))
	}
	lines = append(lines, "]")
	return ensureFileContents(dashboardsBzl, []byte(strings.Join(lines, "\n")+"\n"))
}

// Writes the given file contents, avoiding a file modification if the file
// already contains the given contents.
func ensureFileContents(path string, contents []byte) error {
	oldContent, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if oldContent == nil || !bytes.Equal(oldContent, contents) {
		return os.WriteFile(path, contents, 0644)
	}
	return nil
}

func httpGetBody(url string) ([]byte, error) {
	rsp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	b, err := io.ReadAll(rsp.Body)
	if rsp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", rsp.StatusCode, string(b))
	}
	return b, err
}

func openInBrowser(url string) error {
	for _, name := range []string{"open", "xdg-open"} {
		open, err := exec.LookPath(name)
		if err == nil {
			return exec.Command(open, url).Run()
		}
	}
	return fmt.Errorf("failed to locate 'open' binary")
}
