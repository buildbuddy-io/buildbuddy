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
	vm   = flag.Bool("vm", false, "Use VictoriaMetrics instead of Prometheus")

	// Note: these flags only take effect when setting -kube=true:
	namespace  = flag.String("namespace", "monitor-dev", "k8s namespace")
	deployment = flag.String("deployment", "prometheus-global-server", "Prometheus server resource to port-forward to.")
)

const (
	dockerComposeDir = "tools/metrics"
	dashboardsDir    = "tools/metrics/grafana/dashboards"
	vmDashboardsDir  = "bazel-bin/tools/metrics/grafana/dashboards-vm"
	normalizeScript  = "tools/metrics/process_dashboard.py"
	dashboardsBzl    = "tools/metrics/grafana/dashboards.bzl"

	grafanaAPIURL = "http://admin:admin@localhost:4500/api"
	grafanaUIURL  = "http://localhost:4500/d/1rsE5yoGz?orgId=1&refresh=5s"
	prometheusURL = "http://localhost:9100/metrics"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf(err.Error())
	}
}

func run() error {
	flag.Parse()
	// cd to the repo root
	workspaceRoot := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
	if err := os.Chdir(workspaceRoot); err != nil {
		return err
	}
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// Start docker-compose
		args := []string{"--file", "docker-compose.grafana.yml"}
		if !*kube {
			args = append(args, "--file", "docker-compose.redis-exporter.yml")
			if *vm {
				os.Setenv("DASHBOARDS_DIR", filepath.Join(workspaceRoot, vmDashboardsDir))
				args = append(args, "--file", "docker-compose.victoria-metrics.yml")
			} else {
				os.Setenv("DASHBOARDS_DIR", filepath.Join(workspaceRoot, dashboardsDir))
				args = append(args, "--file", "docker-compose.prometheus.yml")
			}
		}
		args = append(args, "up")
		// Note: CommandContext kills with SIGKILL - we don't want that since it
		// doesn't give docker a chance to clean up.
		cmd := exec.Command("docker-compose", args...)
		cmd.Dir = dockerComposeDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	})
	eg.Go(func() error {
		if !*kube {
			return nil
		}
		// Start kubectl port-forward
		cmd := exec.CommandContext(
			ctx, "kubectl", "--namespace", *namespace,
			"port-forward", "deployment/"+*deployment,
			"--address=0.0.0.0", "9100:9090")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
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
			for _, d := range dashboards {
				fileName, err := exportNormalizedDashboard(d)
				if err != nil {
					log.Printf("Failed to export dashboard: %s", err)
				}
				fileNames = append(fileNames, fileName)
			}
			slices.Sort(fileNames)
			if err := writeDashboardsBzl(fileNames); err != nil {
				log.Printf("Failed to write %s: %s", dashboardsBzl, err)
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
			if _, err := httpGetBody(grafanaUIURL); err != nil {
				continue
			}
			if _, err := httpGetBody(prometheusURL); err != nil {
				continue
			}
			_ = openInBrowser(grafanaUIURL)
			return nil
		}
	})
	return eg.Wait()
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
	// If the dashboard already has a uid then preserve it to avoid breaking
	// go links.
	uid, err := existingDashboardUID(outPath)
	if err != nil {
		return "", err
	}
	processedJSON, err := processDashboard(dashboardJSON, fileName, uid)
	if err != nil {
		return "", err
	}
	if err := ensureFileContents(outPath, processedJSON); err != nil {
		return "", err
	}
	return fileName, nil
}

func existingDashboardUID(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	var d Dashboard
	if err := json.Unmarshal(b, &d); err != nil {
		return "", err
	}
	if uid, ok := d["uid"]; ok {
		return uid.(string), nil
	}
	return "", nil
}

func processDashboard(dashboardJSON []byte, fileName, uid string) ([]byte, error) {
	cmd := exec.Command(normalizeScript, "--name", fileName, "--uid", uid, "--data_source_uid=prom")
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
