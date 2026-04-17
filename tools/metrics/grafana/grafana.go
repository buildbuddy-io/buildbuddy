// This tool is used to view and edit Grafana dashboards locally.
// See tools/metrics/README.md for more info.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
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
	dockerComposeDir  = "tools/metrics"
	dashboardsDir     = "tools/metrics/grafana/dashboards"
	generatedRoot     = "tools/metrics/grafana/generated"
	generatedBazelDir = "bazel-bin/tools/metrics/grafana/generated"
	generatedBuildAll = "//tools/metrics/grafana/generated/..."
	normalizeScript   = "tools/metrics/process_dashboard.py"
	dashboardsBzl     = "tools/metrics/grafana/dashboards.bzl"
	watcherDebounce   = 300 * time.Millisecond

	grafanaAPIURL = "http://admin:admin@localhost:4500/api"
	grafanaUIURL  = "http://localhost:4500/d/1rsE5yoGz?orgId=1&refresh=5s"

	victoriaMetricsLocalURL   = "http://localhost:8428"
	victoriaMetricsClusterURL = "http://localhost:8481/select/0/prometheus"
)

// generatedSet is the set of dashboard JSON filenames that are generated from
// Go source under tools/metrics/grafana/generated/... These are treated as
// read-only from the UI's perspective: Grafana mounts them for preview, but
// changes made via the UI are not written back. The code under
// tools/metrics/grafana/generated/** is the source of truth.
var (
	generatedSet   = map[string]bool{}
	generatedSetMu sync.RWMutex
)

func setGeneratedDashboards(names []string) {
	generatedSetMu.Lock()
	defer generatedSetMu.Unlock()
	generatedSet = make(map[string]bool, len(names))
	for _, n := range names {
		generatedSet[n] = true
	}
}

func isGeneratedDashboard(fileName string) bool {
	generatedSetMu.RLock()
	defer generatedSetMu.RUnlock()
	return generatedSet[fileName]
}

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

	// Build and stage generated dashboards before starting Grafana.
	if err := runBazelBuild(ctx); err != nil {
		return fmt.Errorf("initial bazel build of generated dashboards: %w", err)
	}
	tempDashboards, err := os.MkdirTemp("", "grafana-dashboards-")
	if err != nil {
		return fmt.Errorf("create temp dashboards dir: %w", err)
	}
	defer os.RemoveAll(tempDashboards)
	// MkdirTemp creates the dir with mode 0700. Grafana runs as a different
	// UID inside the docker container, so it needs world-readable + executable
	// permissions to list and read the dashboard JSONs.
	if err := os.Chmod(tempDashboards, 0755); err != nil {
		return fmt.Errorf("chmod temp dashboards dir: %w", err)
	}
	if err := populateDashboardsDir(tempDashboards, workspaceRoot); err != nil {
		return fmt.Errorf("populate dashboards dir: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer cancel() // stop everything else even when docker exits cleanly
		// Start docker-compose
		os.Setenv("DASHBOARDS_DIR", tempDashboards)
		os.Setenv("GF_DATASOURCE_URL", strings.Replace(datasourceURL(), "localhost", "host.docker.internal", 1))

		commandName := "docker"
		args := []string{"compose"}

		args = append(args, "--file", "docker-compose.grafana.yml")
		if !*kube {
			args = append(args, "--file", "docker-compose.redis-exporter.yml")
			args = append(args, "--file", "docker-compose.victoria-metrics.yml")
			args = append(args, "--file", "docker-compose.node-exporter.yml")
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
		// Watch the generated dashboards directory for source changes and
		// rebuild + restage the JSON outputs live. Failures are logged.
		return watchGeneratedDashboards(ctx, workspaceRoot, tempDashboards)
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
				// Generated dashboards live under tools/metrics/grafana/generated/...
				// and are not tracked in dashboards.bzl.
				if isGeneratedDashboard(fileName) {
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
		if after, ok := strings.CutPrefix(tag, "file:"); ok {
			fileName = after
			break
		}
	}
	// Generated dashboards are one-way (code -> JSON). Don't write UI edits
	// back to the source tree; the code is the source of truth.
	if isGeneratedDashboard(fileName) {
		return fileName, nil
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

// runBazelBuild invokes `bazel build` for every generator under
// tools/metrics/grafana/generated/... The outputs land in bazel-bin at stable
// paths that populateDashboardsDir / stageGeneratedDashboards then copy into
// the temp dashboards dir. stderr is forwarded so the user sees compile errors
// in the terminal.
func runBazelBuild(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "bazel", "build", generatedBuildAll)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// reloadGrafanaProvisioning triggers Grafana to rescan its provisioned
// dashboards directory. The provisioning config sets updateIntervalSeconds to
// effectively infinity (to prevent polling from fighting with the UI-edit
// workflow), so changes to files on disk are invisible to Grafana until this
// endpoint is hit. It's idempotent and only reloads dashboards whose file
// mtimes changed since the last load, so hand-edited dashboards with
// unexported UI changes are not clobbered.
func reloadGrafanaProvisioning(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "POST", grafanaAPIURL+"/admin/provisioning/dashboards/reload", nil)
	if err != nil {
		return err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()
	if rsp.StatusCode >= 300 {
		body, _ := io.ReadAll(rsp.Body)
		return fmt.Errorf("HTTP %d: %s", rsp.StatusCode, string(body))
	}
	return nil
}

// saveDashboardViaAPI reads the dashboard JSON at jsonPath and POSTs it to
// Grafana's /api/dashboards/db endpoint. This is the exact path the UI takes
// when you click "Save", so Grafana publishes its internal "dashboard saved"
// notification to connected viewers, which triggers a soft reload.
//
// The "id" field is stripped from the body so Grafana looks the dashboard up
// by uid. "overwrite: true" ensures existing dashboards are updated in place.
func saveDashboardViaAPI(ctx context.Context, jsonPath string) error {
	b, err := os.ReadFile(jsonPath)
	if err != nil {
		return err
	}
	var dashboard map[string]any
	if err := json.Unmarshal(b, &dashboard); err != nil {
		return fmt.Errorf("parse %s: %w", jsonPath, err)
	}
	// Strip id so Grafana looks up by uid. Version is also stripped so we
	// don't get "someone else updated this dashboard" version conflicts.
	delete(dashboard, "id")
	delete(dashboard, "version")

	body, err := json.Marshal(map[string]any{
		"dashboard": dashboard,
		"overwrite": true,
		"message":   "Auto-refresh from grafana.go watcher",
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", grafanaAPIURL+"/dashboards/db", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()
	if rsp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(rsp.Body)
		return fmt.Errorf("HTTP %d: %s", rsp.StatusCode, string(respBody))
	}
	return nil
}

// populateDashboardsDir seeds the temp dashboards directory with both
// hand-edited dashboards (from the source tree) and the generated dashboards
// (from bazel-bin). It also updates the global generatedSet so the export
// loop knows which names are generated.
func populateDashboardsDir(tempDir, workspaceRoot string) error {
	// Hand-edited dashboards.
	handEdited, err := filepath.Glob(filepath.Join(workspaceRoot, dashboardsDir, "*.json"))
	if err != nil {
		return fmt.Errorf("glob hand-edited dashboards: %w", err)
	}
	for _, src := range handEdited {
		dst := filepath.Join(tempDir, filepath.Base(src))
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy %s: %w", src, err)
		}
	}
	// Generated dashboards.
	_, err = stageGeneratedDashboards(workspaceRoot, tempDir)
	return err
}

// stageGeneratedDashboards walks bazel-bin/tools/metrics/grafana/generated/**
// for JSON outputs and copies each into the temp dashboards directory. It
// refreshes the generatedSet so the export loop can filter accordingly, and
// returns the list of staged file paths (inside tempDir) so callers can
// push them to Grafana via the dashboard API.
func stageGeneratedDashboards(workspaceRoot, tempDir string) ([]string, error) {
	root := filepath.Join(workspaceRoot, generatedBazelDir)
	var names []string
	var stagedPaths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		name := filepath.Base(path)
		dst := filepath.Join(tempDir, name)
		if err := copyFile(path, dst); err != nil {
			return fmt.Errorf("copy %s: %w", path, err)
		}
		names = append(names, name)
		stagedPaths = append(stagedPaths, dst)
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("walk generated dashboards: %w", err)
	}
	setGeneratedDashboards(names)
	return stagedPaths, nil
}

// copyFile copies src to dst and sets mode 0644 so the grafana container
// (running as a different uid) can read it. dst is written atomically via
// rename so Grafana's file provisioning watcher never sees a half-written
// file.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	// os.CreateTemp creates the file with mode 0600. Relax to 0644 so the
	// grafana container (running as a different UID) can read it.
	if err := os.Chmod(tmpPath, 0644); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, dst)
}

// watchGeneratedDashboards watches every .go and BUILD file under
// tools/metrics/grafana/generated/** and rebuilds + restages the generated
// dashboards whenever the user saves an edit. Build failures are logged but
// do not tear down the session — the last good JSON stays in place.
func watchGeneratedDashboards(ctx context.Context, workspaceRoot, tempDir string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create fsnotify watcher: %w", err)
	}
	defer watcher.Close()

	generatedPath := filepath.Join(workspaceRoot, generatedRoot)
	err = filepath.WalkDir(generatedPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		return watcher.Add(path)
	})
	if err != nil {
		return fmt.Errorf("add watches under %s: %w", generatedPath, err)
	}
	log.Printf("Watching %s for dashboard source changes", generatedPath)

	var timer *time.Timer
	trigger := make(chan struct{}, 1)
	armTimer := func() {
		if timer == nil {
			timer = time.AfterFunc(watcherDebounce, func() {
				select {
				case trigger <- struct{}{}:
				default:
				}
			})
			return
		}
		timer.Reset(watcherDebounce)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if !isInterestingEvent(ev) {
				continue
			}
			// If a new subdirectory appeared, start watching it too.
			if ev.Op&fsnotify.Create != 0 {
				if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
					_ = watcher.Add(ev.Name)
				}
			}
			armTimer()
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("fsnotify error: %s", err)
		case <-trigger:
			log.Printf("Rebuilding generated dashboards...")
			if err := runBazelBuild(ctx); err != nil {
				log.Printf("Bazel build failed; keeping previous dashboard JSONs in place: %s", err)
				continue
			}
			stagedPaths, err := stageGeneratedDashboards(workspaceRoot, tempDir)
			if err != nil {
				log.Printf("Failed to restage generated dashboards: %s", err)
				continue
			}
			if err := reloadGrafanaProvisioning(ctx); err != nil {
				log.Printf("Failed to reload Grafana dashboard provisioning: %s", err)
				continue
			}
			for _, p := range stagedPaths {
				if err := saveDashboardViaAPI(ctx, p); err != nil {
					log.Printf("Failed to save dashboard %s via API: %s", filepath.Base(p), err)
				}
			}
			log.Printf("Generated dashboards refreshed.")
		}
	}
}

// isInterestingEvent filters out fsnotify events we don't care about:
// chmod-only events, temporary editor files, and non-source files like JSON
// outputs that Bazel writes into the subtree.
func isInterestingEvent(ev fsnotify.Event) bool {
	if ev.Op == fsnotify.Chmod {
		return false
	}
	name := filepath.Base(ev.Name)
	return filepath.Ext(name) == ".go" || name == "BUILD"
}
