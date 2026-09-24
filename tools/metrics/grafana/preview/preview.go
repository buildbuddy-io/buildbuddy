// preview runs a throwaway copy of our deployed Grafana in Kubernetes, loaded
// with the dashboards from this checkout, so local dashboard changes can be
// shared with a link before they are merged and deployed.
//
//	bazel run //tools/metrics/grafana/preview -- --env=dev up      # (re)deploy, print link
//	bazel run //tools/metrics/grafana/preview -- --env=dev status  # what's running, time left
//	bazel run //tools/metrics/grafana/preview -- --env=dev down    # tear down
//
// The preview mirrors enterprise/deployment/templates/grafana.yaml in the
// internal repo (same image, env, provisioning and datasources), with one pod
// instead of two and the dashboards delivered through ConfigMaps instead of a
// pushed image. It is reachable through the bbaccess tunnel by its service
// name.
//
// Grafana runs as a Job rather than a Deployment so Kubernetes shuts it down
// by itself: the Job's activeDeadlineSeconds kills the pod after maxLifetime,
// ttlSecondsAfterFinished then deletes the Job, and everything else the tool
// created (service and configmaps) is owned by the Job and garbage collected
// with it.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	envFlag    = flag.String("env", "", "Which environment to run the preview in: dev or prod.")
	suffixFlag = flag.String("suffix", "", "Suffix for the names of everything the preview creates (grafana-preview-<suffix>), "+
		"so one person can run several previews. Lowercase letters, digits and dashes. Defaults to your username.")
)

const (
	// maxLifetime is how long a preview lives before Kubernetes shuts it down
	// on its own, for the case where nobody runs "down".
	maxLifetime = 3 * 24 * time.Hour

	// Keep these in sync with enterprise/deployment/templates/grafana.yaml in
	// the internal repo, which is what the preview is meant to look like.
	grafanaImage     = "grafana/grafana:11.6.2"
	clickhousePlugin = "grafana-clickhouse-datasource@4.11.2"

	// clickhousePasswordsSecret is a Secret that already exists in each
	// monitor namespace: the internal repo's clickhouse-otel.<env>.yaml creates
	// it, and jaeger and otel-collector read from it. The preview reads the
	// read-only password from it too, so no password passes through the
	// machine running this tool.
	clickhousePasswordsSecret = "clickhouse-passwords"

	dashboardsDir     = "tools/metrics/grafana/dashboards"
	generatedTarget   = "//tools/metrics/grafana/generated:all_dashboards"
	provisioningDir   = "tools/metrics/grafana/provisioning/deploy"
	grafanaDashboards = "/var/lib/grafana/dashboards"

	// Kubernetes caps a ConfigMap at 1MiB. Dashboards are packed into as few
	// ConfigMaps as fit under this, leaving headroom for object metadata.
	maxConfigMapBytes = 700 * 1024

	// previewLabel marks everything the tool creates, so a label selector can
	// find (and delete) all of it. previewKinds is everything it may have
	// created; secrets are included because earlier versions made one.
	previewLabel = "buildbuddy.io/grafana-preview"
	previewKinds = "job,service,configmap,secret"

	fieldManager = "grafana-preview"
	readyTimeout = 5 * time.Minute
	readyPoll    = 2 * time.Second
)

type environment struct {
	kubeContext string
	namespace   string
	// tunnelSuffix is the DNS suffix under which the bbaccess tunnel resolves
	// this cluster's services (see `bbaccess tunnel status`). It stands in
	// for svc.cluster.local.
	tunnelSuffix string
	// clickhousePasswordKey is the key in clickhousePasswordsSecret holding
	// the password of the read-only user the datasources connect as.
	clickhousePasswordKey string
}

var environments = map[string]environment{
	"dev": {
		kubeContext:           "gke_flame-build_us-west1_dev-nv8eh",
		namespace:             "monitor-dev",
		tunnelSuffix:          "svc.k8s.us-west1.gcp.dev.bb.internal",
		clickhousePasswordKey: "buildbuddy_dev_readonly",
	},
	"prod": {
		kubeContext: "gke_flame-build_us-west1_prod-hs6in",
		namespace:   "monitor-prod",
		// Follows the dev zone's naming. There is no prod relay gateway yet,
		// so until one exists this name only resolves via port-forward.
		tunnelSuffix:          "svc.k8s.us-west1.gcp.prod.bb.internal",
		clickhousePasswordKey: "buildbuddy_prod_readonly",
	},
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: bazel run //tools/metrics/grafana/preview -- --env=<dev|prod> [--suffix=<name>] <up|status|down>

  up      Build the dashboards in this checkout and (re)deploy a preview
          Grafana serving them. Prints the link when it is ready.
  status  Show what is running and when it will shut itself down.
  down    Delete the preview.

Flags:
`)
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	env, ok := environments[*envFlag]
	if !ok {
		usage()
		return fmt.Errorf("--env must be dev or prod, got %q", *envFlag)
	}
	if flag.NArg() != 1 {
		usage()
		return errors.New("expected exactly one command: up, status or down")
	}
	// bazel run starts us in the runfiles tree; work from the checkout.
	if root := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); root != "" {
		if err := os.Chdir(root); err != nil {
			return err
		}
	}
	suffix, explicit, err := resolveSuffix(*suffixFlag)
	if err != nil {
		return err
	}
	p := newPreview(*envFlag, env, suffix, explicit)
	switch flag.Arg(0) {
	case "up":
		return p.up(ctx)
	case "status":
		return p.status(ctx)
	case "down":
		return p.down(ctx)
	default:
		usage()
		return fmt.Errorf("unknown command %q", flag.Arg(0))
	}
}

type preview struct {
	envName string
	env     environment
	kube    kubectl
	// suffix distinguishes this preview from others in the namespace. It is
	// the value of previewLabel on everything the preview creates.
	suffix string
	// explicitSuffix is whether suffix came from --suffix rather than the
	// username, and so must be repeated in printed commands.
	explicitSuffix bool
	// name is the Job, Service and pod-label name.
	name string
}

func newPreview(envName string, env environment, suffix string, explicitSuffix bool) *preview {
	return &preview{
		envName:        envName,
		env:            env,
		kube:           kubectl{context: env.kubeContext, namespace: env.namespace},
		suffix:         suffix,
		explicitSuffix: explicitSuffix,
		name:           "grafana-preview-" + suffix,
	}
}

// Suffixes end up in DNS-1035 names (with the grafana-preview- prefix and a
// -dashboards-N tail) and in label values, so they are kept short and to the
// characters both allow.
const maxSuffixLen = 24

var (
	validSuffix   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	nonLabelChars = regexp.MustCompile(`[^a-z0-9]+`)
)

// resolveSuffix returns the suffix from the flag if given, otherwise the
// local username, and whether it was given explicitly.
func resolveSuffix(flagValue string) (suffix string, explicit bool, err error) {
	if flagValue != "" {
		if len(flagValue) > maxSuffixLen || !validSuffix.MatchString(flagValue) {
			return "", false, fmt.Errorf("--suffix %q must be 1-%d lowercase letters, digits or dashes, and start and end with a letter or digit", flagValue, maxSuffixLen)
		}
		return flagValue, true, nil
	}
	suffix, err = currentUser()
	return suffix, false, err
}

// currentUser returns the local username reduced to a valid suffix.
func currentUser() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("determine current user: %w", err)
	}
	name := strings.Trim(nonLabelChars.ReplaceAllString(strings.ToLower(u.Username), "-"), "-")
	if len(name) > maxSuffixLen {
		name = strings.TrimRight(name[:maxSuffixLen], "-")
	}
	if name == "" {
		return "", fmt.Errorf("username %q has no usable characters; pass --suffix", u.Username)
	}
	return name, nil
}

func (p *preview) selector() string {
	return previewLabel + "=" + p.suffix
}

func (p *preview) labels() map[string]string {
	return map[string]string{
		"app":        p.name,
		previewLabel: p.suffix,
	}
}

// bazelCommand is how to run this tool again for the same preview.
func (p *preview) bazelCommand(verb string) string {
	cmd := "bazel run //tools/metrics/grafana/preview -- --env=" + p.envName
	if p.explicitSuffix {
		cmd += " --suffix=" + p.suffix
	}
	return cmd + " " + verb
}

// kubectlDeleteCommand deletes the preview without bazel; it is what "down"
// runs.
func (p *preview) kubectlDeleteCommand() string {
	return fmt.Sprintf("kubectl --context %s -n %s delete %s -l %s", p.env.kubeContext, p.env.namespace, previewKinds, p.selector())
}

func (p *preview) url() string {
	return fmt.Sprintf("http://%s.%s.%s/", p.name, p.env.namespace, p.env.tunnelSuffix)
}

func (p *preview) up(ctx context.Context) error {
	// Gather everything before touching the cluster, so a build failure
	// leaves any existing preview running.
	log.Printf("Building generated dashboards...")
	dashboards, err := collectDashboards(ctx)
	if err != nil {
		return err
	}
	provisioning, err := readProvisioning(p.envName)
	if err != nil {
		return err
	}
	configMaps := p.dashboardConfigMaps(dashboards)
	log.Printf("%d dashboards in %d configmaps", len(dashboards), len(configMaps))

	log.Printf("Removing any previous preview in %s/%s...", p.env.kubeContext, p.env.namespace)
	if err := p.deleteAll(ctx); err != nil {
		return err
	}

	// The Job is created suspended so it does not start a pod before the
	// objects the pod mounts exist. Those objects get the Job as their owner
	// so they disappear with it, which needs the Job's UID first.
	log.Printf("Creating %s...", p.name)
	if err := p.kube.apply(ctx, p.job(configMaps)); err != nil {
		return err
	}
	uid, err := p.kube.output(ctx, nil, "get", "job", p.name, "-o", "jsonpath={.metadata.uid}")
	if err != nil {
		return err
	}
	owner := metav1.OwnerReference{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Name:       p.name,
		UID:        types.UID(strings.TrimSpace(string(uid))),
	}
	var dependents []any
	for _, cm := range configMaps {
		cm.OwnerReferences = []metav1.OwnerReference{owner}
		dependents = append(dependents, cm)
	}
	provisioningCM := p.provisioningConfigMap(provisioning)
	provisioningCM.OwnerReferences = []metav1.OwnerReference{owner}
	service := p.service()
	service.OwnerReferences = []metav1.OwnerReference{owner}
	dependents = append(dependents, provisioningCM, service)
	if err := p.kube.apply(ctx, dependents...); err != nil {
		return err
	}
	if _, err := p.kube.output(ctx, nil, "patch", "job", p.name, "--type=merge", "-p", `{"spec":{"suspend":false}}`); err != nil {
		return err
	}

	log.Printf("Waiting for the pod to become ready...")
	if err := p.waitForReady(ctx); err != nil {
		return fmt.Errorf("%w\nInspect with: %s", err, p.bazelCommand("status"))
	}
	p.printLink(time.Now().Add(maxLifetime))
	return nil
}

func (p *preview) status(ctx context.Context) error {
	out, err := p.kube.output(ctx, nil, "get", "job", "-l", p.selector(), "-o", "json")
	if err != nil {
		return err
	}
	var jobs batchv1.JobList
	if err := json.Unmarshal(out, &jobs); err != nil {
		return fmt.Errorf("parse job list: %w", err)
	}
	if len(jobs.Items) == 0 {
		fmt.Printf("No preview named %s in %s/%s.\n", p.name, p.env.kubeContext, p.env.namespace)
		return nil
	}
	if err := p.kube.passthrough(ctx, "get", "job,pod,service,configmap", "-l", p.selector()); err != nil {
		return err
	}
	fmt.Println()
	job := jobs.Items[0]
	if job.Status.StartTime == nil || job.Spec.ActiveDeadlineSeconds == nil {
		fmt.Println("Not started yet.")
	} else {
		p.printLink(job.Status.StartTime.Add(time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second))
	}
	return nil
}

func (p *preview) down(ctx context.Context) error {
	log.Printf("Deleting preview %s in %s/%s...", p.name, p.env.kubeContext, p.env.namespace)
	return p.deleteAll(ctx)
}

// deleteAll removes everything labeled as this preview and waits until it is
// gone, pods included, so a following "up" starts from a clean slate.
func (p *preview) deleteAll(ctx context.Context) error {
	return p.kube.passthrough(ctx, "delete", previewKinds,
		"-l", p.selector(), "--ignore-not-found", "--cascade=foreground", "--wait=true")
}

func (p *preview) printLink(expiry time.Time) {
	fmt.Printf("\nGrafana preview (%s):\n\n%s\n\n", p.envName, p.url())
	fmt.Printf("Anyone with the bbaccess tunnel running (bbaccess --tunnel) can open it.\n")
	fmt.Printf("Without the tunnel:\nkubectl --context %s -n %s port-forward service/%s 4500:80\n",
		p.env.kubeContext, p.env.namespace, p.name)
	fmt.Printf("\nShuts itself down at %s (in %s). Tear it down sooner with:\n",
		expiry.Local().Format("Mon Jan 2 15:04 MST"), time.Until(expiry).Round(time.Minute))
	fmt.Printf("    %s\nor, without bazel:\n    %s\n", p.bazelCommand("down"), p.kubectlDeleteCommand())
	if !reachable(p.url()) {
		fmt.Printf("\nNote: the link is not reachable from this machine right now. Is the tunnel up? (bbaccess --tunnel)\n")
	}
}

// reachable reports whether Grafana answers at url, which also checks that
// the tunnel resolves and relays the name.
func reachable(url string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	rsp, err := client.Get(url + "api/health")
	if err != nil {
		return false
	}
	rsp.Body.Close()
	return rsp.StatusCode == http.StatusOK
}

// waitForReady polls the preview's pods until one passes its readiness probe,
// logging state changes (image pulls, init container progress, crashes) on
// the way.
func (p *preview) waitForReady(ctx context.Context) error {
	deadline := time.Now().Add(readyTimeout)
	last := ""
	for {
		out, err := p.kube.output(ctx, nil, "get", "pods", "-l", "app="+p.name, "-o", "json")
		if err != nil {
			return err
		}
		var pods corev1.PodList
		if err := json.Unmarshal(out, &pods); err != nil {
			return fmt.Errorf("parse pod list: %w", err)
		}
		for i := range pods.Items {
			if podReady(&pods.Items[i]) {
				return nil
			}
		}
		if state := describePods(pods.Items); state != last {
			log.Print(state)
			last = state
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pod not ready after %s", readyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(readyPoll):
		}
	}
}

func podReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func describePods(pods []corev1.Pod) string {
	if len(pods) == 0 {
		return "no pod yet"
	}
	var parts []string
	for _, pod := range pods {
		desc := fmt.Sprintf("pod %s: %s", pod.Name, pod.Status.Phase)
		var reasons []string
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
			for _, cs := range statuses {
				if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
					reasons = append(reasons, cs.Name+" "+cs.State.Waiting.Reason)
				}
				if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
					reasons = append(reasons, fmt.Sprintf("%s exited %d", cs.Name, cs.State.Terminated.ExitCode))
				}
			}
		}
		if len(reasons) > 0 {
			desc += " (" + strings.Join(reasons, ", ") + ")"
		}
		parts = append(parts, desc)
	}
	return strings.Join(parts, "; ")
}

// collectDashboards builds the generated dashboards and returns every
// dashboard JSON that the deployed Grafana image would carry, keyed by file
// name: the hand-edited ones under tools/metrics/grafana/dashboards plus the
// outputs of //tools/metrics/grafana/generated:all_dashboards.
func collectDashboards(ctx context.Context) (map[string][]byte, error) {
	build := exec.CommandContext(ctx, "bazel", "build", generatedTarget)
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return nil, fmt.Errorf("bazel build %s: %w", generatedTarget, err)
	}
	query := exec.CommandContext(ctx, "bazel", "cquery", "--output=files", generatedTarget)
	query.Stderr = os.Stderr
	out, err := query.Output()
	if err != nil {
		return nil, fmt.Errorf("bazel cquery %s: %w", generatedTarget, err)
	}
	var paths []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			paths = append(paths, line)
		}
	}
	handEdited, err := filepath.Glob(filepath.Join(dashboardsDir, "*.json"))
	if err != nil {
		return nil, err
	}
	paths = append(paths, handEdited...)

	dashboards := make(map[string][]byte, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		if _, dup := dashboards[name]; dup {
			return nil, fmt.Errorf("two dashboards are named %s (one of them is %s)", name, path)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(b) > maxConfigMapBytes {
			return nil, fmt.Errorf("%s is %d bytes, which does not fit in a configmap", path, len(b))
		}
		dashboards[name] = b
	}
	if len(dashboards) == 0 {
		return nil, errors.New("found no dashboards")
	}
	return dashboards, nil
}

// readProvisioning returns the deployed provisioning files (dashboard
// provider and datasources) with %{ENV} filled in, as the internal repo's
// grafana_configmap does.
func readProvisioning(envName string) (map[string]string, error) {
	files := map[string]string{
		"dashboards.yml":  filepath.Join(provisioningDir, "dashboards", "dashboards.yml"),
		"datasources.yml": filepath.Join(provisioningDir, "datasources", "datasources.yml"),
	}
	out := make(map[string]string, len(files))
	for key, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		out[key] = strings.ReplaceAll(string(b), "%{ENV}", envName)
	}
	return out, nil
}

// dashboardConfigMaps packs the dashboards into ConfigMaps named
// <name>-dashboards-<n>, each under maxConfigMapBytes. Names are sorted so
// the packing is stable between runs.
func (p *preview) dashboardConfigMaps(dashboards map[string][]byte) []*corev1.ConfigMap {
	names := make([]string, 0, len(dashboards))
	for name := range dashboards {
		names = append(names, name)
	}
	sort.Strings(names)

	var cms []*corev1.ConfigMap
	var current *corev1.ConfigMap
	size := 0
	for _, name := range names {
		b := dashboards[name]
		if current == nil || size+len(b) > maxConfigMapBytes {
			current = &corev1.ConfigMap{
				APIVersion: "v1", Kind: "ConfigMap",
				ObjectMeta: p.objectMeta(fmt.Sprintf("%s-dashboards-%d", p.name, len(cms))),
				Data:       map[string]string{},
			}
			cms = append(cms, current)
			size = 0
		}
		current.Data[name] = string(b)
		size += len(b)
	}
	return cms
}

func (p *preview) provisioningConfigMap(files map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		APIVersion: "v1", Kind: "ConfigMap",
		ObjectMeta: p.objectMeta(p.name + "-config"),
		Data:       files,
	}
}

func (p *preview) service() *corev1.Service {
	return &corev1.Service{
		APIVersion: "v1", Kind: "Service",
		ObjectMeta: p.objectMeta(p.name),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": p.name},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Protocol:   corev1.ProtocolTCP,
				Port:       80,
				TargetPort: intstr.FromString("http"),
			}},
		},
	}
}

// job is the deployed grafana-server pod spec, wrapped in a Job that expires
// after maxLifetime and cleans up after itself. It is created suspended. The
// dashboard configmaps are projected into one directory for the init
// container to flatten.
func (p *preview) job(dashboardConfigMaps []*corev1.ConfigMap) *batchv1.Job {
	healthz := &corev1.Probe{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")}}
	var dashboardSources []corev1.VolumeProjection
	for _, cm := range dashboardConfigMaps {
		dashboardSources = append(dashboardSources, corev1.VolumeProjection{
			ConfigMap: &corev1.ConfigMapProjection{
				Name: cm.Name,
			},
		})
	}
	return &batchv1.Job{
		APIVersion: "batch/v1", Kind: "Job",
		ObjectMeta: p.objectMeta(p.name),
		Spec: batchv1.JobSpec{
			Suspend:                 new(true),
			ActiveDeadlineSeconds:   new(int64(maxLifetime / time.Second)),
			TTLSecondsAfterFinished: new(int32(0)),
			// Grafana crashing should restart it, not end the preview.
			BackoffLimit: new(int32(1000)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: p.labels()},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					// The configmaps are mounted with a nested structure which
					// Grafana doesn't handle properly, so flatten them into a
					// dashboards dir with all json files at the top level, like
					// the deployed init container does.
					InitContainers: []corev1.Container{{
						Name:    "setup-dashboards",
						Image:   grafanaImage,
						Command: []string{"/bin/sh"},
						Args:    []string{"-ec", "cp -L /grafana-dashboards/*.json " + grafanaDashboards + "/"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "dashboard-sources", MountPath: "/grafana-dashboards", ReadOnly: true},
							{Name: "dashboards", MountPath: grafanaDashboards},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "grafana",
						Image: grafanaImage,
						Env: []corev1.EnvVar{
							{Name: "GF_AUTH_ANONYMOUS_ENABLED", Value: "true"},
							{Name: "GF_AUTH_ANONYMOUS_ORG_ROLE", Value: "Editor"},
							{Name: "GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH", Value: grafanaDashboards + "/buildbuddy.json"},
							{Name: "GF_PLUGINS_PREINSTALL", Value: clickhousePlugin},
							// Expanded by Grafana into the datasources' ${CLICKHOUSE_PASSWORD}.
							{Name: "CLICKHOUSE_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									Name: clickhousePasswordsSecret,
									Key:  p.env.clickhousePasswordKey,
								},
							}},
							{Name: "GF_DATE_FORMATS_DEFAULT_TIMEZONE", Value: "America/Los_Angeles"},
						},
						Ports:          []corev1.ContainerPort{{Name: "http", ContainerPort: 3000}},
						LivenessProbe:  healthz,
						ReadinessProbe: healthz,
						VolumeMounts: []corev1.VolumeMount{
							{Name: "dashboards", MountPath: grafanaDashboards},
							{Name: "provisioning", MountPath: "/etc/grafana/provisioning"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "dashboards", EmptyDir: &corev1.EmptyDirVolumeSource{}},
						{Name: "dashboard-sources", Projected: &corev1.ProjectedVolumeSource{
							Sources: dashboardSources,
						}},
						{Name: "provisioning", ConfigMap: &corev1.ConfigMapVolumeSource{
							Name: p.name + "-config",
							Items: []corev1.KeyToPath{
								{Key: "dashboards.yml", Path: "dashboards/dashboards.yml"},
								{Key: "datasources.yml", Path: "datasources/datasources.yml"},
							},
						}},
					},
				},
			},
		},
	}
}

func (p *preview) objectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: p.env.namespace,
		Labels:    p.labels(),
	}
}

// kubectl runs kubectl against one context and namespace.
type kubectl struct {
	context   string
	namespace string
}

func (k kubectl) command(ctx context.Context, args ...string) *exec.Cmd {
	full := append([]string{"--context", k.context, "--namespace", k.namespace}, args...)
	return exec.CommandContext(ctx, "kubectl", full...)
}

// output runs kubectl and returns its stdout. stderr goes into the error on
// failure and to our stderr otherwise, so warnings still show.
func (k kubectl) output(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := k.command(ctx, args...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("kubectl %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	if stderr.Len() > 0 {
		os.Stderr.Write(stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// passthrough runs kubectl with our stdout and stderr, for commands whose
// output is for the user.
func (k kubectl) passthrough(ctx context.Context, args ...string) error {
	cmd := k.command(ctx, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// apply server-side applies the given objects in one request. Server-side
// apply is used because client-side apply stores a copy of each object in an
// annotation, which would push the dashboard configmaps past the size limit.
func (k kubectl) apply(ctx context.Context, objects ...any) error {
	list := map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items":      objects,
	}
	b, err := json.Marshal(list)
	if err != nil {
		return err
	}
	out, err := k.output(ctx, b, "apply", "--server-side", "--field-manager="+fieldManager, "-f", "-")
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			log.Print(line)
		}
	}
	return nil
}
