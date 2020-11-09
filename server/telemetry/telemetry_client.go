package telemetry

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/google/uuid"

	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
)

const (
	unknownFieldValue        = "Unknown"
	versionFilename          = "VERSION"
	installationUUIDFilename = "buildbuddy_installation_uuid"
	maxFailedLogs            = 365
)

var (
	telemetryInterval = flag.Duration("telemetry_interval", 24*time.Hour, "How often telemetry data will be reported")
	verboseTelemetry  = flag.Bool("verbose_telemetry_client", false, "If true; print telemetry client information")
	disableTelemetry  = flag.Bool("disable_telemetry", false, "If true; telemetry will be disabled")
	telemetryEndpoint = flag.String("telemetry_endpoint", "grpcs://t.buildbuddy.io:443", "The telemetry endpoint to use")
)

type TelemetryClient struct {
	env    environment.Env
	ticker *time.Ticker
	quit   chan struct{}

	version          string
	instanceUUID     string
	installationUUID string

	failedLogs []*telpb.TelemetryLog
}

func NewTelemetryClient(env environment.Env) *TelemetryClient {
	return &TelemetryClient{
		env:              env,
		version:          getAppVersion(),
		instanceUUID:     getInstanceUUID(),
		installationUUID: getInstallationUUID(env),
		failedLogs:       []*telpb.TelemetryLog{},
	}
}

func (t *TelemetryClient) Start() {
	t.ticker = time.NewTicker(*telemetryInterval)
	t.quit = make(chan struct{})

	if *disableTelemetry {
		printIfVerbose("Telemetry disabled")
		return
	}

	go func() {
		for {
			select {
			case <-t.ticker.C:
				t.logTelemetryData()
			case <-t.quit:
				printIfVerbose("Telemetry task %d exiting.", 0)
				break
			}
		}
	}()

	// Log once on startup.
	t.logTelemetryData()
}

func (t *TelemetryClient) Stop() {
	close(t.quit)
	t.ticker.Stop()
}

func (t *TelemetryClient) logTelemetryData() {
	ctx := context.Background()
	conn, err := grpc_client.DialTarget(*telemetryEndpoint)
	if err != nil {
		printIfVerbose("Error dialing endpoint: %s", err)
		return
	}
	defer conn.Close()
	client := telpb.NewTelemetryClient(conn)

	log := &telpb.TelemetryLog{
		InstallationUuid: t.installationUUID,
		InstanceUuid:     t.instanceUUID,
		LogUuid:          getLogUUID(),
		RecordedAtUsec:   time.Now().UnixNano() / int64(time.Microsecond),
		AppVersion:       t.version,
		AppUrl:           t.env.GetConfigurator().GetAppBuildBuddyURL(),
		Hostname:         getHostname(),
		TelemetryStat:    &telpb.TelemetryStat{},
		TelemetryFeature: getFeatures(t.env),
	}

	// Fill invocation related stats
	if err := t.env.GetInvocationDB().FillCounts(ctx, log.TelemetryStat); err != nil {
		printIfVerbose("Error getting telemetry invocation counts: %s", err)
	}

	// Fill user related stats.
	if userDB := t.env.GetUserDB(); userDB != nil {
		if userDB.FillCounts(ctx, log.TelemetryStat); err != nil {
			printIfVerbose("Error getting telemetry invocation counts: %s", err)
		}
	}

	req := &telpb.LogTelemetryRequest{
		Log: append(t.failedLogs, log),
	}

	response, err := client.LogTelemetry(ctx, req)
	if err != nil || response.Status.Code != 0 {
		printIfVerbose("Error posting telemetry data: %s", err)
		if len(t.failedLogs) >= maxFailedLogs {
			t.failedLogs = t.failedLogs[1:]
		}
		t.failedLogs = append(t.failedLogs, log)
		return
	}

	t.failedLogs = []*telpb.TelemetryLog{}
	printIfVerbose("Telemetry data posted: %+v", response)
}

// TODO(tylerw): use a better logging framework.
func printIfVerbose(message string, args ...interface{}) {
	if !*verboseTelemetry {
		return
	}
	log.Printf(message, args...)
}

// Getters

func getAppVersion() string {
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		printIfVerbose("Error reading getting version file path: %s", err)
		return unknownFieldValue
	}
	versionBytes, err := ioutil.ReadFile(filepath.Join(rfp, versionFilename))
	if err != nil {
		printIfVerbose("Error reading version file: %s", err)
		return unknownFieldValue
	}

	return strings.TrimSpace(string(versionBytes))
}

func getInstallationUUID(env environment.Env) string {
	ctx := context.Background()
	store, err := blobstore.GetConfiguredBlobstore(env.GetConfigurator())
	if err != nil {
		printIfVerbose("Error getting blobstore: %s", err)
		return unknownFieldValue
	}

	exists, err := store.BlobExists(ctx, installationUUIDFilename)
	if err != nil {
		printIfVerbose("Error checking blobstore for UUID: %s", err)
		return unknownFieldValue
	}

	if !exists {
		installationUUID, err := uuid.NewRandom()
		if err != nil {
			printIfVerbose("Error generating installation UUID: %s", err)
			return unknownFieldValue
		}

		_, err = store.WriteBlob(ctx, installationUUIDFilename, []byte(installationUUID.String()))
		if err != nil {
			printIfVerbose("Error storing UUID: %s", err)
			return unknownFieldValue
		}

		return installationUUID.String()
	}

	uuidBytes, err := store.ReadBlob(ctx, installationUUIDFilename)
	if err != nil {
		printIfVerbose("Error getting UUID from blobstore: %s", err)
		return unknownFieldValue
	}
	return string(uuidBytes)
}

func getInstanceUUID() string {
	instanceUUID, err := uuid.NewRandom()
	if err != nil {
		printIfVerbose("Error generating instance UUID: %s", err)
		return unknownFieldValue
	}
	return instanceUUID.String()
}

func getLogUUID() string {
	logUUID, err := uuid.NewRandom()
	if err != nil {
		printIfVerbose("Error generating telemetry log UUID: %s", err)
		return unknownFieldValue
	}
	return logUUID.String()
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		printIfVerbose("Error retrieving hostname: %s", err)
		return unknownFieldValue
	}
	return hostname
}

func getFeatures(env environment.Env) *telpb.TelemetryFeature {
	cache := env.GetCache()
	remoteExecutionConfig := env.GetConfigurator().GetRemoteExecutionConfig()
	apiConfig := env.GetConfigurator().GetAPIConfig()
	authenticator := env.GetAuthenticator()

	return &telpb.TelemetryFeature{
		CacheEnabled: cache != nil,
		RbeEnabled:   remoteExecutionConfig != nil,
		ApiEnabled:   apiConfig != nil && apiConfig.EnableAPI,
		AuthEnabled:  authenticator != nil,
	}
}
