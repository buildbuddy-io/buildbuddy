package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	// This exit code is returned if the metrics look unhealthy and the rollout should proceed
	metricsHealthyExitCode = 0

	greenColor = "#36a64f"
	redColor   = "#ad1411"
)

var (
	spec       = flag.Struct("spec", Config{}, "Config to specify which metrics should be monitored by this script.")
	configPath = flag.String("config_path", "config.yaml", "Path to config file.")
)

type metricStatus struct {
	name                      string
	secondary                 bool
	consecutiveUnhealthyCount int
}

// This script is intended to compare system health for a canary that has been deployed to the rest of the apps
// If the metrics are healthy, this script will return exit code 0 indicating that the rollout should proceed
// If the metrics are not healthy for the specified amount of time, the script will return an exit code indicating
// that the release should be rolled back
//
// Pre-requisites:
//   - You must create a tunnel to prometheus and port-forward in order to run this script with
//     `kubectl --context="" --namespace=<NAMESPACE> port-forward "<POD_NAME>" --address 0.0.0.0 <FROM_PORT>:<TO_PORT>`
func main() {
	flag.Parse()
	err := config.LoadFromFile(*configPath)
	if err != nil {
		log.Fatalf("Could not read metrics config from %s: %s", *configPath, err)
	}
	err = log.Configure()
	if err != nil {
		log.Fatalf("Could not configure logger: %s", err)
	}
	prometheusClient, err := api.NewClient(api.Config{
		Address: spec.PrometheusAddress,
	})
	if err != nil {
		log.Fatalf("Error creating prometheus client: %s\nDid you run the prometheus port-forward command?", err)
	}
	promAPI := promapi.NewAPI(prometheusClient)

	mu := &sync.Mutex{}
	failedMetrics := make([]string, 0)
	metricStatuses := make([]*metricStatus, 0, len(spec.PrometheusMetrics))

	eg, gctx := errgroup.WithContext(context.Background())
	for _, metric := range spec.PrometheusMetrics {
		status := &metricStatus{
			name:      metric.Name,
			secondary: metric.Secondary,
		}
		metricStatuses = append(metricStatuses, status)

		metric := metric
		eg.Go(func() error {
			pollingIntervalSec := spec.PollingIntervalSeconds
			if metric.PollingIntervalSeconds != 0 {
				pollingIntervalSec = metric.PollingIntervalSeconds
			}

			maxUnhealthyCount := spec.MaxMetricPollUnhealthyCount
			if metric.MaxMetricPollUnhealthyCount != 0 {
				maxUnhealthyCount = metric.MaxMetricPollUnhealthyCount
			}

			monitoringTimer := time.After(time.Duration(spec.MonitoringTimeframeSeconds) * time.Second)
			monitoringTicker := time.NewTicker(time.Duration(pollingIntervalSec) * time.Second)
			defer monitoringTicker.Stop()

			for {
				select {
				case <-gctx.Done():
					return nil
				case <-monitoringTimer:
					return nil
				case <-monitoringTicker.C:
					healthy, err := metricHealthy(promAPI, metric.Name, metric.Query, metric.HealthThreshold, metric.IsMissingDataValid)
					if err != nil {
						log.Warningf("Error querying metric %s: %s", metric.Name, err)
					}

					if healthy {
						status.consecutiveUnhealthyCount = 0
					} else {
						status.consecutiveUnhealthyCount++
						log.Debugf("Metric %s is unhealthy, increasing unhealthy count to %d", metric.Name, status.consecutiveUnhealthyCount)
					}

					if status.consecutiveUnhealthyCount >= maxUnhealthyCount {
						log.Warningf("Metric %s has been consecutively unhealthy %d times. Marking as failed.", metric.Name, status.consecutiveUnhealthyCount)

						mu.Lock()
						failedMetrics = append(failedMetrics, metric.Name)
						mu.Unlock()

						if !metric.Secondary || len(failedMetrics) >= spec.MaxSecondaryMetricFailureCount {
							failedMetricsStr := strings.Join(failedMetrics, ", ")
							return errors.New(fmt.Sprintf("Metrics unhealthy: %s", failedMetricsStr))
						}
						return nil
					}
				}
			}
		})
	}

	err = eg.Wait()

	triggeredRollback := err != nil
	sendMetricOverview(metricStatuses, triggeredRollback, spec.MaxMetricPollUnhealthyCount, spec.MaxSecondaryMetricFailureCount)

	if triggeredRollback {
		log.Fatalf("Exiting metrics script: %s", err)
	}

	// If all metrics look healthy for the duration of the monitoring timeframe, continue with rollout
	log.Info("Metrics look healthy! Exiting the script.")
	os.Exit(metricsHealthyExitCode)
}

func queryMetric(promAPI promapi.API, metricName string, query string, isMissingDataValid bool) (float64, error) {
	log.Debugf("Attempting to query metric %s", metricName)

	result, _, err := promAPI.Query(context.Background(), query, time.Now())
	if err != nil {
		return 0, err
	}

	resultVector := result.(model.Vector)
	if len(resultVector) == 0 {
		if isMissingDataValid {
			return 0, nil
		}
		return 0, status.NotFoundErrorf("no data for metric %s", metricName)
	}

	// Default NAN values to 0
	// NAN is reported if there is no data (Ex. if there are no invocations on dev, invocation failure rate can't be reported)
	var value float64
	if !math.IsNaN(float64(resultVector[0].Value)) {
		value = float64(resultVector[0].Value)
	}
	return value, nil
}

func metricHealthy(promAPI promapi.API, metricName string, query string, healthThreshold HealthThreshold, isMissingDataValid bool) (bool, error) {
	metricValue, err := queryMetric(promAPI, metricName, query, isMissingDataValid)
	if err != nil {
		return false, err
	}

	if healthThreshold.AbsoluteRange != nil {
		threshold := healthThreshold.AbsoluteRange
		return absoluteRangeMetricHealthy(metricName, metricValue, threshold.Min, threshold.Max), nil
	}

	if healthThreshold.RelativeRange != nil {
		return relativeRangeMetricHealthy(promAPI, metricName, metricValue, *healthThreshold.RelativeRange, isMissingDataValid)
	}

	return false, status.InvalidArgumentErrorf("must specify a health threshold")
}

func absoluteRangeMetricHealthy(metricName string, metricValue float64, min *float64, max *float64) bool {
	log.Debugf("Polling absolute range metric %s, metric value is %v", metricName, metricValue)

	if min != nil && metricValue < *min {
		return false
	}
	if max != nil && metricValue > *max {
		return false
	}
	return true
}

func relativeRangeMetricHealthy(promAPI promapi.API, metricName string, metricValue float64, relativeRange RelativeRange, isMissingDataValid bool) (bool, error) {
	comparisonValue, err := queryMetric(promAPI, fmt.Sprintf("comparison-%s", metricName), relativeRange.ComparisonQuery, isMissingDataValid)
	if err != nil {
		return false, err
	}

	log.Debugf("Polling relative range metric %s, metric value is %v, comparison value is %v", metricName, metricValue, comparisonValue)

	if relativeRange.Within != nil {
		within := relativeRange.Within
		if within.GreaterBy != nil && *within.GreaterBy {
			greaterBy := metricValue - comparisonValue
			return greaterBy < 0 || greaterBy < within.Value, nil
		}
		if within.LessBy != nil && *within.LessBy {
			lessBy := comparisonValue - metricValue
			return lessBy < 0 || lessBy < within.Value, nil
		}
		return math.Abs(comparisonValue-metricValue) < within.Value, nil
	} else if relativeRange.WithinPercentage != nil {
		within := relativeRange.WithinPercentage
		if within.GreaterBy != nil && *within.GreaterBy {
			percentGreater := (metricValue - comparisonValue) / comparisonValue
			return percentGreater < 0 || percentGreater < within.Value, nil
		}
		if within.LessBy != nil && *within.LessBy {
			percentLess := (comparisonValue - metricValue) / comparisonValue
			return percentLess < 0 || percentLess < within.Value, nil
		}
		absPercentDiff := math.Abs(comparisonValue-metricValue) / comparisonValue
		return absPercentDiff < within.Value, nil
	}
	return false, status.InvalidArgumentErrorf("must specify a Within field for relative range thresholds")
}

type AttachmentBlock struct {
	Blocks []SlackBlock `json:"blocks"`
	Color  string       `json:"color"`
}

type SlackBlock struct {
	Type string    `json:"type"`
	Text TextBlock `json:"text"`
}

type TextBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func sendMetricOverview(metricStatuses []*metricStatus, triggeredRollback bool, maxPollUnhealthyCount int, maxSecondaryMetricFailureCount int) {
	slackWebhookURL := os.Getenv("SLACK_WEBHOOK")
	if slackWebhookURL == "" {
		log.Errorf("No slack webhook set. Cannot send warning messages.")
		return
	}

	msg := ""
	for _, m := range metricStatuses {
		metricLabel := fmt.Sprintf("`%s`", m.name)
		if m.secondary {
			metricLabel += " _(Secondary metric)_"
		}

		statusLabel := "Healthy"
		if m.consecutiveUnhealthyCount > 0 {
			statusLabel = fmt.Sprintf("Last %d consecutive polls were unhealthy", m.consecutiveUnhealthyCount)
		}

		metricOverview := fmt.Sprintf("%s: %s\n", metricLabel, statusLabel)

		if m.consecutiveUnhealthyCount > 0 {
			// Display unhealthy metrics at the top of the message
			msg = metricOverview + msg
		} else {
			msg += metricOverview
		}
	}

	msgColor := greenColor
	if triggeredRollback {
		msgColor = redColor
		msg += fmt.Sprintf("\nIf a metric consecutively polls as unhealthy %d times, it is considered a failure.\n"+
			"If a primary metric fails, it will trigger a rollback immediately.\n%d secondary metrics must fail to trigger"+
			" a rollback.", maxPollUnhealthyCount, maxSecondaryMetricFailureCount)
	}

	data := map[string][]AttachmentBlock{
		"attachments": {
			{
				Color: msgColor,
				Blocks: []SlackBlock{
					{
						Type: "section",
						Text: TextBlock{
							Type: "mrkdwn",
							Text: "*Canary metric overview:*",
						},
					},
					{
						Type: "section",
						Text: TextBlock{
							Type: "mrkdwn",
							Text: msg,
						},
					},
				},
			},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		log.Errorf("Error marshalling data: %s", err)
		return
	}

	req, err := http.NewRequest("POST", slackWebhookURL, bytes.NewBuffer(payload))
	if err != nil {
		log.Errorf("Error creating request: %s", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Error making request: %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Errorf("Error sending data to slack: %s", resp.Status)
		return
	}
}
