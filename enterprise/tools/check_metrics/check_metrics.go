package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
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
)

var (
	config     = flagutil.New("spec", Config{}, "Config to specify which metrics should be monitored by this script.")
	configPath = flag.String("config_path", "config.yaml", "Path to config file.")
)

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
	err := yaml.PopulateFlagsFromFile(*configPath)
	if err != nil {
		log.Fatalf("Could not read metrics config from %s: %s", *configPath, err)
	}
	err = log.Configure()
	if err != nil {
		log.Fatalf("Could not configure logger: %s", err)
	}
	prometheusClient, err := api.NewClient(api.Config{
		Address: config.PrometheusAddress,
	})
	if err != nil {
		log.Fatalf("Error creating prometheus client: %s\nDid you run the prometheus port-forward command?", err)
	}
	promAPI := promapi.NewAPI(prometheusClient)

	eg, gctx := errgroup.WithContext(context.Background())
	for _, metric := range config.PrometheusMetrics {
		metric := metric
		eg.Go(func() error {
			pollingIntervalSec := config.PollingIntervalSeconds
			if metric.PollingIntervalSeconds != 0 {
				pollingIntervalSec = metric.PollingIntervalSeconds
			}

			maxUnhealthyCount := config.MaxUnhealthyCount
			if metric.MaxUnhealthyCount != 0 {
				maxUnhealthyCount = metric.MaxUnhealthyCount
			}

			monitoringTimer := time.After(time.Duration(config.MonitoringTimeframeSeconds) * time.Second)
			monitoringTicker := time.NewTicker(time.Duration(pollingIntervalSec) * time.Second)
			defer monitoringTicker.Stop()

			unhealthyCount := 0
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
						unhealthyCount = 0
					} else {
						unhealthyCount++
						log.Debugf("Metric %s is unhealthy, increasing unhealthy count to %d", metric.Name, unhealthyCount)
					}

					if unhealthyCount >= maxUnhealthyCount {
						return errors.New(fmt.Sprintf("%s metrics unhealthy.", metric.Name))
					}
				}
			}
		})
	}

	err = eg.Wait()
	if err != nil {
		log.Fatalf("Exiting metrics script: %s", err)
	}

	// If all metrics look healthy for the duration of the monitoring timeframe, continue with rollout
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
	return float64(resultVector[0].Value), nil
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
