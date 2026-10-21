// Package pebble_dash builds the Grafana rows for a pebble database's
// metrics, which apps, cache proxies and raft metadata servers all export.
package pebble_dash

import (
	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const (
	pebbleCache = `buildbuddy_remote_cache_pebble_cache`
	pebble      = pebbleCache + `_pebble`
	level       = pebble + `_level`
	zombie      = pebbleCache + `_zombie_table`

	// The dashboard variable PerCache repeats over.
	cacheNameVar = "cache_name"
)

// Config selects the pebble database a dashboard shows.
type Config struct {
	// Selects the dashboard's pods, e.g. `region="${region}", job="buildbuddy-app"`.
	Filter string
	// The cache_name label value. "${cache_name}" repeats the rows per cache.
	CacheName string
	// The pebble_id label value of the op metrics; empty means CacheName.
	PebbleID string
	// Adds the compression ratio panel, which the raft store does not feed.
	Compression bool
}

// PerCache configures a pebble_cache dashboard, repeated per ${cache_name}.
func PerCache(filter string) Config {
	return Config{
		Filter:      filter,
		CacheName:   "${" + cacheNameVar + "}",
		Compression: true,
	}
}

func (c Config) repeat() bool {
	return c.CacheName == "${"+cacheNameVar+"}"
}

func (c Config) cacheFilter() string {
	return c.Filter + `, cache_name="` + c.CacheName + `"`
}

func (c Config) pebbleFilter() string {
	id := c.PebbleID
	if id == "" {
		id = c.CacheName
	}
	return c.Filter + `, pebble_id="` + id + `"`
}

// panel returns a full-width panel titled for the cache, repeated if needed.
func (c Config) panel(title, unit string) *timeseries.PanelBuilder {
	p := dash.PlainTimeseries(title+" ("+c.CacheName+")", unit).
		Span(24)
	if c.repeat() {
		p.Repeat(cacheNameVar).
			RepeatDirection(dashboard.PanelRepeatDirectionH)
	}
	return p
}

// Row returns the "Remote cache pebble" row.
func Row(c Config) *dashboard.RowBuilder {
	cf := c.cacheFilter()
	requests := pebble + `_block_cache_requests_count`
	opLatency := func(q string) string {
		return `histogram_quantile(` + q + `, sum(rate(` + pebble + `_op_latency_usec_bucket{` + c.pebbleFilter() + `}[1m])) by (le, pebble_op))`
	}
	r := dashboard.NewRowBuilder("Remote cache pebble").
		Collapsed(true)
	if c.Compression {
		r.WithPanel(c.compressionPanel())
	}
	return r.
		WithPanel(c.panel("Compaction rate by type", "").
			WithTarget(dash.PromQuery(`sum(rate(`+pebble+`_compact_count{`+cf+`}[1m])) by (compaction_type)`, "__auto"))).
		WithPanel(c.panel("Compaction state", dash.UnitBytes).
			OverrideByName("in progress (count)", dash.RightAxisProps(dash.UnitNone)).
			OverrideByName("marked files", dash.RightAxisProps(dash.UnitNone)).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_in_progress_bytes{`+cf+`})`, "in progress (bytes)").RefId("A")).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_in_progress{`+cf+`})`, "in progress (count)").RefId("B")).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_marked_files{`+cf+`})`, "marked files").RefId("C"))).
		WithPanel(c.panel("Compaction estimated debt", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_estimated_debt_bytes{`+cf+`}) by (pod_name)`, "{{pod_name}}"))).
		WithPanel(c.panel("Op Rate", dash.UnitOps).
			WithTarget(dash.PromQuery(`sum(rate(`+pebble+`_op_count{`+c.pebbleFilter()+`}[1m])) by (pebble_op)`, "__auto"))).
		WithPanel(c.panel("Op p50 Latency", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(opLatency("0.50"), "__auto"))).
		WithPanel(c.panel("Op p95 Latency", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(opLatency("0.95"), "__auto"))).
		WithPanel(c.panel("Op p99 Latency", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(opLatency("0.99"), "__auto"))).
		WithPanel(c.panel("Block cache size", dash.UnitBytes).
			Height(10).
			WithTarget(dash.PromQuery(`sum by (pod_name) (`+pebble+`_block_cache_size_bytes{`+cf+`})`, "{{pod_name}}"))).
		WithPanel(c.panel("Block cache hits and misses", dash.UnitRequestsPerSec).
			Height(10).
			AxisPlacement(common.AxisPlacementLeft).
			Legend(dash.RightLegend()).
			Tooltip(dash.MultiTooltip()).
			OverrideByName("hit_ratio", dash.RightAxisProps(dash.UnitPercentUnit)).
			WithTarget(dash.PromQuery(`sum by (cache_status) (rate(`+requests+`{`+cf+`}[${window}]))`, "{{cache_status}}").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(`+requests+`{`+cf+`, cache_status="hit"}[${window}])) / sum(rate(`+requests+`{`+cf+`}[${window}]))`, "hit_ratio").RefId("B"))).
		WithPanel(c.panel("Zombie Table Count", "").
			Height(10).
			WithTarget(dash.PromQuery(`sum by (pod_name) (`+zombie+`_count{`+cf+`})`, "{{pod_name}}"))).
		WithPanel(c.panel("Zombie Table Size", dash.UnitBytes).
			Height(10).
			WithTarget(dash.PromQuery(`sum by (pod_name) (`+zombie+`_size_bytes{`+cf+`})`, "{{pod_name}}")))
}

func (c Config) compressionPanel() *timeseries.PanelBuilder {
	// Quantile q of the compressed/decompressed ratio is percentile 1-q of
	// the compression factor.
	factor := func(q string) string {
		return `1 / histogram_quantile(` + q + `, sum(rate(buildbuddy_pebble_compression_ratio_bucket{` + c.cacheFilter() + `}[10m])) by (le))`
	}
	return c.panel("Compression Ratio", "").
		Description("How many times smaller pebble's compression makes a stream of data (decompressed / compressed bytes), at percentiles of that factor across streams: p90 is what the best-compressing 10% of streams exceed, p1 is what 99% of streams achieve. Each series is the inverse of the matching quantile of the compressed/decompressed ratio histogram.").
		WithTarget(dash.PromQuery(factor("0.1"), "p90").RefId("A")).
		WithTarget(dash.PromQuery(factor("0.5"), "p50").RefId("B")).
		WithTarget(dash.PromQuery(factor("0.99"), "p1").RefId("C"))
}

// LevelsRow returns the "Remote cache pebble levels" row.
func LevelsRow(c Config) *dashboard.RowBuilder {
	cf := c.cacheFilter()
	gauge := func(title, unit, metric string) *timeseries.PanelBuilder {
		return dash.PlainTimeseries(title, unit).
			WithTarget(dash.PromQuery(`sum(`+level+`_`+metric+`{`+cf+`}) by (level)`, "{{level}}"))
	}
	rate := func(title, unit, metric string) *timeseries.PanelBuilder {
		return dash.PlainTimeseries(title, unit).
			WithTarget(dash.PromQuery(`sum(rate(`+level+`_`+metric+`{`+cf+`}[1m])) by (level)`, "{{level}}"))
	}
	r := dashboard.NewRowBuilder("Remote cache pebble levels (" + c.CacheName + ")").
		Collapsed(true)
	if c.repeat() {
		r.Repeat(cacheNameVar)
	}
	return r.
		WithPanel(gauge("Number files (by level)", "", "num_files")).
		WithPanel(gauge("Size (by level)", dash.UnitBytes, "size_bytes")).
		WithPanel(gauge("Compaction score (by level)", dash.UnitNone, "score")).
		WithPanel(rate("Bytes in (by level)", dash.UnitBytes, "bytes_in_count")).
		WithPanel(rate("Bytes ingested (by level)", dash.UnitBytes, "bytes_ingested_count")).
		WithPanel(rate("Bytes moved (by level)", dash.UnitBytes, "bytes_moved_count")).
		WithPanel(rate("Bytes read (by level)", dash.UnitBytes, "bytes_read_count")).
		WithPanel(rate("Bytes compacted (by level)", dash.UnitBytes, "bytes_compacted_count")).
		WithPanel(rate("Bytes flushed (by level)", dash.UnitBytes, "bytes_flushed_count")).
		WithPanel(rate("Tables compacted (by level)", dash.UnitNone, "tables_compacted_count")).
		WithPanel(rate("Tables flushed (by level)", dash.UnitNone, "tables_flushed_count")).
		WithPanel(rate("Tables ingested (by level)", dash.UnitNone, "tables_ingested_count")).
		WithPanel(rate("Tables moved (by level)", dash.UnitNone, "tables_moved_count"))
}
