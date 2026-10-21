// Package pebble_dash builds the Grafana rows that show the health of a
// pebble database: compaction, operation latency, the block cache, zombie
// tables and per-level LSM statistics. Every binary that opens a database
// through enterprise/server/util/pebble exports the same metrics (apps and
// cache proxies through pebble_cache, metadata servers through the raft
// store), so each dashboard builds the rows from here with its own Config
// instead of keeping a copy of the panels.
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

	// cacheNameVar is the dashboard variable that PerCache repeats over.
	cacheNameVar = "cache_name"
)

// Config says which pebble database a dashboard shows.
type Config struct {
	// Filter is the label selector for this dashboard's pods, including the
	// region, e.g. `region="${region}", job="buildbuddy-app"`.
	Filter string
	// CacheName is the cache_name label value of the database, such as
	// "raft". PerCache sets it to the ${cache_name} variable instead, which
	// also repeats every panel, and the levels row, once per selected cache.
	CacheName string
	// PebbleID is the pebble_id label value, which the operation count and
	// latency metrics carry instead of cache_name. Empty means CacheName:
	// pebble_cache opens its database under the cache name, while the raft
	// store is opened as "raft_store" and names its cache "raft".
	PebbleID string
	// Compression adds the compression ratio panel, which caches that write
	// through the zstd stream compressor (pebble_cache, metacache) observe;
	// the raft store does not.
	Compression bool
}

// PerCache returns the Config for a dashboard whose pods run pebble_cache,
// repeating the rows once per value of its ${cache_name} variable.
func PerCache(filter string) Config {
	return Config{
		Filter:      filter,
		CacheName:   "${" + cacheNameVar + "}",
		Compression: true,
	}
}

// repeat reports whether CacheName follows the ${cache_name} variable.
func (c Config) repeat() bool {
	return c.CacheName == "${"+cacheNameVar+"}"
}

// cacheFilter selects this database's series by cache_name.
func (c Config) cacheFilter() string {
	return c.Filter + `, cache_name="` + c.CacheName + `"`
}

// pebbleFilter selects this database's series by pebble_id.
func (c Config) pebbleFilter() string {
	id := c.PebbleID
	if id == "" {
		id = c.CacheName
	}
	return c.Filter + `, pebble_id="` + id + `"`
}

// panel returns a full-width panel titled for this database, repeated per
// cache when CacheName follows the ${cache_name} variable.
func (c Config) panel(title, unit string) *timeseries.PanelBuilder {
	p := dash.PlainTimeseries(title+" ("+c.CacheName+")", unit).
		Span(24)
	if c.repeat() {
		p.Repeat(cacheNameVar).
			RepeatDirection(dashboard.PanelRepeatDirectionH)
	}
	return p
}

// Row returns the "Remote cache pebble" row: compaction, operation rate and
// latency, the block cache and zombie tables.
func Row(c Config) *dashboard.RowBuilder {
	cf := c.cacheFilter()
	requests := pebble + `_block_cache_requests_count`
	// opLatency is quantile q of operation latency, by operation.
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

// compressionPanel shows how many times smaller compression makes a stream
// of data, at percentiles of that factor across streams.
func (c Config) compressionPanel() *timeseries.PanelBuilder {
	// factor is the inverse of the compression ratio (compressed /
	// decompressed bytes) at quantile q of the ratio distribution, i.e. how
	// many times smaller the data got. Because of the inversion, quantile q
	// of the ratio is percentile 1-q of the factor.
	factor := func(q string) string {
		return `1 / histogram_quantile(` + q + `, sum(rate(buildbuddy_pebble_compression_ratio_bucket{` + c.cacheFilter() + `}[10m])) by (le))`
	}
	return c.panel("Compression Ratio", "").
		Description("How many times smaller pebble's compression makes a stream of data (decompressed / compressed bytes), at percentiles of that factor across streams: p90 is what the best-compressing 10% of streams exceed, p1 is what 99% of streams achieve. Each series is the inverse of the matching quantile of the compressed/decompressed ratio histogram.").
		WithTarget(dash.PromQuery(factor("0.1"), "p90").RefId("A")).
		WithTarget(dash.PromQuery(factor("0.5"), "p50").RefId("B")).
		WithTarget(dash.PromQuery(factor("0.99"), "p1").RefId("C"))
}

// LevelsRow returns the "Remote cache pebble levels" row: file counts,
// sizes, compaction scores and the byte and table flow rates of each LSM
// level.
func LevelsRow(c Config) *dashboard.RowBuilder {
	cf := c.cacheFilter()
	// gauge sums a per-level gauge across pods.
	gauge := func(title, unit, metric string) *timeseries.PanelBuilder {
		return dash.PlainTimeseries(title, unit).
			WithTarget(dash.PromQuery(`sum(`+level+`_`+metric+`{`+cf+`}) by (level)`, "{{level}}"))
	}
	// rate sums the per-second rate of a per-level counter across pods.
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
