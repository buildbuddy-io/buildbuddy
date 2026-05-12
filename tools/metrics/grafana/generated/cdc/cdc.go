// Generates the Grafana dashboard for Content Defined Chunking (CDC) metrics.
package main

import (
	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const (
	window        = "${window}"
	appRegion     = "${app_region}"
	proxyRegion   = "${proxy_region}"
	cacheProxyJob = `job="cache-proxy"`

	proxyFilter    = `region=~"` + proxyRegion + `", ` + cacheProxyJob
	executorFilter = `region=~"` + proxyRegion + `"`
	appFilter      = `region=~"` + appRegion + `"`
)

const (
	proxyWriteBytesSavedWindow    = `(sum(increase(buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_deduped{` + proxyFilter + `}[` + window + `])) or vector(0))`
	executorWriteBytesSavedWindow = `(sum(increase(buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped{` + executorFilter + `}[` + window + `])) or vector(0))`
	writeBytesSavedWindow         = proxyWriteBytesSavedWindow + ` + ` + executorWriteBytesSavedWindow

	proxyWriteBytesSaved7d    = `(sum(increase(buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_deduped{` + proxyFilter + `}[7d])) or vector(0))`
	executorWriteBytesSaved7d = `(sum(increase(buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped{` + executorFilter + `}[7d])) or vector(0))`
	writeBytesSaved7d         = proxyWriteBytesSaved7d + ` + ` + executorWriteBytesSaved7d
	writeBytesSaved30d        = `(sum(increase(buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_deduped{` + proxyFilter + `}[30d])) or vector(0)) + (sum(increase(buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped{` + executorFilter + `}[30d])) or vector(0))`
	writeBytesSavedPrev7d     = `(sum(increase(buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_deduped{` + proxyFilter + `}[7d] offset 7d)) or vector(0)) + (sum(increase(buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped{` + executorFilter + `}[7d] offset 7d)) or vector(0))`
)

func q(expr, legend string) *prometheus.DataqueryBuilder {
	return dash.PromQuery(expr, legend).Interval(window)
}

func sumRate(metric, filter string) string {
	return `sum(rate(` + metric + `{` + filter + `}[` + window + `]))`
}

func sumRateBy(labels, metric, filter string) string {
	return `sum by (` + labels + `) (rate(` + metric + `{` + filter + `}[` + window + `]))`
}

func sumRateByName(metricNames string) string {
	return `sum(rate({__name__=~"` + metricNames + `", region=~"` + proxyRegion + `"}[` + window + `]))`
}

func sumRateByNameAndLabels(labels, metricNames string) string {
	return `sum by (` + labels + `) (rate({__name__=~"` + metricNames + `", region=~"` + proxyRegion + `"}[` + window + `]))`
}

func statPanel(title, description, unit, expr, legend string) *stat.PanelBuilder {
	return dash.Stat(title, unit).
		Description(description).
		WithTarget(dash.PromQuery(expr, legend))
}

func rowAt(title string, y uint32) *dashboard.RowBuilder {
	return dashboard.NewRowBuilder(title).GridPos(grid(1, 24, 0, y))
}

func grid(h, w, x, y uint32) dashboard.GridPos {
	return dashboard.GridPos{H: h, W: w, X: x, Y: y}
}

func fixedColor(color string) *dashboard.FieldColorBuilder {
	return dashboard.NewFieldColorBuilder().
		Mode(dashboard.FieldColorModeIdFixed).
		FixedColor(color)
}

func fixedThreshold(color string) *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{{Color: color}})
}

func fixedColorStat(panel *stat.PanelBuilder, color string) *stat.PanelBuilder {
	return panel.
		ColorScheme(fixedColor(color)).
		Thresholds(fixedThreshold(color))
}

func heatmapPanel(title, description, unit, expr string) *heatmap.PanelBuilder {
	return dash.Heatmap(title, unit).
		Description(description).
		WithTarget(dash.PromHeatmapQuery(expr).Interval(window).RefId("A"))
}

func writeBytesSavedPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Write Bytes Saved Per "+window+" [proxy + executor]", dash.UnitBytes).
		Description("Bytes saved by chunk deduplication per window interval.").
		GridPos(grid(9, 16, 0, 1)).
		WithTarget(q(writeBytesSavedWindow, "Bytes Saved").RefId("A")).
		WithTarget(q(proxyWriteBytesSavedWindow, "Proxy Bytes Saved").RefId("B")).
		WithTarget(q(executorWriteBytesSavedWindow, "Executor Bytes Saved").RefId("C"))
}

func last7DaysPanel() *stat.PanelBuilder {
	return fixedColorStat(statPanel(
		"Last 7 Days",
		"Total bytes saved by deduplication in the last 7 days",
		dash.UnitBytes,
		writeBytesSaved7d,
		"Last 7 Days",
	), "green").GridPos(grid(6, 8, 16, 1))
}

func last30DaysPanel() *stat.PanelBuilder {
	return fixedColorStat(statPanel(
		"Last 30d",
		"Total bytes saved by deduplication in the last 30 days",
		dash.UnitBytes,
		writeBytesSaved30d,
		"Last 30 Days",
	), "green").GridPos(grid(3, 3, 16, 7))
}

func previousWeekPanel() *stat.PanelBuilder {
	return fixedColorStat(statPanel(
		"Prev Week",
		"Total bytes saved by deduplication in the previous 7-day period",
		dash.UnitBytes,
		writeBytesSavedPrev7d,
		"Previous Week",
	), "blue").GridPos(grid(3, 3, 19, 7))
}

func wowPanel() *stat.PanelBuilder {
	zero := 0.0
	return statPanel(
		"WoW",
		"Week-over-week change in bytes saved by deduplication",
		dash.UnitPercent,
		`((`+writeBytesSaved7d+`) - (`+writeBytesSavedPrev7d+`)) / (`+writeBytesSavedPrev7d+`) * 100`,
		"WoW Change",
	).GridPos(grid(3, 2, 22, 7)).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdThresholds)).
		Thresholds(
			dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{
					{Color: "red"},
					{Color: "green", Value: &zero},
				}),
		)
}

func readThroughputPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Read Throughput (Bytes/s) [proxy]", dash.UnitBytesPerSec).
		Description("Read throughput comparing chunked vs non-chunked requests").
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_read_bytes{`+proxyFilter+`, chunked="true"}[`+window+`]))`, "chunked").RefId("A")).
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_read_bytes{`+proxyFilter+`, chunked!="true"}[`+window+`]))`, "non-chunked").RefId("B"))
}

func writeThroughputPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Write Throughput (Bytes/s) [proxy]", dash.UnitBytesPerSec).
		Description("Write throughput comparing chunked vs non-chunked requests").
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_write_bytes{`+proxyFilter+`, chunked="true"}[`+window+`]))`, "chunked").RefId("A")).
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_write_bytes{`+proxyFilter+`, chunked!="true"}[`+window+`]))`, "non-chunked").RefId("B"))
}

func splitBlobAPICallsPanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("SplitBlob API Calls by Response Code [server-side]", dash.UnitOps).
		Description("SplitBlob API calls by gRPC response code (OK, NotFound, etc)").
		GridPos(grid(8, 12, 0, 10)).
		WithTarget(q(`sum by (grpc_code) (rate(grpc_server_handled_total{`+appFilter+`, grpc_method="SplitBlob"}[`+window+`]))`, "{{grpc_code}}").RefId("A"))
}

func spliceBlobAPICallsPanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("SpliceBlob API Calls by Response Code [server-side]", dash.UnitOps).
		Description("SpliceBlob API calls by gRPC response code (OK, NotFound, etc)").
		GridPos(grid(8, 12, 12, 10)).
		WithTarget(q(`sum by (grpc_code) (rate(grpc_server_handled_total{`+appFilter+`, grpc_method="SpliceBlob"}[`+window+`]))`, "{{grpc_code}}").RefId("A"))
}

func splitBlobDurationPanel() *heatmap.PanelBuilder {
	return heatmapPanel(
		"SplitBlob gRPC Server Handling Duration Heatmap",
		"Histogram heatmap of gRPC server handling duration for /build.bazel.remote.execution.v2.ContentAddressableStorage/SplitBlob requests",
		dash.UnitSeconds,
		`sum by (le) (rate(grpc_server_handling_seconds_bucket{`+appFilter+`, grpc_service="build.bazel.remote.execution.v2.ContentAddressableStorage", grpc_method="SplitBlob"}[`+window+`]))`,
	)
}

func spliceBlobDurationPanel() *heatmap.PanelBuilder {
	return heatmapPanel(
		"SpliceBlob Duration Heatmap",
		"Histogram heatmap of SpliceBlob RPC duration (custom metric with buckets up to 10min)",
		dash.UnitMicroseconds,
		`sum by (le) (rate(buildbuddy_remote_cache_splice_blob_duration_usec_bucket{`+appFilter+`}[`+window+`]))`,
	)
}

func writeDeduplicationRatioPanel() *timeseries.PanelBuilder {
	dedupedBytes := sumRateByName("buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped|buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_deduped")
	totalBytes := sumRateByName("buildbuddy_cache_client_chunked_upload_chunk_bytes_total|buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_total")
	dedupedChunks := sumRateByName("buildbuddy_cache_client_chunked_upload_chunks_deduped|buildbuddy_proxy_byte_stream_chunked_write_chunks_deduped")
	totalChunks := sumRateByName("buildbuddy_cache_client_chunked_upload_chunks_total|buildbuddy_proxy_byte_stream_chunked_write_chunks_total")

	return dash.Timeseries("Write Deduplication Ratio [proxy + executor]", dash.UnitPercentUnit).
		Description("Percentage of chunk bytes and chunk count that were deduplicated during writes.").
		Min(0).
		Max(1).
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(dedupedBytes+` / `+totalBytes, "bytes dedup ratio").RefId("A")).
		WithTarget(q(dedupedChunks+` / `+totalChunks, "chunk count dedup ratio").RefId("B"))
}

func deduplicationSavingsPanel() *timeseries.PanelBuilder {
	proxyBytesSaved := sumRate("buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_deduped", proxyFilter)
	executorBytesSaved := sumRate("buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped", executorFilter)
	totalChunkBytes := sumRateByName("buildbuddy_cache_client_chunked_upload_chunk_bytes_total|buildbuddy_proxy_byte_stream_chunked_write_chunk_bytes_total")

	return dash.Timeseries("Deduplication Savings (Bytes/s) [proxy + executor]", dash.UnitBytesPerSec).
		Description("Bytes saved per second through chunk deduplication").
		WithTarget(q(proxyBytesSaved, "proxy bytes saved").RefId("A")).
		WithTarget(q(executorBytesSaved, "executor bytes saved").RefId("C")).
		WithTarget(q(totalChunkBytes, "total chunk bytes").RefId("B"))
}

func writeDedupRatioByGroupPanel() *timeseries.PanelBuilder {
	dedupedBytes := sumRateByNameAndLabels("group_id", "buildbuddy_cache_client_chunked_upload_chunk_bytes_deduped|buildbuddy_proxy_byte_stream_chunked_write_by_group_chunk_bytes_deduped")
	totalBytes := sumRateByNameAndLabels("group_id", "buildbuddy_cache_client_chunked_upload_chunk_bytes_total|buildbuddy_proxy_byte_stream_chunked_write_by_group_chunk_bytes_total")

	return dash.Timeseries("Write Dedup Ratio by Group [proxy + executor]", dash.UnitPercentUnit).
		Description("CDC write byte deduplication ratio by group ID, combining proxy-side and executor/client-side chunked uploads.").
		Min(0).
		Max(1).
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(dedupedBytes+` / `+totalBytes, "{{group_id}}").RefId("A"))
}

func writeUniqueChunksByGroupPanel() *timeseries.PanelBuilder {
	totalChunks := sumRateByNameAndLabels("group_id", "buildbuddy_cache_client_chunked_upload_chunks_total|buildbuddy_proxy_byte_stream_chunked_write_chunks_total")
	dedupedChunks := sumRateByNameAndLabels("group_id", "buildbuddy_cache_client_chunked_upload_chunks_deduped|buildbuddy_proxy_byte_stream_chunked_write_chunks_deduped")

	return dash.Timeseries("Unique Chunks by Group [proxy + executor]", "suffix:c/s").
		Description("Estimated GCS PUT pressure from unique CDC chunks by group ID, combining proxy-side and executor/client-side chunked uploads.").
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(`topk(20, (`+totalChunks+`) - (`+dedupedChunks+`))`, "{{group_id}} unique chunks/s").RefId("A"))
}

func writeDedupRatioByActionPanel() *timeseries.PanelBuilder {
	dedupedBytes := sumRateByNameAndLabels("action_mnemonic", "buildbuddy_cache_client_chunked_upload_by_action_mnemonic_chunk_bytes_deduped|buildbuddy_proxy_byte_stream_chunked_write_by_action_mnemonic_chunk_bytes_deduped")
	totalBytes := sumRateByNameAndLabels("action_mnemonic", "buildbuddy_cache_client_chunked_upload_by_action_mnemonic_chunk_bytes_total|buildbuddy_proxy_byte_stream_chunked_write_by_action_mnemonic_chunk_bytes_total")

	return dash.Timeseries("Write Dedup Ratio by Action [proxy + executor]", dash.UnitPercentUnit).
		Description("CDC write byte deduplication ratio by action mnemonic, combining proxy-side and executor/client-side chunked uploads.").
		Min(0).
		Max(1).
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(dedupedBytes+` / `+totalBytes, "{{action_mnemonic}}").RefId("A"))
}

func writeUniqueChunksByActionPanel() *timeseries.PanelBuilder {
	totalChunks := sumRateByNameAndLabels("action_mnemonic", "buildbuddy_cache_client_chunked_upload_by_action_mnemonic_chunks_total|buildbuddy_proxy_byte_stream_chunked_write_by_action_mnemonic_chunks_total")
	dedupedChunks := sumRateByNameAndLabels("action_mnemonic", "buildbuddy_cache_client_chunked_upload_by_action_mnemonic_chunks_deduped|buildbuddy_proxy_byte_stream_chunked_write_by_action_mnemonic_chunks_deduped")

	return dash.Timeseries("Unique Chunks by Action [proxy + executor]", "suffix:c/s").
		Description("Estimated GCS PUT pressure from unique CDC chunks by action mnemonic, combining proxy-side and executor/client-side chunked uploads.").
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(`topk(20, (`+totalChunks+`) - (`+dedupedChunks+`))`, "{{action_mnemonic}} unique chunks/s").RefId("A"))
}

func chunkReadHitRatiosPanel() *timeseries.PanelBuilder {
	proxyLocalChunks := sumRate("buildbuddy_proxy_byte_stream_chunked_read_chunks_local", proxyFilter)
	proxyTotalChunks := sumRate("buildbuddy_proxy_byte_stream_chunked_read_chunks_total", proxyFilter)
	fastPathAttempts := sumRate("buildbuddy_proxy_byte_stream_chunked_read_fast_path_attempts", proxyFilter)
	fastPathAttemptsByOutcome := sumRateBy("outcome", "buildbuddy_proxy_byte_stream_chunked_read_fast_path_attempts", proxyFilter)
	executorLocalChunks := sumRate("buildbuddy_cache_client_chunked_download_chunks_local", executorFilter)
	executorTotalChunks := sumRate("buildbuddy_cache_client_chunked_download_chunks_total", executorFilter)

	return dash.Timeseries("Chunk Read Hit Ratios [proxy + executor]", dash.UnitPercentUnit).
		Description("Proxy local chunk hit ratio, proxy fast-path outcome ratios, and executor cachetools chunk-location hit ratio.").
		Min(0).
		Max(1).
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(proxyLocalChunks+` / `+proxyTotalChunks, "proxy local chunk hit ratio").RefId("A")).
		WithTarget(q(fastPathAttemptsByOutcome+` / ignoring(outcome) group_left `+fastPathAttempts, "proxy fast path {{outcome}} ratio").RefId("B")).
		WithTarget(q(executorLocalChunks+` / `+executorTotalChunks, "executor chunk-location hit ratio").RefId("C"))
}

func chunkReadsBySourcePanel() *timeseries.PanelBuilder {
	proxyLocalChunks := sumRate("buildbuddy_proxy_byte_stream_chunked_read_chunks_local", proxyFilter)
	proxyRemoteChunks := sumRate("buildbuddy_proxy_byte_stream_chunked_read_chunks_remote", proxyFilter)
	executorLocalChunks := sumRate("buildbuddy_cache_client_chunked_download_chunks_local", executorFilter)
	executorTotalChunks := sumRate("buildbuddy_cache_client_chunked_download_chunks_total", executorFilter)

	return dash.StackedTimeseries("Chunk Reads by Source (chunks/s) [proxy + executor]", "suffix:c/s").
		Description("Chunks read from proxy local cache vs remote, plus executor cachetools local-file reuse vs remote CAS fallback.").
		WithTarget(q(proxyLocalChunks, "proxy local").RefId("A")).
		WithTarget(q(proxyRemoteChunks, "proxy remote").RefId("B")).
		WithTarget(q(executorLocalChunks, "executor local file").RefId("C")).
		WithTarget(q(executorTotalChunks+` - `+executorLocalChunks, "executor remote/fallback").RefId("D"))
}

func chunkReadBytesBySourcePanel() *timeseries.PanelBuilder {
	proxyLocalBytes := sumRate("buildbuddy_proxy_byte_stream_chunked_read_bytes_local", proxyFilter)
	proxyRemoteBytes := sumRate("buildbuddy_proxy_byte_stream_chunked_read_bytes_remote", proxyFilter)
	executorLocalBytes := sumRate("buildbuddy_cache_client_chunked_download_chunk_bytes_local", executorFilter)
	executorTotalBytes := sumRate("buildbuddy_cache_client_chunked_download_chunk_bytes_total", executorFilter)

	return dash.StackedTimeseries("Chunk Read Bytes by Source (Bytes/s) [proxy + executor]", dash.UnitBytesPerSec).
		Description("Bytes served from local cache/filecache vs fetched remotely for proxy chunked reads and executor cachetools chunked downloads.").
		WithTarget(q(proxyLocalBytes, "proxy local").RefId("A")).
		WithTarget(q(proxyRemoteBytes, "proxy remote").RefId("B")).
		WithTarget(q(executorLocalBytes, "executor local file").RefId("C")).
		WithTarget(q(executorTotalBytes+` - `+executorLocalBytes, "executor remote/fallback").RefId("D"))
}

func validationMarkerHitRatePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("SpliceBlob Validation Marker Hit Rate [app]", dash.UnitPercentUnit).
		Description("Share of chunked manifest Store() validations that found the shared validation marker and skipped re-hashing chunks.").
		Min(0).
		Max(1).
		LineWidth(2).
		FillOpacity(10).
		WithTarget(q(`sum(rate(buildbuddy_remote_cache_chunked_manifest_validation_count{`+appFilter+`, cache_status="hit"}[`+window+`])) / sum(rate(buildbuddy_remote_cache_chunked_manifest_validation_count{`+appFilter+`}[`+window+`]))`, "validation marker hit rate").RefId("A"))
}

func chunkedReadFailuresPanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("Chunked Read Failures by Reason [proxy + app]", dash.UnitOps).
		Description("CDC-specific read failure counters by reason/status; more actionable than broad ByteStream request status errors.").
		WithTarget(q(`sum by (reason, status) (rate(buildbuddy_proxy_byte_stream_chunked_read_failures{`+proxyFilter+`}[`+window+`]))`, "proxy {{reason}} ({{status}})").RefId("A")).
		WithTarget(q(`sum by (reason, status, offset_read) (rate(buildbuddy_remote_cache_byte_stream_chunked_read_failures{`+appFilter+`}[`+window+`]))`, "app {{reason}} ({{status}}, offset={{offset_read}})").RefId("B"))
}

func chunkedWriteTotalDurationPanel() *heatmap.PanelBuilder {
	return heatmapPanel(
		"Chunked Write Total Duration",
		"Heatmap of total chunked write duration (chunking + remote phases)",
		dash.UnitMicroseconds,
		`sum by (le) (rate(buildbuddy_proxy_byte_stream_chunked_write_duration_usec_bucket{`+proxyFilter+`}[`+window+`]))`,
	)
}

func averageChunkedWriteDurationPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Average Chunked Write Duration", dash.UnitMicroseconds).
		Description("Average duration of each chunked write phase").
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_chunked_write_duration_usec_sum{`+proxyFilter+`}[`+window+`])) / sum(rate(buildbuddy_proxy_byte_stream_chunked_write_duration_usec_count{`+proxyFilter+`}[`+window+`]))`, "total").RefId("A")).
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_chunked_write_chunking_duration_usec_sum{`+proxyFilter+`}[`+window+`])) / sum(rate(buildbuddy_proxy_byte_stream_chunked_write_chunking_duration_usec_count{`+proxyFilter+`}[`+window+`]))`, "chunking phase").RefId("B")).
		WithTarget(q(`sum(rate(buildbuddy_proxy_byte_stream_chunked_write_remote_duration_usec_sum{`+proxyFilter+`}[`+window+`])) / sum(rate(buildbuddy_proxy_byte_stream_chunked_write_remote_duration_usec_count{`+proxyFilter+`}[`+window+`]))`, "remote phase").RefId("C"))
}

func chunkedWriteChunkingDurationPanel() *heatmap.PanelBuilder {
	return heatmapPanel(
		"Chunked Write Chunking Phase Duration",
		"Heatmap of chunking phase duration (receive, decompress, chunk, compress, local write)",
		dash.UnitMicroseconds,
		`sum by (le) (rate(buildbuddy_proxy_byte_stream_chunked_write_chunking_duration_usec_bucket{`+proxyFilter+`}[`+window+`]))`,
	)
}

func chunkedWriteRemoteDurationPanel() *heatmap.PanelBuilder {
	return heatmapPanel(
		"Chunked Write Remote Phase Duration",
		"Heatmap of remote phase duration (FindMissingBlobs, upload, SpliceBlob)",
		dash.UnitMicroseconds,
		`sum by (le) (rate(buildbuddy_proxy_byte_stream_chunked_write_remote_duration_usec_bucket{`+proxyFilter+`}[`+window+`]))`,
	)
}

func localCacheMaintenancePanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("Proxy Chunked Read Local Cache Maintenance [proxy]", dash.UnitOps).
		Description("Local manifest store attempts and failures writing remotely fetched chunks back to proxy local cache.").
		WithTarget(q(`sum by (status) (rate(buildbuddy_proxy_byte_stream_chunked_read_local_manifest_store_attempts{`+proxyFilter+`}[`+window+`]))`, "manifest store {{status}}").RefId("A")).
		WithTarget(q(`sum by (status) (rate(buildbuddy_proxy_byte_stream_chunked_read_local_write_back_failures{`+proxyFilter+`}[`+window+`]))`, "write-back failure {{status}}").RefId("B"))
}

func manifestLoadsPanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("Manifest Loads by Key Scheme", dash.UnitOps).
		Description("Manifest loads by AC key prefix (current vs legacy scheme)").
		WithTarget(q(`sum by (prefix) (rate(buildbuddy_remote_cache_chunked_manifest_load_count{`+appFilter+`}[`+window+`]))`, "{{prefix}}").RefId("A"))
}

func regionVariable(name, label string) *dashboard.QueryVariableBuilder {
	query := `label_values(up, region)`
	return dash.QueryVar(name, query).
		Label(label).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		AllValue(".*").
		Definition(query)
}

func windowVariable() *dashboard.CustomVariableBuilder {
	values := "30s, 1m, 5m, 10m, 15m, 30m, 1h, 6h, 12h, 1d"
	return dashboard.NewCustomVariableBuilder("window").
		Label("Averaging Window").
		Values(dashboard.StringOrMap{String: &values}).
		Current(dash.SelectedOption("1h", "1h"))
}

func build() (dashboard.Dashboard, error) {
	const collapsedRowsStartY uint32 = 18

	return dashboard.NewDashboardBuilder("CDC Metrics").
		Uid("cdc-metrics").
		Tags([]string{"generated", "file:cdc.json"}).
		Editable().
		Refresh("1m").
		Time("now-7d", "now").
		Tooltip(dashboard.DashboardCursorSyncCrosshair).
		Timepicker(dashboard.NewTimePickerBuilder()).
		WithVariable(regionVariable("app_region", "App Region")).
		WithVariable(regionVariable("proxy_region", "Proxy / Executor Region")).
		WithVariable(windowVariable()).
		WithRow(rowAt("Summary", 0)).
		WithPanel(writeBytesSavedPanel()).
		WithPanel(last7DaysPanel()).
		WithPanel(last30DaysPanel()).
		WithPanel(previousWeekPanel()).
		WithPanel(wowPanel()).
		WithPanel(splitBlobAPICallsPanel()).
		WithPanel(spliceBlobAPICallsPanel()).
		WithRow(rowAt("Proxy traffic", collapsedRowsStartY).
			WithPanel(readThroughputPanel()).
			WithPanel(writeThroughputPanel()).
			WithPanel(chunkReadBytesBySourcePanel()).
			WithPanel(chunkReadsBySourcePanel())).
		WithRow(rowAt("Server APIs", collapsedRowsStartY+1).
			WithPanel(splitBlobDurationPanel()).
			WithPanel(spliceBlobDurationPanel())).
		WithRow(rowAt("Deduplication", collapsedRowsStartY+2).
			WithPanel(writeDeduplicationRatioPanel()).
			WithPanel(deduplicationSavingsPanel()).
			WithPanel(writeDedupRatioByGroupPanel()).
			WithPanel(writeUniqueChunksByGroupPanel()).
			WithPanel(writeDedupRatioByActionPanel()).
			WithPanel(writeUniqueChunksByActionPanel()).
			WithPanel(chunkReadHitRatiosPanel()).
			WithPanel(validationMarkerHitRatePanel())).
		WithRow(rowAt("Write latency", collapsedRowsStartY+3).
			WithPanel(chunkedWriteTotalDurationPanel()).
			WithPanel(averageChunkedWriteDurationPanel()).
			WithPanel(chunkedWriteChunkingDurationPanel()).
			WithPanel(chunkedWriteRemoteDurationPanel())).
		WithRow(rowAt("Failures and manifests", collapsedRowsStartY+4).
			WithPanel(chunkedReadFailuresPanel()).
			WithPanel(localCacheMaintenancePanel()).
			WithPanel(manifestLoadsPanel())).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
