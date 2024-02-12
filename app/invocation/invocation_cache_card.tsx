import React from "react";
import InvocationModel from "./invocation_model";
import format from "../format/format";
import { PieChart as PieChartIcon, AlertCircle as AlertCircleIcon } from "lucide-react";
import { ResponsiveContainer, PieChart, Pie, Cell } from "recharts";
import capabilities from "../capabilities/capabilities";
import { getChartColor } from "../util/color";
import { blaze } from "../../proto/action_cache_ts_proto";

interface Props {
  model: InvocationModel;
}

const BITS_PER_BYTE = 8;

export default class CacheCardComponent extends React.Component<Props> {
  render() {
    const hasCacheStats =
      this.props.model.cacheStats.length &&
      (+this.props.model.cacheStats[0]?.actionCacheHits !== 0 ||
        +this.props.model.cacheStats[0]?.actionCacheMisses !== 0 ||
        +this.props.model.cacheStats[0]?.totalDownloadSizeBytes !== 0 ||
        +this.props.model.cacheStats[0]?.totalUploadSizeBytes !== 0);
    return (
      <div className="card">
        <PieChartIcon className="icon" />
        <div className="content">
          <div className="title">Cache stats</div>
          {!hasCacheStats && (
            <div className="no-cache-stats">Cache stats only available when using BuildBuddy cache.</div>
          )}
          {Boolean(this.props.model.cacheStats.length) && (
            <div className="details">
              {hasCacheStats && !this.props.model.hasCacheWriteCapability() && (
                <div className="cache-details">
                  <AlertCircleIcon className="icon" />
                  This invocation was created with a read-only API key. No artifacts were written to the cache.
                </div>
              )}
              {this.props.model.cacheStats.map((cacheStat) => {
                const downloadThroughput = BITS_PER_BYTE * (+cacheStat.downloadThroughputBytesPerSecond / 1000000);
                const uploadThroughput = BITS_PER_BYTE * (+cacheStat.uploadThroughputBytesPerSecond / 1000000);

                const renderExecTime =
                  +cacheStat.totalCachedActionExecUsec > 0 || +cacheStat.totalUncachedActionExecUsec > 0;
                const cachedExecTime = format.durationUsec(cacheStat.totalCachedActionExecUsec);
                const uncachedExecTime = format.durationUsec(cacheStat.totalUncachedActionExecUsec);
                return (
                  <div debug-id="cache-sections" className="cache-sections">
                    <div className="cache-section">
                      <div className="cache-title">Action cache (AC)</div>
                      <div className="cache-subtitle">Maps action hashes to action result metadata</div>
                      <div className="cache-chart">
                        {this.drawChart(+cacheStat.actionCacheHits, "#4CAF50", +cacheStat.actionCacheMisses, "#f44336")}
                        <div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-hit-color-swatch"></span>
                            <span className="cache-stat">{format.formatWithCommas(cacheStat.actionCacheHits)}</span>
                            &nbsp;hits
                          </div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-miss-color-swatch"></span>
                            <span className="cache-stat">{format.formatWithCommas(cacheStat.actionCacheMisses)}</span>
                            &nbsp;misses
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="cache-section">
                      <div className="cache-title">Content addressable store (CAS)</div>
                      <div className="cache-subtitle">Stores files (including logs & profiling info)</div>
                      <div className="cache-chart">
                        {this.drawChart(+cacheStat.casCacheHits, "#4CAF50", +cacheStat.casCacheUploads, "#f44336")}
                        <div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-hit-color-swatch"></span>
                            <span className="cache-stat">{format.formatWithCommas(cacheStat.casCacheHits)}</span>
                            &nbsp;hits
                          </div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-miss-color-swatch"></span>
                            <span className="cache-stat">{format.formatWithCommas(cacheStat.casCacheUploads)}</span>
                            &nbsp;writes
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="cache-section">
                      <div className="cache-title">Volume</div>
                      <div className="cache-subtitle">Total size of files uploaded & downloaded from both caches</div>
                      <div className="cache-chart">
                        {this.drawChart(
                          +cacheStat.totalDownloadSizeBytes,
                          "#03A9F4",
                          +cacheStat.totalUploadSizeBytes,
                          "#3F51B5"
                        )}
                        <div>
                          {this.renderVolumeChartLabel(
                            "downloaded",
                            "download-color-swatch",
                            Number(cacheStat.totalDownloadSizeBytes),
                            Number(cacheStat.totalDownloadTransferredSizeBytes)
                          )}
                          {this.renderVolumeChartLabel(
                            "uploaded",
                            "upload-color-swatch",
                            Number(cacheStat.totalUploadSizeBytes),
                            Number(cacheStat.totalUploadTransferredSizeBytes)
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="cache-section">
                      <div className="cache-title">Throughput</div>
                      <div className="cache-subtitle">Upload / download speed (time-weighted avg)</div>
                      <div className="cache-chart">
                        {this.drawChart(downloadThroughput, "#03A9F4", uploadThroughput, "#3F51B5")}
                        <div>
                          <div className="cache-chart-label">
                            <span className="color-swatch download-color-swatch"></span>
                            <span className="cache-stat">{downloadThroughput.toFixed(2)}</span>&nbsp;Mbps download
                          </div>
                          <div className="cache-chart-label">
                            <span className="color-swatch upload-color-swatch"></span>
                            <span className="cache-stat">{uploadThroughput.toFixed(2)}</span>&nbsp;Mbps upload
                          </div>
                        </div>
                      </div>
                    </div>
                    {capabilities.config.trendsSummaryEnabled && renderExecTime && (
                      <div className="cache-section">
                        <div className="cache-title">CPU savings</div>
                        <div className="cache-subtitle">Cached CPU and actual CPU used</div>
                        <div className="cache-chart">
                          {this.drawChart(
                            +cacheStat.totalCachedActionExecUsec,
                            "#4CAF50",
                            +cacheStat.totalUncachedActionExecUsec,
                            "#f44336"
                          )}
                          <div>
                            <div className="cache-chart-label">
                              <span className="color-swatch cache-hit-color-swatch"></span>
                              <span className="cache-stat">{cachedExecTime}</span>&nbsp;not executed
                            </div>
                            <div className="cache-chart-label">
                              <span className="color-swatch cache-miss-color-swatch"></span>
                              <span className="cache-stat">{uncachedExecTime}</span>&nbsp;executed
                            </div>
                          </div>
                        </div>
                      </div>
                    )}

                    {Boolean(
                      this.props.model.buildMetrics?.actionSummary?.actionCacheStatistics?.missDetails.length
                    ) && (
                      <div className="cache-section">
                        <div className="cache-chart">
                          {renderBreakdown(
                            this.props.model.buildMetrics?.actionSummary?.actionCacheStatistics?.missDetails.map(
                              (d) => {
                                return {
                                  value: d.count,
                                  name: format.enumLabel(blaze.ActionCacheStatistics.MissReason[d.reason]),
                                };
                              }
                            ),
                            "Cache miss reasons",
                            "Reasons why actions missed cache"
                          )}
                        </div>
                      </div>
                    )}

                    {Boolean(this.props.model.buildMetrics?.actionSummary?.actionData.length) && (
                      <div className="cache-section">
                        <div className="cache-chart">
                          {renderBreakdown(
                            this.props.model.buildMetrics?.actionSummary?.actionData.map((d) => {
                              return {
                                value: d.actionsExecuted,
                                name: d.mnemonic,
                              };
                            }),
                            "Action types",
                            "Number of actions with each action mnemonic"
                          )}
                        </div>
                      </div>
                    )}

                    {Boolean(this.props.model.buildMetrics?.actionSummary?.runnerCount.length) && (
                      <div className="cache-section">
                        <div className="cache-chart">
                          {renderBreakdown(
                            this.props.model.buildMetrics?.actionSummary?.runnerCount
                              ?.filter((r) => r.name != "total")
                              .map((r) => {
                                return {
                                  value: r.count,
                                  name: r.name,
                                };
                              }),
                            "Runner types",
                            "Number of actions with each runner type"
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    );
  }

  renderVolumeChartLabel(label: string, swatch: string, sizeBytes: number, compressedSizeBytes: number) {
    const savings = 1 - compressedSizeBytes / sizeBytes;
    return (
      <div className="cache-chart-label">
        <span className={`color-swatch ${swatch}`}></span>
        <div>
          <div>
            <span className="cache-stat">{format.bytes(sizeBytes)}</span> {label}
          </div>
          {this.props.model.isCacheCompressionEnabled() && compressedSizeBytes ? (
            <div className="compressed-size">
              <span className="cache-stat">{format.bytes(compressedSizeBytes)}</span> compressed{" "}
              <span className={`size-savings ${savings < 0 ? "negative" : "positive"}`}>
                {savings < 0 && "+"}
                {(-savings * 100).toPrecision(3)}%
              </span>
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  drawChart(a: number, colorA: string, b: number, colorB: string) {
    if (a == 0 && b == 0) {
      colorA = "#eee";
      colorB = "#eee";
      a = 1;
    }

    let data = [
      { value: a, color: colorA },
      { value: b, color: colorB },
    ];

    return (
      <div className="cache-chart-container">
        <ResponsiveContainer>
          <PieChart>
            <Pie data={data} dataKey="value" outerRadius={40} innerRadius={20}>
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
      </div>
    );
  }
}

function renderBreakdown(data: any[] | undefined, title: string, subtitle: string) {
  data = data?.filter((d) => d.value > 0).sort((a, b) => b.value - a.value);

  let sum = data?.reduce(
    (prev, current) => {
      return { name: "Sum", value: prev.value + current.value };
    },
    { name: "Sum", value: 0 }
  );

  let cap = 5;
  let other = 0;
  let otherLabels: string[] = [];
  if (data && data?.length > cap) {
    for (let i = cap; i < data.length; i++) {
      other += data[i].value;
      otherLabels.push(
        `${format.formatWithCommas(data[i].value)} ${data[i].name} (${format.percent(data[i].value / sum.value)}%)`
      );
    }
  }

  data = data?.splice(0, cap);

  if (other > 0) {
    data?.push({ name: "Other", value: other });
  }

  return (
    <div className="cache-section">
      <div className="cache-title">{title}</div>
      <div className="cache-subtitle">{subtitle}</div>
      <div className="cache-chart">
        <ResponsiveContainer width={80} height={80}>
          <PieChart>
            <Pie data={data} dataKey="value" outerRadius={40} innerRadius={20}>
              {data?.map((_, index) => (
                <Cell key={`cell-${index}`} fill={getChartColor(index)} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        <div>
          {data?.map((entry, index) => (
            <div className="cache-chart-label">
              <span
                className="color-swatch cache-hit-color-swatch"
                style={{ backgroundColor: getChartColor(index) }}></span>
              <span className="cache-stat">
                <span className="cache-stat-duration">{format.formatWithCommas(entry.value)}</span>{" "}
                <span
                  className="cache-stat-description"
                  title={
                    other > 0 && index == cap
                      ? otherLabels.join(", ")
                      : `${entry.name} (${format.percent(entry.value / sum.value)}%)`
                  }>
                  {entry.name} ({format.percent(entry.value / sum.value)}%)
                </span>
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
