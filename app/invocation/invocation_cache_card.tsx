import React from "react";
import InvocationModel from "./invocation_model";
import format from "../format/format";
import { PieChart as PieChartIcon, AlertCircle as AlertCircleIcon } from "lucide-react";
import { ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

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
          {this.props.model.cacheStats.length && (
            <div className="details">
              {hasCacheStats && !this.props.model.hasCacheWriteCapability() && (
                <div className="cache-details">
                  <AlertCircleIcon className="icon" />
                  This invocation was created with a read-only API key. No artifacts were written to the cache.
                </div>
              )}
              {this.props.model.cacheStats.map((cacheStat) => {
                let downloadThroughput = BITS_PER_BYTE * (+cacheStat.downloadThroughputBytesPerSecond / 1000000);
                let uploadThroughput = BITS_PER_BYTE * (+cacheStat.uploadThroughputBytesPerSecond / 1000000);
                return (
                  <div className="cache-sections">
                    <div className="cache-section">
                      <div className="cache-title">Action cache (AC)</div>
                      <div className="cache-subtitle">Maps action hashes to action result metadata</div>
                      <div className="cache-chart">
                        {this.drawChart(+cacheStat.actionCacheHits, "#4CAF50", +cacheStat.actionCacheMisses, "#f44336")}
                        <div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-hit-color-swatch"></span>
                            <span className="cache-stat">{cacheStat.actionCacheHits}</span> hits
                          </div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-miss-color-swatch"></span>
                            <span className="cache-stat">{cacheStat.actionCacheMisses}</span> misses
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
                            <span className="cache-stat">{cacheStat.casCacheHits}</span> hits
                          </div>
                          <div className="cache-chart-label">
                            <span className="color-swatch cache-miss-color-swatch"></span>
                            <span className="cache-stat">{cacheStat.casCacheUploads}</span> writes
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
                          "#607D8B"
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
                        {this.drawChart(downloadThroughput, "#03A9F4", uploadThroughput, "#607D8B")}
                        <div>
                          <div className="cache-chart-label">
                            <span className="color-swatch download-color-swatch"></span>
                            <span className="cache-stat">{downloadThroughput.toFixed(2)}</span> Mbps download
                          </div>
                          <div className="cache-chart-label">
                            <span className="color-swatch upload-color-swatch"></span>
                            <span className="cache-stat">{uploadThroughput.toFixed(2)}</span> Mbps upload
                          </div>
                        </div>
                      </div>
                    </div>
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
