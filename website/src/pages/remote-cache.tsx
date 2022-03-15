import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";
import Globe from "../components/globe/globe";

function Component() {
  return (
    <Layout title="Bazel at Enterprise Scale">
      <div className={common.page}>
        <Hero
          title="Remote Cache"
          subtitle="Global remote caching infrastructure made easy. Highly scalable, blazing fast, and incredibly simple to setup."
          component={<Globe />}
          lessPadding={true}
          flipped={true}
        />
        <Hero
          title="Globally distributed"
          subtitle="BuildBuddy Cloud cache has 61 points of presence around the world to minimize ping and maximize throughput."
          image={require("../../static/img/ping.png")}
          bigImage={true}
        />
        <Hero
          title="Cache stats"
          subtitle="Tools to track and monitor your cache hit ratios over time. Catch performance regressions and validate performance optimizations."
          image={require("../../static/img/hit-tracking.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Build without the bytes support"
          subtitle="Designed to support Bazel's remote_download_minimal flag to eliminate the need to download of intermediate artifacts."
          image={require("../../static/img/minimal.png")}
          bigImage={true}
        />
        <Hero
          title="Cache hit debugging"
          subtitle="Dive into invocations to see individual cache misses and which targets they're associated with, view input files, and more."
          image={require("../../static/img/cache.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Compression support"
          subtitle="Support for compressed blobs brings a 2x speed-up in network performance over caches that don't support it."
          image={require("../../static/img/compression.png")}
          bigImage={true}
        />
        <Hero
          title="High availability"
          subtitle="Redundant storage, multi-zone deployments, and zero downtime rollouts allow us to offer unmatched SLAs."
          image={require("../../static/img/ha.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Low latency"
          subtitle="Custom caching infrastucture allows us to serve cache artifacts with 2ms average read latencies, compared to ~100ms with S3."
          image={require("../../static/img/latency.png")}
          bigImage={true}
        />
        <Hero
          title="High throughput"
          subtitle="Production clusters easily handle 80k+ QPS with read throughputs of over 700 Mbps per artifact."
          image={require("../../static/img/download-speed.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="gRPC protocol"
          subtitle="gRPC protocool enables connection multiplexing, timing profile & test log uploading, and more."
          image={require("../../static/img/test-parsing.png")}
          bigImage={true}
        />
        <Hero
          title="Vertically and horizontally scalable"
          subtitle="Scale each individual cache node to billions of files and dozens of terrabytes — or just add more cache nodes to scale with ease."
          image={require("../../static/img/scale.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Read-only api keys"
          subtitle="Control who has read/write permissions to your cache with read-only API keys."
          image={require("../../static/img/read-only.png")}
          bigImage={true}
        />
        <Hero
          title="Partition support"
          subtitle="Partition your cache to prevent high-churn projects from evicting the cache of projects with less volume."
          image={require("../../static/img/partition.png")}
          bigImage={true}
          flipped={true}
        />
        <CTA title="And much more!" />
      </div>
    </Layout>
  );
}

export default Component;
