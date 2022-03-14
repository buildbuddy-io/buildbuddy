import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";

function Component() {
  return (
    <Layout title="Bazel at Enterprise Scale">
      <div className={common.page}>
        <Hero
          title="Build & Test UI"
          subtitle="Get visibility into your builds and test results. Share links to your local builds with your co-workers and debug together."
          image={require("../../static/img/blog/build_logs.png")}
          flipped={true}
          bigImage={true}
          lessPadding={true}
        />
        <Hero
          title="Timing profile"
          subtitle="With the in-product timing profile you can dive deep into where time is being spent for each and every build — with just one click."
          image={require("../../static/img/ui.png")}
          bigImage={true}
        />
        <Hero
          title="Cache stats"
          subtitle="Debug cache misses and monitor cache performance. Examine individual invocations, or track cache hit ratios across builds."
          image={require("../../static/img/blog/cache_misses.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Trends"
          subtitle="Track how your build performance is changing over time. Catch performance regressions and validate performance optimizations."
          image={require("../../static/img/blog/trends.png")}
          bigImage={true}
        />
        <Hero
          title="Action explorer"
          subtitle="View input files, command details, environment variables, timing information, output files, and more for each individual action."
          image={require("../../static/img/action-explorer.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Invocation diffing"
          subtitle={`Automatically compare two Bazel invocations to see what changed. Find out why the build "works on my machine".`}
          image={require("../../static/img/blog/compare.png")}
          bigImage={true}
        />
        <Hero
          title="Dependency graph"
          subtitle="Visualize the dependencies between your targets using Bazel's powerful query tool."
          image={require("../../static/img/blog/query_graph.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Team build history"
          subtitle="View builds across your organization. Filter by date range, user, repo, branch, commit, host, CI, and more."
          image={require("../../static/img/blog/date_picker.png")}
          bigImage={true}
        />
        <Hero
          title="Downloadable artifacts"
          subtitle="Download the outputs of your builds from the remote cache with just a single click."
          image={require("../../static/img/artifacts.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Test grid"
          subtitle="Quickly find and fix flaky tests. Click on past failures to see test logs from failed runs to identify patterns."
          image={require("../../static/img/test-grid.png")}
          bigImage={true}
        />
        <Hero
          title="Suggestions"
          subtitle="Get relevant suggestions on how to improve your builds surfaced inline — right when you need them."
          image={require("../../static/img/blog/suggested-fixes.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Live updating"
          subtitle="See the current state of your builds and tests in real-time, whether they're local or on CI."
          image={require("../../static/img/live.png")}
          bigImage={true}
        />
        <Hero
          title="Test log parsing"
          subtitle="Test outputs are parsed and rendered in an easy to navigate UI, with any failures bubbled to the top."
          image={require("../../static/img/test-parsing.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Organization management"
          subtitle="Create multiple organizations and manage members, roles, API keys, and more. SSO — check, SAML — check, OIDC — check."
          image={require("../../static/img/blog/settings.png")}
          bigImage={true}
        />
        <Hero
          title="Live remote execution view"
          subtitle="Watch remotely executing actions live, as they're happening."
          image={require("../../static/img/execution.png")}
          bigImage={true}
          flipped={true}
        />
        <CTA title="And much more!" />
      </div>
    </Layout>
  );
}

export default Component;
