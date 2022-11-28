import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";
import Terminal, { Prompt } from "../components/terminal/terminal";

function Index() {
  return (
    <Layout title="Bazel at Enterprise Scale">
      <div className={common.page}>
        <Hero
          title="Build & Test UI"
          subtitle="Get visibility into your build and test performance. Share invocation links with co-workers and debug together."
          image={require("../../static/img/ui.png")}
          secondaryButtonText=""
          primaryButtonText="Learn more about the Build & Test UI"
          primaryButtonHref="/ui"
          bigImage={true}
          peekMore={true}
          flipped={true}
        />
        <Hero
          title="Remote Execution"
          subtitle="Automatically parallelize your build actions and test runs across thousands of cores."
          component={<Terminal />}
          primaryButtonText="Learn more about Remote Execution"
          primaryButtonHref="/remote-execution"
          secondaryButtonText=""
          bigImage={true}
        />
        <Hero
          title="Remote Cache"
          subtitle="Global remote caching infrastructure made easy. Highly scalable, blazing fast, and incredibly simple to setup."
          image={require("../../static/img/globe.png")}
          secondaryButtonText=""
          primaryButtonText="Learn more about Remote Caching"
          primaryButtonHref="/remote-cache"
          lessPadding={true}
          flipped={true}
        />
        <Hero
          title="BuildBuddy CLI"
          subtitle="Take BuildBuddy to the command line &mdash; built on top of Bazelisk with support for plugins, authentication, flaky network conditions, and so much more."
          component={
            <Terminal
              contents={
                <>
                  <Prompt />
                  bb build //...
                </>
              }
            />
          }
          secondaryButtonText=""
          primaryButtonText="Learn more about BuildBuddy CLI"
          primaryButtonHref="/cli"
          bigImage={true}
          flipped={false}
        />
        <Hero
          title="Workflows"
          subtitle="A CI system that's designed and built just for Bazel. Always hit a warm Bazel instance and enjoy CI runs that finish in seconds."
          image={require("../../static/img/workflow.png")}
          secondaryButtonText=""
          primaryButtonText="Learn more about Workflows"
          primaryButtonHref="/workflows"
          flipped={true}
        />
        <Hero
          title="Plugin Library"
          subtitle="Add functionality and customize your BuildBuddy CLI experience with plugins built by our incredible developer community."
          image={require("../../static/img/plugins.png")}
          secondaryButtonText=""
          primaryButtonText="View the Plugin Library"
          primaryButtonHref="/plugins"
          bigImage={true}
          peekMore={true}
          flipped={false}
        />
        <CTA />
      </div>
    </Layout>
  );
}

export default Index;
