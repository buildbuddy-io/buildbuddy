import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";
import Terminal from "../components/terminal/terminal";
import Globe from "../components/globe/globe";

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
          component={<Globe />}
          secondaryButtonText=""
          primaryButtonText="Learn more about Remote Caching"
          primaryButtonHref="/remote-cache"
          lessPadding={true}
          flipped={true}
        />
        <CTA />
      </div>
    </Layout>
  );
}

export default Index;
