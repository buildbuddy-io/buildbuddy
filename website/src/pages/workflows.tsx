import Layout from "@theme/Layout";
import React from "react";
import common from "../css/common.module.css";

import CTA from "../components/cta/cta";
import Hero from "../components/hero/hero";

function Component() {
  return (
    <Layout title="BuildBuddy Workflows">
      <div className={common.page}>
        <Hero
          title="Workflows"
          subtitle="A CI system that's designed and built just for Bazel. Always hit a warm Bazel instance and enjoy CI runs that finish in seconds."
          image={require("../../static/img/workflow.png")}
          bigImage={true}
          lessPadding={true}
          primaryButtonHref="https://app.buildbuddy.io/workflows/"
          secondaryButtonText="Workflow Docs"
          secondaryButtonHref="/docs/workflows-setup"
          gradientButton={true}
        />

        <CTA title="Try Workflows today!" href="https://app.buildbuddy.io/workflows/" />
      </div>
    </Layout>
  );
}

export default Component;
