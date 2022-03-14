import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";
import Terminal from "../components/terminal/terminal";

function Component() {
  return (
    <Layout title="Bazel at Enterprise Scale">
      <div className={common.page}>
        <Hero
          title="Remote Execution"
          subtitle="Automatically parallelize your build actions and test runs across thousands of cores."
          component={<Terminal />}
          bigImage={true}
        />
        <Hero
          title="Simple configuration"
          subtitle="Just copy a few lines to your .bazelrc, and you're off to the races."
          image={require("../../static/img/setup.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Auto-scaling"
          subtitle="Scale your executor cluster up and down auto-magically based on load using custom metrics."
          image={require("../../static/img/blog/autoscaling-prometheus.png")}
          bigImage={true}
        />
        <Hero
          title="Custom docker images"
          subtitle="Pick your distro, install the tools you need. All you need to do is specify your own custom docker image."
          image={require("../../static/img/platforms.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Pre-emptible machines"
          subtitle="Our executors were designed from the ground up to run on preemptible machines. This means you can take advantage of Spot VM pricing."
          image={require("../../static/img/preemptible.png")}
          bigImage={true}
        />
        <Hero
          title="Remote persistent workers"
          subtitle="Need a warm JVM? Our executors support remote persistent workers to speed up JIT compilation."
          image={require("../../static/img/ui.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Bring your own executors"
          subtitle="Bringing your own executors gives you all of the advantages of BuildBuddy Cloud while still using your own hardware."
          image={require("../../static/img/byor.png")}
          bigImage={true}
        />
        <Hero
          title="Live action view"
          subtitle="Watch remotely executing actions live, as they're happening."
          image={require("../../static/img/execution.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Action explorer"
          subtitle="View input files, command details, environment variables, timing information, output files, and more for each individual action."
          image={require("../../static/img/action-explorer.png")}
          bigImage={true}
        />
        <Hero
          title="Action timeline"
          subtitle="Dive into the timing breakdown of individual actions to see where the time is being spent."
          image={require("../../static/img/action-timeline.png")}
          bigImage={true}
          flipped={true}
        />
        <Hero
          title="Mac executors"
          subtitle="Intel and M1 executor binaries allow you to run Mac-native executors for iOS builds and more."
          image={require("../../static/img/mac-executors.png")}
          bigImage={true}
        />
        <CTA title="And much more!" />
      </div>
    </Layout>
  );
}

export default Component;
