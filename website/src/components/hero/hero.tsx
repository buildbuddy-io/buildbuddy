import React from "react";
import styles from "./hero.module.css";
import common from "../../css/common.module.css";
import Image from "@theme/IdealImage";

function Component() {
  return (
    <div className={common.section}>
      <div className={`${common.container} ${common.splitContainer}`}>
        <div className={common.text}>
          <h1 className={common.title}>
            Faster builds. <br /> Happier developers.
          </h1>
          <div className={common.subtitle}>
            BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to build
            and test software 10x faster.
          </div>
          <div className={styles.buttons}>
            <a href="https://app.buildbuddy.io" className={`${common.button} ${common.buttonPrimary}`}>
              Get Started for Free
            </a>
            <a href="/request-demo" className={common.button}>
              Request a Demo
            </a>
          </div>
        </div>
        <div className={styles.image}>
          <Image alt="BuildBuddy Enterprise Bazel Results UI" img={require("../../../static/img/hero.png")} />
        </div>
      </div>
    </div>
  );
}

export default Component;
