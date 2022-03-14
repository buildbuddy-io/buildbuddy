import React from "react";
import styles from "./logs.module.css";
import common from "../../css/common.module.css";
import Image from "@theme/IdealImage";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionGray}`}>
      <div className={`${common.container} ${common.splitContainer}`}>
        <div className={common.text}>
          <h2 className={common.title}>
            Unlock your
            <br /> Bazel build logs
          </h2>
          <div className={common.subtitle}>
            BuildBuddy captures build logs, test logs, invocation details, target information, and artifacts — so you
            can dive deep into and easily share the details of each and every build.
          </div>
        </div>
        <Image
          alt="BuildBuddy Bazel build and test logs"
          className={styles.image}
          img={require("../../../static/img/logs.png")}
          shouldAutoDownload={() => true}
          threshold={10000}
        />
      </div>
    </div>
  );
}

export default Component;
