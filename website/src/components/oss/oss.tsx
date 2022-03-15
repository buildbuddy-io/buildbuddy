import React from "react";
import common from "../../css/common.module.css";
import styles from "./oss.module.css";
import OSSList from "./oss-list";
import Image from "@theme/IdealImage";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionDark} ${common.sectionRounded}`}>
      <div className={`${common.container} ${common.splitContainer} ${styles.ossContainer}`}>
        <div className={common.text}>
          <div className={styles.title}>
            BuildBuddy <span className={styles.heart}>&hearts;</span> Open Source
          </div>
          <div className={styles.subtitle}>
            BuildBuddy is{" "}
            <a target="_blank" className={styles.link} href="https://en.wikipedia.org/wiki/Open-core_model">
              open-core
            </a>{" "}
            and our features targeted at individual developers are available for free and MIT Licensed on{" "}
            <a target="_blank" className={styles.link} href="https://github.com/buildbuddy-io/buildbuddy">
              GitHub
            </a>
            . To show our appreciation to the open source community, we also offer BuildBuddy Cloud for free to
            individuals and Open Source projects.
          </div>
        </div>
        <Image
          className={styles.image}
          img={require("../../../static/img/oss/buildbuddy-io_buildbuddy.png")}
          shouldAutoDownload={() => true}
          threshold={10000}
        />
      </div>
      <hr className={styles.hr} />
      <div className={`${common.container} ${common.splitContainer} ${styles.ossContainer}`}>
        <div className={common.text}>
          <div className={styles.title}>Powered by BuildBuddy</div>
          <div className={styles.subtitle}>
            Here are some of our favorite open source repositories that are powered by BuildBuddy.
          </div>
        </div>
      </div>
      <div className={styles.ossList}>
        <OSSList length={6} />
      </div>
      <div className={common.container}>
        <a href="/open-source-repos" className={styles.repoButton}>
          See all repos
        </a>
      </div>
    </div>
  );
}

export default Component;
