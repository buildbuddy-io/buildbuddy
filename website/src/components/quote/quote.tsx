import React from "react";
import common from "../../css/common.module.css";
import styles from "./quote.module.css";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionDark} ${common.sectionRounded}`}>
      <div>
        <div className={`${common.container} ${styles.stats}`}>
          <div className={styles.stat}>
            <div className={styles.statNumber}></div>
            <div className={styles.statTitle}></div>
          </div>
        </div>
        <div className={common.container}>
          <div className={common.centeredText}>
            <h2 className={styles.quote}>
              “BuildBuddy delivers the remote cache and remote execution performance we need without the maintenance
              burden. All while providing best in class visibility into our builds.”
            </h2>
            <div className={styles.attribution}>
              <div className={styles.attributionName}>Keith Smiley</div>
              <div className={styles.attributionDescription}>
                Senior Staff Engineer at Modular & Lead Maintainer of Bazel's Apple rules
              </div>
            </div>
          </div>
        </div>
        <div className={styles.logos}>
          <img alt="Modular Logo" className={styles.logo} width="120px" height="50px" src="/img/modular-white.svg" />
        </div>
      </div>
    </div>
  );
}

export default Component;
