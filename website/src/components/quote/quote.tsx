import React from "react";
import common from "../../css/common.module.css";
import styles from "./quote.module.css";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionDark} ${common.sectionRounded}`}>
      <div className={`${common.container} ${styles.stats}`}>
        <div className={styles.stat}>
          <div className={styles.statNumber}>15.2x</div>
          <div className={styles.statTitle}>faster builds</div>
        </div>
        <div className={styles.stat}>
          <div className={styles.statNumber}>16.5x</div>
          <div className={styles.statTitle}>faster tests</div>
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
              Principal Engineer at Lyft & Lead Maintainer of Bazel's Apple rules
            </div>
          </div>
        </div>
      </div>
      <div className={styles.logos}>
        <img alt="Lyft Logo" className={styles.logo} width="70px" height="50px" src="/img/lyft-white.svg" />
      </div>
    </div>
  );
}

export default Component;
