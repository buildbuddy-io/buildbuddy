import React from "react";
import styles from "./enterprise.module.css";
import common from "../../css/common.module.css";
import { Eye, Users, Zap } from "lucide-react";

function Component() {
  return (
    <div className={common.section}>
      <div className={common.container}>
        <div className={common.centeredText}>
          <h2 className={common.title}>BuildBuddy is enterprise Bazel</h2>
          <div className={common.subtitle}>
            BuildBuddy brings the power of Bazel to your organization, along with mission-critical features,
            enterprise-grade stability, and 24/7 expert support.
          </div>
        </div>
      </div>
      <div className={common.container}>
        <div className={styles.features}>
          <div className={styles.feature}>
            <div className={styles.featureIcon}>
              <Eye />
            </div>
            <div className={styles.featureTitle}>Visibility</div>
            <div className={styles.featureDescription}>
              The BuildBuddy Results Store and Results UI gives you full visibility into your builds so you can debug
              failing tests, view invocation details, and track down slow targets.
            </div>
          </div>
          <div className={styles.feature}>
            <div className={styles.featureIcon}>
              <Zap />
            </div>
            <div className={styles.featureTitle}>Performance</div>
            <div className={styles.featureDescription}>
              With a built-in Remote Build Cache and Remote Build Execution, BuildBuddy gives you the tools to make your
              whole engineering organization more productive.
            </div>
          </div>
          <div className={styles.feature}>
            <div className={styles.featureIcon}>
              <Users />
            </div>
            <div className={styles.featureTitle}>Collaboration</div>
            <div className={styles.featureDescription}>
              Share BuildBuddy links with co-workers to get your problems solved quickly — so you can get back to
              building.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Component;
