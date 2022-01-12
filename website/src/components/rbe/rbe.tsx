import React from "react";
import common from "../../css/common.module.css";
import styles from "./rbe.module.css";
import { Cloud, Home } from "lucide-react";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionDark}`}>
      <div className={`${common.container} ${common.splitContainer}`}>
        <div className={common.text}>
          <div className={common.pillTitle}>RBE</div>
          <h2 className={common.title}>
            Remote Build
            <br /> Execution
          </h2>
          <div className={common.subtitle}>
            Massively parallel builds with just a few lines of configuration. BuildBuddy provides remote build execution
            both on-prem and as a fully-managed cloud service. Supports custom Docker images and automatically scales to
            thousands of worker nodes.
          </div>
        </div>
        <div className={styles.deploymentModels}>
          <div className={styles.deploymentModel}>
            <div className={styles.deploymentModelTitle}>
              <Cloud className={styles.deploymentModelIcon} /> Cloud
            </div>
            <div className={styles.deploymentModelDescription}>
              Fully managed BuildBuddy Cloud enables you to parallelize your Bazel builds across 1000s of machines
              instantly.
            </div>
          </div>
          <div className={styles.deploymentModel}>
            <div className={styles.deploymentModelTitle}>
              <Home className={styles.deploymentModelIcon} /> On-prem
            </div>
            <div className={styles.deploymentModelDescription}>
              Run BuildBuddy in your own Kubernetes cluster. It's easy to configure and supports GCP, AWS, and Azure.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Component;
