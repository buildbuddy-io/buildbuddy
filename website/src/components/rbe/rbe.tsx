import React, { useEffect, useState } from "react";
import common from "../../css/common.module.css";
import styles from "./rbe.module.css";

function Component() {
  let [index, setIndex] = useState(0);
  useEffect(() => {
    const interval = setInterval(() => {
      setIndex((index) => index + 1);
    }, 33);
    return () => clearInterval(interval);
  }, []);

  const baselineUnixTime = Date.parse("2026-09-11T00:00:00Z") / 1000;
  const minutesSavedAtBaseline = 29686846011;
  const savingsPerSecond = 815.3;
  const minutesInAYear = 525600;
  const minuteSaved = Math.round(minutesSavedAtBaseline + savingsPerSecond * (Date.now() / 1000 - baselineUnixTime));

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
            Massively parallel builds with just a few lines of configuration and no maintenance burden. BuildBuddy
            provides remote build execution and caching as a fully-managed cloud service. Supports custom Docker images,
            and automatically scales to thousands of worker nodes.
          </div>
        </div>
        <div className={styles.deploymentModels}>
          <div className={styles.deploymentModel}>
            <div className={styles.deploymentModelTitle}>{minuteSaved.toLocaleString()} </div>
            <div className={styles.deploymentModelUnit}>Compute Minutes Saved</div>
            <div className={styles.deploymentModelDescription}>
              By organizations using BuildBuddy Cloud remote caching &mdash; that's{" "}
              {Math.round(minuteSaved / minutesInAYear).toLocaleString()} years not spent waiting for builds to finish.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Component;
