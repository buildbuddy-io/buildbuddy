import React from "react";
import common from "../../css/common.module.css";
import styles from "./rbe.module.css";
import { useEffect } from "react";
import { useState } from "react";

function Component() {
  let [index, setIndex] = useState(0);
  useEffect(() => {
    const interval = setInterval(() => {
      setIndex((index) => index + 1);
    }, 33);
    return () => clearInterval(interval);
  }, []);

  let unixTimeForOct1st2023 = 1696143600;
  let minutesSavedOnOct1st2023 = 2943179231;
  let savingsPerSecond = 158.28;
  let minutesInAYear = 525600;
  let minuteSaved = Math.round(
    minutesSavedOnOct1st2023 + savingsPerSecond * (new Date().getTime() / 1000 - unixTimeForOct1st2023)
  );

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
            <div className={styles.deploymentModelUnit}>Total Compute Minutes Saved</div>
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
