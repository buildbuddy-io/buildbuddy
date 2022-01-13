import React from "react";
import common from "../../css/common.module.css";
import styles from "./cta.module.css";

function Component() {
  return (
    <div className={common.section}>
      <div className={common.container}>
        <div className={`${common.centeredText}`}>
          <h2 className={common.title}>Build better today.</h2>
          <a href="https://app.buildbuddy.io" className={`${common.button} ${common.buttonPrimary} ${styles.button}`}>
            Get Started for Free
          </a>
        </div>
      </div>
    </div>
  );
}

export default Component;
