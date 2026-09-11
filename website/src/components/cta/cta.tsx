import React from "react";
import common from "../../css/common.module.css";
import styles from "./cta.module.css";

function Component(props) {
  return (
    <div className={common.section}>
      <div className={common.container}>
        <div className={`${common.centeredText}`}>
          <h2 className={common.title}>{props.title || "Build better today."}</h2>
          <a
            href={props.href || "https://app.buildbuddy.io"}
            className={`${common.button} ${common.buttonPrimary} ${styles.button}`}>
            {props.text || `Get Started for Free`}
          </a>
        </div>
      </div>
    </div>
  );
}

export default Component;
