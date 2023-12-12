import React from "react";
import styles from "./version_pill.module.css";

function Component(props) {
  return (
    <span className={styles.pill}>
      <span className={styles.hide}>[</span>
      {props.version}
      <span className={styles.hide}>]</span>
    </span>
  );
}

export default Component;
