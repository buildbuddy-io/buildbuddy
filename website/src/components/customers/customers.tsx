import React from "react";
import styles from "./customers.module.css";

function Component() {
  let color = "";
  return (
    <div className={styles.customerSection}>
      <div className={styles.header}>TRUSTED BY THE BEST ENGINEERING TEAMS</div>
      <div className={styles.customers}>
        <img alt="Spotify Logo" className={styles.logo} width="150px" src={`/img/spotify${color}.svg`} />
        <img alt="Benchling Logo" className={styles.logo} width="150px" src={`/img/benchling${color}.svg`} />
        <img alt="Lyft Logo" className={styles.logo} width="70px" src={`/img/lyft${color}.svg`} />
        <img
          alt="Mercari Logo"
          className={styles.logo}
          style={{ margin: "0 0 6px 0" }}
          width="150px"
          src={`/img/mercari${color}.svg`}
        />
        <img alt="Squarespace Logo" className={styles.logo} width="210px" src={`/img/squarespace${color}.svg`} />
      </div>
    </div>
  );
}

export default Component;
