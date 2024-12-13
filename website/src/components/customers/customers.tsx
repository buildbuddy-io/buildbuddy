import React from "react";
import styles from "./customers.module.css";

function Component() {
  return (
    <div className={styles.customerSection}>
      <div>
        <div className={styles.header}>TRUSTED BY THE BEST ENGINEERING TEAMS</div>
        <div className={styles.customers}>
          <img alt="Spotify Logo" className={styles.logo} width="150px" src={`/img/spotify.svg`} />
          <img alt="Cisco Logo" className={styles.logo} width="80px" src={`/img/cisco.svg`} />
          <img alt="Asana Logo" className={styles.logo} width="150px" src={`/img/asana.svg`} />
          <img alt="Benchling Logo" className={styles.logo} width="160px" src={`/img/benchling.svg`} />
          <img alt="Retool Logo" className={styles.logo} width="110px" src={`/img/retool.svg`} />
          <img
            alt="Mercari Logo"
            className={styles.logo}
            style={{ margin: "0 0 2px 0" }}
            width="110px"
            src={`/img/mercari.svg`}
          />
          <img alt="Verkada Logo" className={styles.logo} width="140px" src={`/img/verkada.svg`} />
          <img alt="Tecton Logo" className={styles.logo} width="110px" src={`/img/tecton.svg`} />
        </div>
      </div>
    </div>
  );
}

export default Component;
