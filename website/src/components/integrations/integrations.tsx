import React from "react";
import common from "../../css/common.module.css";
import styles from "./integrations.module.css";
import Image from "@theme/IdealImage";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionGray} ${styles.integrationSection}`}>
      <div className={common.container}>
        <div className={styles.text}>
          <h2 className={styles.title}>BuildBuddy integrates with</h2>
        </div>
      </div>
      <div className={common.container}>
        <div className={styles.logos}>
          <Image
            className={styles.logo}
            style={{ padding: "20px 0", marginRight: "-32px" }}
            img={require("../../../static/img/github-image.png")}
          />
          <Image
            className={styles.logo}
            style={{ padding: "20px 0" }}
            img={require("../../../static/img/github-text.png")}
          />
          <img className={styles.logo} src="/img/slack.svg" />
          <img className={styles.logo} src="/img/gitlab.svg" />
          <img className={styles.logo} style={{ padding: "28px 0" }} src="/img/buildkite.svg" />
          <Image
            className={styles.logo}
            style={{ padding: "16px 0" }}
            img={require("../../../static/img/circleci.png")}
          />
          <Image
            className={styles.logo}
            style={{ padding: "12px 0" }}
            img={require("../../../static/img/travis.png")}
          />
          <img className={styles.logo} style={{ padding: "16px 0" }} src="/img/jenkins.svg" />
          <Image className={styles.logo} img={require("../../../static/img/gcp.png")} />
          <Image className={styles.logo} style={{ padding: "24px 0" }} img={require("../../../static/img/aws.png")} />
        </div>
      </div>
    </div>
  );
}

export default Component;
