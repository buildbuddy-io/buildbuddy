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
            alt="Github Integration Octocat"
            className={styles.logo}
            style={{ padding: "20px 0", marginRight: "-32px" }}
            img={require("../../../static/img/github-image.png")}
          />
          <Image
            alt="Github Integration"
            className={styles.logo}
            style={{ padding: "20px 0" }}
            img={require("../../../static/img/github-text.png")}
          />
          <img alt="Slack Integration" width="244px" height="100px" className={styles.logo} src="/img/slack.svg" />
          <img alt="Gitlab Integration" width="226px" height="100px" className={styles.logo} src="/img/gitlab.svg" />
          <img
            alt="Buildkite Integration"
            width="247px"
            height="100px"
            className={styles.logo}
            style={{ padding: "28px 0" }}
            src="/img/buildkite.svg"
          />
          <Image
            alt="CircleCI Integration"
            className={styles.logo}
            style={{ padding: "16px 0" }}
            img={require("../../../static/img/circleci.png")}
          />
          <Image
            alt="Travis Integration"
            className={styles.logo}
            style={{ padding: "12px 0" }}
            img={require("../../../static/img/travis.png")}
          />
          <img
            alt="Jenkins Integration"
            width="211px"
            height="100px"
            className={styles.logo}
            style={{ padding: "16px 0" }}
            src="/img/jenkins.svg"
          />
          <Image alt="GCP Integration" className={styles.logo} img={require("../../../static/img/gcp.png")} />
          <Image
            alt="AWS Integration"
            className={styles.logo}
            style={{ padding: "24px 0" }}
            img={require("../../../static/img/aws.png")}
          />
        </div>
      </div>
    </div>
  );
}

export default Component;
