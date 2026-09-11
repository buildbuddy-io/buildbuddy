import React from "react";
import common from "../../css/common.module.css";
import styles from "./integrations.module.css";

function Component() {
  return (
    <div className={`${common.section} ${common.sectionGray} ${styles.integrationSection}`}>
      <div className={common.container}>
        <div className={common.centeredText}>
          <h2 className={styles.title}>BuildBuddy integrates with</h2>
        </div>
      </div>
      <div className={common.container}>
        <div className={styles.logos}>
          <img
            alt="Github Integration Octocat"
            className={styles.logo}
            height="416"
            src="/img/github-image.png"
            style={{ marginRight: "-32px", padding: "20px 0" }}
            width="500"
          />
          <img
            alt="Github Integration"
            className={styles.logo}
            height="205"
            src="/img/github-text.png"
            style={{ padding: "20px 0" }}
            width="500"
          />
          <img alt="Slack Integration" className={styles.logo} src="/img/slack.svg" />
          <img alt="Gitlab Integration" className={styles.logo} src="/img/gitlab.svg" />
          <img
            alt="Buildkite Integration"
            className={styles.logo}
            style={{ padding: "28px 0" }}
            src="/img/buildkite.svg"
          />
          <img
            alt="CircleCI Integration"
            className={styles.logo}
            height="142"
            src="/img/circleci.png"
            style={{ padding: "16px 0" }}
            width="500"
          />
          <img
            alt="Travis Integration"
            className={styles.logo}
            height="201"
            src="/img/travis.png"
            style={{ padding: "12px 0" }}
            width="642"
          />
          <img alt="Jenkins Integration" className={styles.logo} style={{ padding: "16px 0" }} src="/img/jenkins.svg" />
          <img
            alt="GCP Integration"
            className={styles.logo}
            height="204"
            src="/img/gcp.png"
            width="800"
          />
          <img
            alt="AWS Integration"
            className={styles.logo}
            height="300"
            src="/img/aws.png"
            style={{ padding: "24px 0" }}
            width="500"
          />
        </div>
      </div>
    </div>
  );
}

export default Component;
