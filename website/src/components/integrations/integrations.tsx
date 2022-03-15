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
            style={{ marginRight: "-32px", width: "72px", height: "100px", padding: "20px 0" }}
            img={require("../../../static/img/github-image.png")}
            shouldAutoDownload={() => true}
            threshold={10000}
          />
          <Image
            alt="Github Integration"
            className={styles.logo}
            style={{ width: "146px", height: "100px", padding: "20px 0" }}
            img={require("../../../static/img/github-text.png")}
            shouldAutoDownload={() => true}
            threshold={10000}
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
            style={{ width: "240px", height: "100px", padding: "16px 0" }}
            img={require("../../../static/img/circleci.png")}
            shouldAutoDownload={() => true}
            threshold={10000}
          />
          <Image
            alt="Travis Integration"
            className={styles.logo}
            style={{ width: "243px", height: "100px", padding: "12px 0" }}
            img={require("../../../static/img/travis.png")}
            shouldAutoDownload={() => true}
            threshold={10000}
          />
          <img
            alt="Jenkins Integration"
            width="211px"
            height="100px"
            className={styles.logo}
            style={{ padding: "16px 0" }}
            src="/img/jenkins.svg"
          />
          <Image
            alt="GCP Integration"
            className={styles.logo}
            style={{ width: "392px", height: "100px" }}
            img={require("../../../static/img/gcp.png")}
            shouldAutoDownload={() => true}
            threshold={10000}
          />
          <Image
            alt="AWS Integration"
            className={styles.logo}
            style={{ width: "87px", height: "100px", padding: "24px 0" }}
            img={require("../../../static/img/aws.png")}
            shouldAutoDownload={() => true}
            threshold={10000}
          />
        </div>
      </div>
    </div>
  );
}

export default Component;
