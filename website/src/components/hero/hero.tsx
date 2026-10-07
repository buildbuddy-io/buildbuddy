import Image from "@theme/IdealImage";
import { CalendarDays, Copy } from "lucide-react";
import React, { useState } from "react";
import common from "../../css/common.module.css";
import { copyToClipboard } from "../../util/clipboard";
import styles from "./hero.module.css";

function Component(props) {
  let [copied, setCopied] = useState(0);

  return (
    <div
      style={props.style}
      className={`${common.section} ${styles.hero} ${props.lessPadding ? styles.lessPadding : ""} ${
        props.noImage ? styles.noImage : ""
      } ${props.cropImageOnSmallScreens ? styles.cropImageOnSmallScreens : ""}`}>
      <div className={`${common.container} ${common.splitContainer} ${props.flipped ? styles.flipped : ""}`}>
        <div className={`${common.text} ${!props.title ? styles.homepageText : ""}`}>
          <h1 className={`${common.title} ${!props.title ? styles.homepageTitle : ""}`}>
            {props.title || (
              <>
                The engineering acceleration platform <span className={styles.bazel}>built for Bazel</span>
              </>
            )}
          </h1>
          <div className={common.subtitle}>
            {props.subtitle || (
              <>
                Build and test your software 10x faster while reducing compute costs with remote caching, remote execution,
                analytics, and more.
              </>
            )}
          </div>
          <div className={styles.buttons}>
            {props.snippet && (
              <div
                className={`${styles.snippet} ${(copied && styles.copied) || ""}`}
                onClick={() => {
                  copyToClipboard(props.snippet);
                  setCopied(1);
                  setTimeout(() => setCopied(0), 2000);
                }}>
                {props.snippet}
                <Copy />
              </div>
            )}
            {props.primaryButtonText !== "" && (
              <a
                href={props.primaryButtonHref || "https://app.buildbuddy.io"}
                className={`${common.button} ${common.buttonPrimary} ${styles.heroButton}`}>
                {props.primaryButtonText || <>Get Started for Free</>}
              </a>
            )}
            {props.secondaryButtonText !== "" && (
              <a
                href={props.secondaryButtonHref || "/request-demo"}
                className={`${common.button} ${props.gradientButton ? common.buttonGradient : ""} ${styles.heroButton}`}>
                {props.secondaryButtonText || (
                  <>
                    <CalendarDays aria-hidden="true" /> Request a Demo
                  </>
                )}
              </a>
            )}
          </div>
        </div>
        <div
          className={`${styles.image} ${props.bigImage ? styles.bigImage : ""} ${
            props.peekMore ? styles.peekMore : ""
          }`}>
          {props.component || (
            <Image
              alt={props.title ? `Bazel ${props.title}` : "BuildBuddy Enterprise Bazel Results UI"}
              img={props.image || require("../../../static/img/hero.png")}
              shouldAutoDownload={() => true}
              placeholder={{ color: "#9e9e9e" }}
              threshold={10000}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default Component;
