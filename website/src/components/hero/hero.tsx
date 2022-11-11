import React, { useState } from "react";
import styles from "./hero.module.css";
import common from "../../css/common.module.css";
import Image from "@theme/IdealImage";
import { copyToClipboard } from "../../util/clipboard";
import { Copy } from "lucide-react";

function Component(props) {
  let [copied, setCopied] = useState(0);

  return (
    <div
      style={props.style}
      className={`${common.section} ${styles.hero} ${props.lessPadding ? styles.lessPadding : ""} ${
        props.noImage ? styles.noImage : ""
      }`}>
      <div className={`${common.container} ${common.splitContainer} ${props.flipped ? styles.flipped : ""}`}>
        <div className={common.text}>
          <h1 className={common.title}>
            {props.title || (
              <>
                Faster builds. <br /> Happier developers.
              </>
            )}
          </h1>
          <div className={common.subtitle}>
            {props.subtitle || (
              <>
                BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to
                build and test software 10x faster.
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
                className={`${common.button} ${common.buttonPrimary}`}>
                {props.primaryButtonText || <>Get Started for Free</>}
              </a>
            )}
            {props.secondaryButtonText !== "" && (
              <a href={props.secondaryButtonHref || "/request-demo"} className={common.button}>
                {props.secondaryButtonText || <>Request a Demo</>}
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
              placeholder={{ color: "#607D8B" }}
              threshold={10000}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default Component;
