import React from "react";
import styles from "./hero.module.css";
import common from "../../css/common.module.css";
import Image from "@theme/IdealImage";

function Component(props) {
  return (
    <div
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
              alt="BuildBuddy Enterprise Bazel Results UI"
              img={props.image || require("../../../static/img/hero.png")}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default Component;
