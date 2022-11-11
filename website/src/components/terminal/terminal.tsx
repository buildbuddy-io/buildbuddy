import React, { useState, useEffect } from "react";
import styles from "./terminal.module.css";

function Term(props) {
  let maxActions = 8191;
  let actions = 100 + random(500);
  let [count, setCount] = useState(0);
  let [index, setIndex] = useState(0);
  if (count > maxActions) {
    setCount(0);
  }

  useEffect(() => {
    const interval = setInterval(() => {
      setCount((count) => count + actions);
      setIndex((index) => index + 1);
    }, props.duration || 300);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className={styles.terminal}>
      <div className={styles.terminalMenu}>
        <div className={styles.terminalButtons}>
          <div className={styles.terminalClose} />
          <div className={styles.terminalMinimize} />
          <div className={styles.terminalExpand} />
        </div>
        <div className={styles.terminalTitle}>siggi@lunchbox: ~/tensorflow</div>
      </div>
      <div className={styles.terminalContentWrapper}>
        <div className={styles.terminalContent}>
          {(props.states && props.states[index % props.states.length]) ||
            props.contents ||
            remote(count, maxActions, actions)}
        </div>
      </div>
    </div>
  );
}

function random(max) {
  return Math.floor(Math.random() * max);
}

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function commas(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function remote(count, maxActions, actions) {
  return (
    <>
      <Prompt /> bazel build tensorflow --config=remote
      <br />
      <Info /> Invocation ID: 5fa9285b-b01e-435b-9ad4-96543f939e6a
      <br />
      <Info /> Streaming build results to:{" "}
      <a href="#">https://app.buildbuddy.io/invocation/5fa9285b-b01e-435b-9ad4-96543f939e6a</a>
      <br />
      <Info /> Analyzed target //tensorflow:tensorflow (226 packages loaded, 20226 targets configured)
      <br />
      <Info /> Found 1 target...
      <br />
      <span className={styles.terminalInfo}>
        [{commas(count)} / {commas(maxActions)}]
      </span>{" "}
      {actions} actions, {Math.floor(actions * 0.8)} running
      <br />
      {shuffle([
        "GoToolchainBinaryCompile external/go_sdk_Linux/builder.a [for host]; 2s remote",
        "Compiling src/google/protobuf/message_lite.cc; 2s remote",
        "Compiling src/google/protobuf/io/zero_copy_stream_impl_lite.cc; 2s remote",
        "Compiling src/google/protobuf/any_lite.cc; 2s remote",
        "Compiling src/google/protobuf/inlined_string_field.cc; 2s remote",
        "Compiling src/google/protobuf/wire_format_lite.cc; 2s remote",
        "Compiling sc/google/protobuf/stubs/strutil.cc; 2s remote",
        "Compiling src/google/protobuf/stubs/structurally_valid.cc; 2s remote",
      ]).map((message, index) => (
        <div key={index}>&nbsp;&nbsp;&nbsp;&nbsp;{message}</div>
      ))}
    </>
  );
}

function Prompt() {
  return (
    <>
      <span className={styles.terminalUser}>siggi@lunchbox</span>:{" "}
      <span className={styles.terminalPath}>~/tensorflow</span>${" "}
    </>
  );
}

function Info(props) {
  return <span style={{ color: "#64dd17" }}>{props.text || "INFO:"}</span>;
}

function Error(props) {
  return <span style={{ color: "#F44336" }}>{props.text || "ERROR:"}</span>;
}

function Question(props) {
  return <span style={{ color: "#00BCD4" }}>{props.text || "ERROR:"}</span>;
}

function Detail(props) {
  return <span style={{ color: "#9E9E9E" }}>{props.text || "ERROR:"}</span>;
}

export { Term as default, Prompt, Info, Error, Question, Detail };
