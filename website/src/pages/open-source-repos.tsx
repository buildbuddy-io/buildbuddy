import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";
import styles from "./open-source-repos.module.css";
import contact from "./contact.module.css";
import OSSList from "../components/oss/oss-list";
import message from "../util/message";

let form = {
  name: React.createRef<HTMLInputElement>(),
  email: React.createRef<HTMLInputElement>(),
  repo: React.createRef<HTMLInputElement>(),
  button: React.createRef<HTMLButtonElement>(),
};

function OpenSource() {
  return (
    <Layout title="Powered By BuildBuddy">
      <div className={common.page}>
        <div className={common.section}>
          <div className={common.container}>
            <div className={common.centeredText}>
              <div className={common.title}>Powered by BuildBuddy</div>
              <div className={common.subtitle}>
                <br />
                Here are some of our favorite open source repos that are powered by BuildBuddy.
              </div>
            </div>
          </div>
        </div>
        <div className={`${common.section} ${common.sectionDark} ${common.sectionLessBottom} ${styles.ossSection}`}>
          <div className={common.container}>
            <OSSList />
          </div>
        </div>
        <div className={`${common.section} ${common.sectionDark} ${styles.ossSection}`}>
          <div className={`${common.container} ${common.splitContainer}`}>
            <div className={common.text}>
              <h1 className={common.title}>Want to see your open source repo on this list?</h1>
              <div className={common.subtitle}>We love open source projects of all sizes! </div>
            </div>
            <div className={contact.form}>
              <input ref={form.name} placeholder="Name" />
              <input ref={form.email} placeholder="Email address" />
              <input ref={form.repo} placeholder="Repo URL" className={contact.span2} />
              <button
                ref={form.button}
                onClick={() => sendMessage()}
                className={`${common.button} ${common.buttonPrimary} ${contact.span2}`}>
                Submit your repo
              </button>
            </div>
          </div>
        </div>
      </div>
    </Layout>
  );
}

function sendMessage() {
  message(
    `New Open Source Repo!\nName: ${form.name.current.value}\nEmail: ${form.email.current.value}\nRepo URL: ${form.repo.current.value}`
  );

  form.name.current.disabled = true;
  form.email.current.disabled = true;
  form.repo.current.disabled = true;

  form.button.current.innerText = "Repo Submitted!";
  form.button.current.disabled = true;
}

export default OpenSource;
