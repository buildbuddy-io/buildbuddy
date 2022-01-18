import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";
import styles from "./contact.module.css";
import message from "../util/message";
import { Calendar, Github, Mail, Slack } from "lucide-react";

let form = {
  company: React.createRef<HTMLInputElement>(),
  email: React.createRef<HTMLInputElement>(),
  firstName: React.createRef<HTMLInputElement>(),
  lastName: React.createRef<HTMLInputElement>(),
  message: React.createRef<HTMLTextAreaElement>(),
  button: React.createRef<HTMLButtonElement>(),
};

function Contact() {
  return (
    <Layout title="Contact Us">
      <div className={common.page}>
        <div className={common.section}>
          <div className={common.container}>
            <div className={common.centeredText}>
              <div className={common.title}>
                Get in touch,
                <br />
                we love to chat.
              </div>
              <div className={common.subtitle}>
                <br />
                Questions, feature requests, and ideas welcome!
              </div>
            </div>
          </div>
          <div className={common.container}>
            <div className={styles.contactMethods}>
              <a href="mailto:hello@buildbuddy.io">
                <Mail /> Email us at hello@buildbuddy.io
              </a>
              <a href="/request-demo">
                <Calendar /> Schedule a demo
              </a>
              <a href="https://slack.buildbuddy.io" target="_blank">
                <Slack /> Chat with us on Slack
              </a>
              <a href="https://github.com/buildbuddy-io/buildbuddy/issues/new" target="_blank">
                <Github /> Open a Github issue
              </a>
            </div>
          </div>
          <div className={common.container}>
            <div className={styles.form}>
              <input ref={form.company} placeholder="Company" />
              <input ref={form.email} placeholder="Work email address" />
              <input ref={form.firstName} placeholder="First name" />
              <input ref={form.lastName} placeholder="Last name" />
              <textarea ref={form.message} placeholder="Your message" className={styles.span2} />
              <button
                ref={form.button}
                onClick={() => sendMessage()}
                className={`${common.button} ${common.buttonPrimary} ${styles.span2}`}>
                Send message
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
    `New Contact Form Message!\nName: ${form.firstName.current.value} ${form.lastName.current.value}\nEmail: ${form.email.current.value}\nCompany: ${form.company.current.value}\nMessage: ${form.message.current.value}`
  );

  form.firstName.current.disabled = true;
  form.lastName.current.disabled = true;
  form.email.current.disabled = true;
  form.company.current.disabled = true;
  form.message.current.disabled = true;

  form.button.current.innerText = "Message Sent!";
  form.button.current.disabled = true;
}

export default Contact;
