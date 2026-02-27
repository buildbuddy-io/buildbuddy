import Layout from "@theme/Layout";
import { Calendar, Github, Mail, Slack } from "lucide-react";
import React from "react";
import common from "../css/common.module.css";
import message from "../util/message";
import styles from "./contact.module.css";

let form = {
  company: React.createRef<HTMLInputElement>(),
  email: React.createRef<HTMLInputElement>(),
  firstName: React.createRef<HTMLInputElement>(),
  lastName: React.createRef<HTMLInputElement>(),
  message: React.createRef<HTMLTextAreaElement>(),
  button: React.createRef<HTMLButtonElement>(),
};

function Contact() {
  const [errors, setErrors] = React.useState<{ email?: string; message?: string }>({});

  const handleSendMessage = () => {
    const newErrors: { email?: string; message?: string } = {};

    const emailValue = form.email.current?.value?.trim() || "";
    const messageValue = form.message.current?.value?.trim() || "";

    if (!emailValue) {
      newErrors.email = "Email is required";
    } else if (!emailValue.includes("@")) {
      newErrors.email = "Please enter a valid email address";
    }

    if (!messageValue) {
      newErrors.message = "Message is required";
    }

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }

    setErrors({});
    sendMessage();
  };

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
              <a href="https://community.buildbuddy.io" target="_blank">
                <Slack /> Chat with us on Slack
              </a>
              <a href="https://github.com/buildbuddy-io/buildbuddy/issues/new" target="_blank">
                <Github /> Open a Github issue
              </a>
            </div>
          </div>
          <div className={common.container}>
            <div className={styles.form}>
              {(errors.email || errors.message) && (
                <div className={`${styles.errorSummary} ${styles.span2}`}>
                  {errors.email && <div>{errors.email}</div>}
                  {errors.message && <div>{errors.message}</div>}
                </div>
              )}
              <input ref={form.company} placeholder="Company" />
              <input
                ref={form.email}
                placeholder="Work email address"
                className={errors.email ? styles.inputError : ""}
              />
              <input ref={form.firstName} placeholder="First name" />
              <input ref={form.lastName} placeholder="Last name" />
              <textarea
                ref={form.message}
                placeholder="Your message"
                className={`${styles.span2} ${errors.message ? styles.inputError : ""}`}
              />
              <button
                ref={form.button}
                onClick={handleSendMessage}
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
