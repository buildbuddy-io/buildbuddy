import React from "react";
import common from "../../css/common.module.css";
import styles from "../../pages/contact.module.css";
import message from "../../util/message";

let form = {
  firstName: React.createRef<HTMLInputElement>(),
  lastName: React.createRef<HTMLInputElement>(),
  email: React.createRef<HTMLInputElement>(),
  phone: React.createRef<HTMLInputElement>(),
  linkedin: React.createRef<HTMLInputElement>(),
  github: React.createRef<HTMLInputElement>(),
};

function Component() {
  return (
    <div className={styles.form}>
      <input ref={form.firstName} placeholder="First name" />
      <input ref={form.lastName} placeholder="Last name" />
      <input ref={form.email} placeholder="Email address" />
      <input ref={form.phone} placeholder="Phone number" />
      <input ref={form.linkedin} placeholder="LinkedIn Profile" />
      <input ref={form.github} placeholder="Github Profile" />
      <button onClick={() => sendMessage()} className={`${common.button} ${common.buttonPrimary} ${styles.span2}`}>
        Submit Application
      </button>
    </div>
  );
}

function sendMessage() {
  message(
    `New Job Application!\nURL: ${window.location.href}\nName: ${form.firstName.current.value} ${form.lastName.current.value}\nEmail: ${form.email.current.value}\nPhone: ${form.phone.current.value}\nLinkedIn: ${form.linkedin.current.value}\nGithub: ${form.github.current.value}`
  );
}

export default Component;
