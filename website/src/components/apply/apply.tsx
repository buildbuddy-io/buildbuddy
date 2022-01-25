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
  button: React.createRef<HTMLButtonElement>(),
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
      <button
        ref={form.button}
        onClick={() => sendMessage()}
        className={`${common.button} ${common.buttonPrimary} ${styles.span2}`}>
        Submit Application
      </button>
    </div>
  );
}

function sendMessage() {
  message(
    `New Job Application!\nURL: ${window.location.href}\nName: ${form.firstName.current.value} ${form.lastName.current.value}\nEmail: ${form.email.current.value}\nPhone: ${form.phone.current.value}\nLinkedIn: ${form.linkedin.current.value}\nGithub: ${form.github.current.value}`
  );

  form.firstName.current.disabled = true;
  form.lastName.current.disabled = true;
  form.email.current.disabled = true;
  form.phone.current.disabled = true;
  form.linkedin.current.disabled = true;
  form.github.current.disabled = true;

  form.button.current.innerText = "Application Submitted!";
  form.button.current.disabled = true;
}

export default Component;
