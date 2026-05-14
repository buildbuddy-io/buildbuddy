const FORMS_WEBHOOK = atob(
  "aHR0cHM6Ly9ob29rcy5zbGFjay5jb20vc2VydmljZXMvVE4zMk1ENlRVL0IwMlQ2QUo0M1JEL3dPckNRQUJTQ1BLNTlyem8xUEF5RWV1Yw"
);
const JOB_APPLICATION_WEBHOOK = atob(
  "aHR0cHM6Ly9ob29rcy5zbGFjay5jb20vc2VydmljZXMvVE4zMk1ENlRVL0IwQUpXVVM2OUg2L2dJc0NJSzVCVm5kWUkybmhQdzJlajlyRw=="
);

function post(webhook: string, message: string) {
  return fetch(webhook, {
    method: "POST",
    body: `{"text":"${message.replace(/"/g, '\\"')}"}`,
  });
}

export function sendFormsMessage(message: string) {
  return post(FORMS_WEBHOOK, message);
}

export function sendJobApplicationMessage(message: string) {
  return post(JOB_APPLICATION_WEBHOOK, message);
}
