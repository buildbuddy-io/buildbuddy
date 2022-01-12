export default function sendMessage(message: string) {
  return fetch(
    atob("aHR0cHM6Ly9ob29rcy5zbGFjay5jb20vc2VydmljZXMvVE4zMk1ENlRVL0IwMlQ2QUo0M1JEL3dPckNRQUJTQ1BLNTlyem8xUEF5RWV1Yw"),
    {
      method: "POST",
      body: `{"text":"${message.replace(/"/g, '\\"')}"}`,
    }
  );
}
