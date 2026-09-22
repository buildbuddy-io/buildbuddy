// Client script for auto-refresh. Makes a long-running request to
// /_/auto-refresh and tries to quickly reconnect if the request disconnects.
// The client sends the server ID in each request, and if the server's ID
// doesn't match, the client reloads.
//
// This request consumes 1 of the 6 connections that are allowed by browsers,
// which becomes problematic with lots of tabs open, so the request is canceled
// if the page is hidden and then resumed again when it becomes visible.
(async () => {
  const url = "/_/auto-refresh?server_id=" + encodeURIComponent(document.currentScript.dataset.serverId);
  let controller = new AbortController();
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) controller.abort();
  });
  let delayMs = 10;
  while (true) {
    if (document.hidden) {
      await new Promise((resolve) => document.addEventListener("visibilitychange", resolve, { once: true }));
      continue;
    }
    controller = new AbortController();
    try {
      const response = await fetch(url, { signal: controller.signal });
      if (response.status === 205) {
        window.location.reload();
        return;
      }
      await response.text();
    } catch {}
    if (controller.signal.aborted) continue;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    delayMs = Math.min(delayMs * 1.01, 5000);
  }
})();
