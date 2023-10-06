export default {
  open: (url: string, timeout = 360, width = 400, height = 600) => {
    return new Promise<any>((resolve, reject) => {
      const left = window.screenX + (window.innerWidth - width) / 2;
      const top = window.screenY + (window.innerHeight - height) / 2;

      // Open a centered popup
      let popup = window.open(
        url,
        "buildbuddy:authorize:popup",
        `left=${left},top=${top},width=${width},height=${height},resizable,scrollbars=yes,status=1`
      );

      let popupEventListener: (e: MessageEvent) => void;

      // Check if whether popup has closed.
      const popupTimer = setInterval(() => {
        if (popup && popup.closed) {
          clearInterval(popupTimer);
          clearTimeout(timeoutId);
          window.removeEventListener("message", popupEventListener, false);
          reject("Authentication popup closed.");
        }
      }, 1000);

      // Check if we've hit our timeout yet.
      const timeoutId = setTimeout(() => {
        clearInterval(popupTimer);
        popup?.close();
        reject("Authentication timed out.");
        window.removeEventListener("message", popupEventListener, false);
      }, timeout * 1000);

      // If we receieve a message, resolve or reject the promise based on the presence of error text.
      popupEventListener = function (e: MessageEvent) {
        clearTimeout(timeoutId);
        clearInterval(popupTimer);
        window.removeEventListener("message", popupEventListener, false);
        popup?.close();
        console.log("Received message from popup: " + e.data);
        if (e.data != "") {
          reject(e.data);
        }
        resolve(e.data);
      };

      window.addEventListener("message", popupEventListener);
    });
  },
};
