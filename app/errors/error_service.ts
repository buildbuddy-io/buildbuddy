import router from "../router/router";
import { BuildBuddyError, ErrorCode } from "../util/errors";
import alertService from "../alert/alert_service";

const ERROR_SEARCH_PARAM = "error";

export class ErrorService {
  isWindowUnloading = false;

  register() {
    let searchParams = new URLSearchParams(window.location.search);
    if (searchParams.has(ERROR_SEARCH_PARAM)) {
      this.handleError(searchParams.get(ERROR_SEARCH_PARAM));
      searchParams.delete(ERROR_SEARCH_PARAM);
      router.replaceParams(Object.fromEntries(searchParams.entries()));
    }
    // Once the user starts unloading the page, ignore errors, to avoid a brief
    // window where we display the error banner due to errors that happen when
    // RPCs are disconnected due to the browser closing the connection.
    window.addEventListener(
      "beforeunload",
      () => {
        this.isWindowUnloading = true;
      },
      { once: true }
    );
  }

  handleError(e: any, options?: { ignoreErrorCodes: ErrorCode[] }) {
    if (this.isWindowUnloading) {
      console.debug("Ignoring error due to page unload:", e);
      return;
    }
    console.error(e);
    const error = e instanceof BuildBuddyError ? e : BuildBuddyError.parse(e);
    if (options?.ignoreErrorCodes?.includes(error.code)) {
      console.log("Ignoring error code: " + error.code);
      return;
    }
    // NOTE: keep this string in sync with webtester.go -
    // web tests check for this message and fail the test if it's logged.
    console.error(new Error("Displaying error banner"));
    alertService.error(String(error));
    window.opener?.postMessage(
      { type: "buildbuddy_message", error: String(error), success: false },
      window.location.origin
    );
  }
}

export default new ErrorService();
