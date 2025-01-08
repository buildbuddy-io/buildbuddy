import router from "../router/router";
import { BuildBuddyError, ErrorCode } from "../util/errors";
import alertService from "../alert/alert_service";

const ERROR_SEARCH_PARAM = "error";

export class ErrorService {
  register() {
    let searchParams = new URLSearchParams(window.location.search);
    if (searchParams.has(ERROR_SEARCH_PARAM)) {
      this.handleError(searchParams.get(ERROR_SEARCH_PARAM));
      searchParams.delete(ERROR_SEARCH_PARAM);
      router.replaceParams(Object.fromEntries(searchParams.entries()));
    }
  }

  handleError(e: any, options?: { ignoreErrorCodes: ErrorCode[] }) {
    // NOTE: keep this string in sync with webtester.go -
    // web tests check for this message and fail the test if it's logged.
    console.error(new Error("Displaying error banner"));

    console.error(e);
    const error = e instanceof BuildBuddyError ? e : BuildBuddyError.parse(e);
    if (options?.ignoreErrorCodes?.includes(error.code)) {
      console.log("Ignoring error code: " + error.code);
      return;
    }
    alertService.error(String(error));
    window.opener?.postMessage(
      { type: "buildbuddy_message", error: String(error), success: false },
      window.location.origin
    );
  }
}

export default new ErrorService();
