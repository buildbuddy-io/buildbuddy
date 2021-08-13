import router from "../router/router";
import { BuildBuddyError } from "../util/errors";
import alertService from "../alert/alert_service";

const ERROR_SEARCH_PARAM = "error";
export class ErrorService {
  register() {
    let searchParams = new URLSearchParams(window.location.search);
    if (searchParams.has(ERROR_SEARCH_PARAM)) {
      this.handleError(searchParams.get(ERROR_SEARCH_PARAM));
    }
    router.replaceParams({ ERROR_SEARCH_PARAM: undefined });
  }

  handleError(e: any) {
    console.error(e);

    const message = String(e instanceof BuildBuddyError ? e : BuildBuddyError.parse(e));
    alertService.error(message);
  }
}

export default new ErrorService();
