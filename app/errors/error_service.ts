import { BuildBuddyError } from "../../app/util/errors";
import alertService from "../alert/alert_service";

export class ErrorService {
  handleError(e: any) {
    console.error(e);

    const message = String(e instanceof BuildBuddyError ? e : BuildBuddyError.parse(e));
    alertService.show({ message, type: "error" });
  }
}

export default new ErrorService();
