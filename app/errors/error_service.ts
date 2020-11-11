import { Subject } from "rxjs";
import { BuildBuddyError } from "../util/errors";

export class ErrorService {
  errorStream = new Subject<BuildBuddyError>();

  handleError(e: any) {
    this.errorStream.next(e instanceof BuildBuddyError ? e : BuildBuddyError.parse(e));
  }
}

export default new ErrorService();
