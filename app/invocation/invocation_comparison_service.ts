import { invocation } from "../../proto/invocation_ts_proto";
import InvocationModel from "./invocation_model";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import { Subject, Subscription } from "rxjs";
import alert_service from "../alert/alert_service";

export const INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY = "invocation_id_to_compare";

export interface IdAndModel {
  id?: string;
  model?: InvocationModel;
}

export class InvocationComparisonService {
  private invocationId?: string;
  private invocationModel?: InvocationModel;
  private timeoutRef?: number;
  private pendingRequest?: CancelablePromise<void>;

  private subject: Subject<IdAndModel> = new Subject();

  constructor() {
    window.addEventListener("storage", this.onStorage.bind(this));
    this.onStorage();
  }

  private onStorage() {
    const storageValue = localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY] || undefined;
    if (this.invocationId !== storageValue) {
      this.setComparisonInvocation(storageValue);
    }
  }

  fetch() {
    if (this.invocationId === undefined) {
      this.invocationModel = undefined;
      return;
    }
    // Don't re-fetch data if it's unlikely to update.
    if (this.invocationModel !== undefined && !this.invocationModel.isInProgress()) {
      return;
    }
    // Don't re-fetch data: the pending request should be for this ID already,
    // and we'll retry if it fails.
    if (this.pendingRequest) {
      return;
    }

    this.pendingRequest = rpcService.service
      .getInvocation(
        new invocation.GetInvocationRequest({
          lookup: new invocation.InvocationLookup({ invocationId: this.invocationId }),
        })
      )
      .then((response: invocation.GetInvocationResponse) => {
        if (!response.invocation || response.invocation.length === 0) {
          // Couldn't find the invocation -- let's just forget it.
          this.setComparisonInvocation(undefined);
        }
        if (response.invocation[0].invocationId !== this.invocationId) {
          return;
        }
        this.invocationModel = new InvocationModel(response.invocation[0]);
        this.publishState();

        // Keep checking for updates to unfinished invocations.
        if (this.invocationModel.isInProgress()) {
          this.scheduleFetch();
        }
      })
      .catch((error: any) => {
        // Something went wrong.  This is pretty non-critical.  Let's just
        // clear the selection so the user doesn't get hosed.
        alert_service.error("Failed to fetch comparison invocation.");
        this.setComparisonInvocation(undefined);
      })
      .finally(() => (this.pendingRequest = undefined));
  }

  private scheduleFetch() {
    clearTimeout(this.timeoutRef);
    // Refetch data every 10 seconds for unfinished invocations.
    // This doesn't need to be constant spam, just want to eventually
    // show an update.
    this.timeoutRef = window.setTimeout(() => {
      this.fetch();
    }, 10000);
  }

  setComparisonInvocation(invocationId?: string) {
    if (this.invocationId === invocationId || (!this.invocationId && !invocationId)) {
      return;
    }

    if (!invocationId) {
      this.invocationId = undefined;
      delete localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY];
    } else {
      this.invocationId = invocationId;
      localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY] = invocationId;
    }
    this.pendingRequest?.cancel();
    this.pendingRequest = undefined;
    this.invocationModel = undefined;

    this.publishState();
  }

  private publishState() {
    this.subject.next({ id: this.invocationId, model: this.invocationModel });
  }

  isLoading(): boolean {
    return Boolean(this.invocationId && this.pendingRequest);
  }

  isInProgress(): boolean {
    return Boolean(this.invocationModel?.isInProgress());
  }

  getComparisonInvocationId(): string | undefined {
    return this.invocationId;
  }

  getComparisonInvocation(): InvocationModel | undefined {
    return this.invocationModel;
  }

  subscribe(observer: (value: IdAndModel) => void): Subscription {
    return this.subject.subscribe(observer);
  }
}

export default new InvocationComparisonService();
