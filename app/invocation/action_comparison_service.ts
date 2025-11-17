import { Subject, Subscription } from "rxjs";
import alert_service from "../alert/alert_service";

export const ACTION_TO_COMPARE_LOCALSTORAGE_KEY = "action_to_compare";

export interface ActionComparisonData {
  invocationId?: string;
  actionDigest?: string;
}

export class ActionComparisonService {
  private comparisonData?: ActionComparisonData;
  private subject: Subject<ActionComparisonData> = new Subject();

  constructor() {
    window.addEventListener("storage", this.onStorage.bind(this));
    this.onStorage();
  }

  private onStorage(): void {
    const storageValue = localStorage[ACTION_TO_COMPARE_LOCALSTORAGE_KEY];
    if (storageValue) {
      try {
        const data = JSON.parse(storageValue) as ActionComparisonData;
        if (
          this.comparisonData?.invocationId !== data.invocationId ||
          this.comparisonData?.actionDigest !== data.actionDigest
        ) {
          this.comparisonData = data;
          this.publishState();
        }
      } catch (e) {
        // Invalid data in localStorage, clear it
        this.clearComparisonAction();
      }
    } else if (this.comparisonData) {
      this.comparisonData = undefined;
      this.publishState();
    }
  }

  setComparisonAction(invocationId: string, actionDigest: string): void {
    this.comparisonData = { invocationId, actionDigest };
    localStorage[ACTION_TO_COMPARE_LOCALSTORAGE_KEY] = JSON.stringify(this.comparisonData);
    this.publishState();
    alert_service.success("Action selected for comparison");
  }

  clearComparisonAction(): void {
    this.comparisonData = undefined;
    delete localStorage[ACTION_TO_COMPARE_LOCALSTORAGE_KEY];
    this.publishState();
  }

  getComparisonData(): ActionComparisonData | undefined {
    return this.comparisonData;
  }

  hasComparisonAction(): boolean {
    return Boolean(this.comparisonData?.invocationId && this.comparisonData?.actionDigest);
  }

  canCompareWith(invocationId: string, actionDigest: string): boolean {
    if (!this.hasComparisonAction()) return false;
    // Can't compare with itself
    return !(this.comparisonData?.invocationId === invocationId && this.comparisonData?.actionDigest === actionDigest);
  }

  private publishState(): void {
    this.subject.next(this.comparisonData || {});
    window.dispatchEvent(new Event("storage"));
  }

  subscribe(observer: (value: ActionComparisonData) => void): Subscription {
    return this.subject.subscribe(observer);
  }
}

const actionComparisonService: ActionComparisonService = new ActionComparisonService();
export default actionComparisonService;
