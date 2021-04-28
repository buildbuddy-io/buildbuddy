import { Subject } from "rxjs";

export type Alert = {
  message: string;
  type: AlertType;
};

export type AlertType = "error" | "success";

export class AlertService {
  alerts = new Subject<Alert>();

  show(alert: Alert) {
    this.alerts.next(alert);
  }
}

export default new AlertService();
