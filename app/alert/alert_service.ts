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

  // Convenience methods for different alert types:

  success(message: string) {
    this.show({ message, type: "success" });
  }

  error(message: string) {
    this.show({ message, type: "error" });
  }
}

export default new AlertService();
