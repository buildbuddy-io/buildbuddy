import { Subject } from "rxjs";

export type Alert = {
  message: string;
  type: AlertType;
};

export type AlertType = "error" | "success" | "warning" | "info";

export class AlertService {
  alerts = new Subject<Alert>();

  show(alert: Alert) {
    this.alerts.next(alert);
  }

  // Convenience methods for different alert types:

  success(message: string) {
    this.show({ message, type: "success" });
  }

  warning(message: string) {
    this.show({ message, type: "warning" });
  }

  error(message: string) {
    this.show({ message, type: "error" });
  }

  loading() {
    this.show({ message: "Loading...", type: "info" });
  }
}

export default new AlertService();
