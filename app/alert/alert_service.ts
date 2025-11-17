import { Subject } from "rxjs";

export type Alert = {
  message: string;
  type: AlertType;
};

export type AlertType = "error" | "success" | "warning" | "info";

export class AlertService {
  alerts: Subject<Alert> = new Subject<Alert>();

  show(alert: Alert): void {
    this.alerts.next(alert);
  }

  // Convenience methods for different alert types:

  success(message: string): void {
    this.show({ message, type: "success" });
  }

  warning(message: string): void {
    this.show({ message, type: "warning" });
  }

  error(message: string): void {
    this.show({ message, type: "error" });
  }

  loading(): void {
    this.show({ message: "Loading...", type: "info" });
  }
}

const alertService: AlertService = new AlertService();
export default alertService;
