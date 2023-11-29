import { Subject } from "rxjs";

export type PickerModel = {
  title: string;
  placeholder: string;
  options: string[];
  emptyState?: any;
  footer?: any;
};

export class PickerService {
  pickers = new Subject<PickerModel>();
  picked = new Subject<string>();
  dismissed = new Subject<string>();

  show(picker: PickerModel) {
    let promise = new Promise<string>((resolve, reject) => {
      this.picked.subscribe((option) => resolve(option));
      this.dismissed.subscribe(() => reject());
    });
    this.pickers.next(picker);
    return promise;
  }
}

export default new PickerService();
