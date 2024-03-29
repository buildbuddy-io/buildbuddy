import { Subject } from "rxjs";

export type PickerModel = {
  title: string;
  placeholder: string;
  emptyState?: any;
  footer?: any;

  // Either a fixed set of options, or a function to fetch options is required.
  options?: string[];
  fetchOptions?: (search: string) => Promise<string[]>;
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
