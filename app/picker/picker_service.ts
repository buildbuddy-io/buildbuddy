import { Subject } from "rxjs";

export type Picker = {
  title: string;
  placeholder: string;
  options: string[];
};

export class PickerService {
  pickers = new Subject<Picker>();
  picked = new Subject<string>();
  dismissed = new Subject<string>();

  show(picker: Picker) {
    let promise = new Promise<string>((resolve, reject) => {
      this.picked.subscribe((option) => resolve(option));
      this.dismissed.subscribe(() => reject());
    });
    this.pickers.next(picker);
    return promise;
  }
}

export default new PickerService();
