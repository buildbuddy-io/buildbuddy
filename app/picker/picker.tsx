import { Search } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import Modal from "../components/modal/modal";
import pickerService, { PickerModel } from "./picker_service";

interface State {
  picker?: PickerModel;
  isVisible: boolean;
  search: string;
  selectedIndex: number;
}

export default class Picker extends React.Component<{}, State> {
  state: State = {
    isVisible: false,
    search: "",
    selectedIndex: 0,
  };

  ref = React.createRef<HTMLInputElement>();

  private subscription: Subscription = pickerService.pickers.subscribe(this.onPicker.bind(this));

  private onPicker(picker: PickerModel) {
    this.setState({ isVisible: true, search: "", picker });
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  handleOptionPicked(o: string) {
    pickerService.picked.next(o);
    this.setState({ isVisible: false });
  }

  handleDismissed() {
    pickerService.dismissed.next();
    this.setState({ isVisible: false });
  }

  handleKeyUp(e: React.KeyboardEvent<HTMLInputElement>) {
    let matchingOptions = this.matchingOptions();
    switch (e.key) {
      case "Enter":
        if (matchingOptions && matchingOptions.length > this.selectedIndex()) {
          this.handleOptionPicked(matchingOptions[this.selectedIndex()]);
          e.preventDefault();
        }
        break;
      case "ArrowDown":
        this.setState({ selectedIndex: this.state.selectedIndex + 1 });
        e.preventDefault();
        break;
      case "ArrowUp":
        this.setState({ selectedIndex: Math.max(0, this.state.selectedIndex - 1) });
        e.preventDefault();
        break;
    }
  }

  selectedIndex() {
    return Math.min((this.matchingOptions()?.length || 0) - 1, this.state.selectedIndex);
  }

  matchingOptions() {
    return this.state.picker?.options.filter((o) => o.includes(this.state.search));
  }

  render() {
    return (
      <Modal
        isOpen={this.state.isVisible}
        onAfterOpen={() => this.ref.current?.focus()}
        onRequestClose={() => this.setState({ isVisible: false })}>
        <div className="picker-container">
          <div onClick={(e) => e.stopPropagation()} className={`picker`}>
            <div className="picker-search">
              <Search />
              <input
                onKeyUp={this.handleKeyUp.bind(this)}
                value={this.state.search}
                ref={this.ref}
                onChange={(e) => this.setState({ search: e.target.value })}
                placeholder={this.state.picker?.placeholder}
              />
            </div>
            <div className="picker-options">
              <div className="picker-options-label">{this.state.picker?.title}</div>
              {this.matchingOptions()?.map((o, index) => (
                <div
                  className={`picker-option ${index == this.selectedIndex() ? "selected" : ""}`}
                  onMouseOver={() => this.setState({ selectedIndex: index })}
                  onClick={this.handleOptionPicked.bind(this, o)}>
                  {o}
                </div>
              ))}
              {!Boolean(this.matchingOptions()?.length) && (
                <div className="picker-option">
                  {this.state.picker?.emptyState ? this.state.picker.emptyState : <>No matches found.</>}
                </div>
              )}
              {Boolean(this.matchingOptions()?.length) && this.state.picker?.footer && (
                <div className="picker-option">{this.state.picker?.footer}</div>
              )}
            </div>
          </div>
        </div>
      </Modal>
    );
  }
}
