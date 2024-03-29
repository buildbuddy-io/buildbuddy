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
  currentOptions: string[];
  optionCache: Map<string, Promise<string[]>>;
}

export default class Picker extends React.Component<{}, State> {
  state: State = {
    isVisible: false,
    search: "",
    selectedIndex: 0,
    currentOptions: [],
    optionCache: new Map<string, Promise<string[]>>(),
  };

  ref = React.createRef<HTMLInputElement>();

  private subscription: Subscription = pickerService.pickers.subscribe(this.onPicker.bind(this));

  private onPicker(picker: PickerModel) {
    this.setState({
      isVisible: true,
      search: "",
      picker,
      optionCache: new Map<string, Promise<string[]>>(),
      currentOptions: [],
    });
    this.fetchOptions("");
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

  handleSearchChanged(value: string) {
    this.fetchOptions(value);
  }

  handleKeyUp(e: React.KeyboardEvent<HTMLInputElement>) {
    switch (e.key) {
      case "Enter":
        if (this.state.currentOptions.length > this.selectedIndex()) {
          this.handleOptionPicked(this.state.currentOptions[this.selectedIndex()]);
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
    return Math.min((this.state.currentOptions.length || 0) - 1, this.state.selectedIndex);
  }

  async fetchOptions(search: string) {
    this.setState({ search: search });
    let searchString = search.toLowerCase();
    let cachedValuePromise = this.state.optionCache.get(searchString);
    if (cachedValuePromise) {
      // If we have a cached option value, use it.
      let cachedValue = await cachedValuePromise;
      if (search != this.state.search) {
        // If the search query has changed since we kicked off the request, don't update the state
        return;
      }
      this.setState({ currentOptions: cachedValue });
      return;
    }

    // If we have an options function, use that.
    if (this.state.picker?.fetchOptions) {
      let resultsPromise = this.state.picker.fetchOptions(searchString);
      this.state.optionCache.set(searchString, resultsPromise);
      let results = await resultsPromise;
      if (search != this.state.search) {
        // If the search query has changed since we kicked off the request, don't update the state
        return;
      }
      this.setState({ currentOptions: results });
      return;
    }

    // If we don't have cached options, or an options function - do a simple text search of the fixed options.
    this.setState({
      currentOptions:
        this.state.picker?.options?.filter((o) => o.toLowerCase().includes(this.state.search.toLowerCase())) || [],
    });
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
                onChange={(e) => this.handleSearchChanged(e.target.value)}
                placeholder={this.state.picker?.placeholder}
              />
            </div>
            <div className="picker-options">
              <div className="picker-options-label">{this.state.picker?.title}</div>
              {this.state.currentOptions.map((o, index) => (
                <div
                  className={`picker-option ${index == this.selectedIndex() ? "selected" : ""}`}
                  onMouseOver={() => this.setState({ selectedIndex: index })}
                  onClick={this.handleOptionPicked.bind(this, o)}>
                  {o}
                </div>
              ))}
              {!Boolean(this.state.currentOptions.length) && (
                <div className="picker-option">
                  {this.state.picker?.emptyState ? this.state.picker.emptyState : <>No matches found.</>}
                </div>
              )}
              {Boolean(this.state.currentOptions.length) && this.state.picker?.footer && (
                <div className="picker-option">{this.state.picker?.footer}</div>
              )}
            </div>
          </div>
        </div>
      </Modal>
    );
  }
}
