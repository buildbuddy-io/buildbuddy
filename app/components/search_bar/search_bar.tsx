import { Search } from "lucide-react";
import React from "react";

interface Props<T> {
  title: string;
  placeholder: string;
  noResults?: any;
  emptyState?: any;
  footer?: any;

  fetchResults: (search: string) => Promise<T[]>;
  renderResult?: (result: T) => JSX.Element;
  onResultPicked: (result: T, query: string) => void;
}

interface State<T> {
  isVisible: boolean;
  search: string;
  selectedIndex: number;
  results: T[];
  resultCache: Map<any, Promise<T[]>>;
  focused: boolean;
  searching: boolean;
}

export default class SearchBar<T> extends React.Component<Props<T>, State<T>> {
  state: State<T> = {
    isVisible: false,
    search: "",
    selectedIndex: 0,
    results: [],
    resultCache: new Map<string, Promise<T[]>>(),
    focused: false,
    searching: false,
  };

  ref = React.createRef<HTMLInputElement>();
  resultsRef = React.createRef<HTMLInputElement>();

  handleResultPicked(result: T) {
    this.props.onResultPicked && this.props.onResultPicked(result, this.state.search);
  }

  handleDismissed() {
    this.setState({ isVisible: false });
  }

  handleSearchChanged(value: string) {
    this.fetchResults(value);
  }

  handleKeyUp(e: React.KeyboardEvent<HTMLInputElement>) {
    switch (e.key) {
      case "Enter":
        if (this.state.results.length > this.selectedIndex()) {
          this.handleResultPicked(this.state.results[this.selectedIndex()]);
          e.preventDefault();
        }
        break;
      case "ArrowDown":
        this.setState({ selectedIndex: Math.min(this.state.selectedIndex + 1, this.state.results.length - 1) });
        this.resultsRef.current?.children[this.state.selectedIndex - 1]?.scrollIntoView(true);
        e.preventDefault();
        break;
      case "ArrowUp":
        this.setState({ selectedIndex: Math.max(0, this.state.selectedIndex - 1) });
        this.resultsRef.current?.children[this.state.selectedIndex - 1]?.scrollIntoView(true);
        e.preventDefault();
        break;
    }
  }

  selectedIndex() {
    return Math.min((this.state.results.length || 0) - 1, this.state.selectedIndex);
  }

  async fetchResults(search: string) {
    this.setState({ search: search, searching: true });

    if (search == "") {
      this.setState({ results: [], searching: false });
      return;
    }

    let searchString = search.toLowerCase();
    let cachedValuePromise = this.state.resultCache.get(searchString);
    if (cachedValuePromise) {
      // If we have a cached result, use it.
      let cachedValue = await cachedValuePromise;
      if (search != this.state.search) {
        // If the search query has changed since we kicked off the request, don't update the state
        return;
      }
      this.setState({ results: cachedValue, searching: false });
      return;
    }

    let resultsPromise = this.props.fetchResults(searchString);
    this.state.resultCache.set(searchString, resultsPromise);
    let results = await resultsPromise;
    if (search != this.state.search) {
      // If the search query has changed since we kicked off the request, don't update the state
      return;
    }
    this.setState({ results: results, searching: false });
  }

  render() {
    return (
      <div className={`search-bar-container ${this.state.focused ? "focused" : ""}`}>
        <div onClick={(e) => e.stopPropagation()} className={`search-bar`}>
          <div className="search-bar-input">
            <Search />
            <input
              onFocus={() => this.setState({ focused: true })}
              onBlur={() => this.setState({ focused: false })}
              onKeyUp={this.handleKeyUp.bind(this)}
              value={this.state.search}
              ref={this.ref}
              onChange={(e) => this.handleSearchChanged(e.target.value)}
              placeholder={this.props?.placeholder}
            />
          </div>
          {this.state.focused && (
            <div className="search-bar-results" ref={this.resultsRef}>
              {!this.state.searching && !Boolean(this.state.results.length) && Boolean(this.state.search) && (
                <div className="search-bar-results-label">{this.props?.title}</div>
              )}
              {this.state.results.map((result, index) => (
                <div
                  className={`search-bar-result ${index == this.selectedIndex() ? "selected" : ""}`}
                  onMouseOver={() => this.setState({ selectedIndex: index })}
                  onMouseDown={() => this.handleResultPicked(result)}>
                  {this.props.renderResult ? this.props.renderResult(result) : result}
                </div>
              ))}
              {!this.state.searching && !Boolean(this.state.results.length) && Boolean(this.state.search) && (
                <div className="search-bar-result">
                  {this.props?.noResults ? this.props.noResults : <>No matches found.</>}
                </div>
              )}
              {this.state.searching && <div className="search-bar-result">Searching...</div>}
              {!Boolean(this.state.search) && (
                <div className="search-bar-result">
                  {this.props?.emptyState ? this.props.emptyState : <>No matches found.</>}
                </div>
              )}
              {Boolean(this.state.results.length) && this.props?.footer && (
                <div className="search-bar-result">{this.props?.footer}</div>
              )}
            </div>
          )}
        </div>
      </div>
    );
  }
}
