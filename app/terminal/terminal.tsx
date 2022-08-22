import { WrapText, Download, ArrowUp, ArrowDown, X } from "lucide-react";
import React from "react";
import { FixedSizeList } from "react-window";
import memoizeOne from "memoize-one";
import errorService from "../errors/error_service";
import Spinner from "../components/spinner/spinner";
import TextInput from "../components/input/input";
import { mod } from "../util/math";
import { Scroller } from "../util/scroller";
import AutoSizer from "react-virtualized-auto-sizer";
import { Row, ROW_HEIGHT_PX } from "./row";
import { getContent, updatedMatchIndexForSearch, toPlainText, Range, ListData } from "./text";

const WRAP_LOCAL_STORAGE_KEY = "terminal-wrap";
const WRAP_LOCAL_STORAGE_VALUE = "wrap";
const CHARACTER_WIDTH_PX = 8.5;
// TODO: Make this a prop
const DEFAULT_VALUE = "No build logs...";
/** If there are at least this many rows, debounce search. */
const SEARCH_DEBOUNCE_THRESHOLD_ROWS = 25_000;
const SEARCH_DEBOUNCE_TIMEOUT_MS = 125;

export interface TerminalProps {
  value?: string;
  loading?: boolean;

  title?: JSX.Element;

  lightTheme?: boolean;
  fullLogsFetcher?: () => Promise<string>;
}

interface State {
  /**
   * Max number of characters in a line before the line is wrapped. This will be
   * `null` before the list element is rendered, since the limit depends on the
   * width of the rendered list.
   */
  lineLengthLimit: number | null;

  search: string;
  activeMatchIndex: number;

  isLoadingFullLog: boolean;
}

/** DOM snapshot returned by the `getSnapshotBeforeUpdate` lifecycle method. */
interface Snapshot {
  /** List element client height. */
  clientHeight: number;
  /** List element scrollTop. */
  scrollTop: number;
  /** List element scrollHeight. */
  scrollHeight: number;
}

export default class TerminalComponent extends React.Component<TerminalProps, State, Snapshot> {
  state: State = {
    lineLengthLimit: null,
    isLoadingFullLog: false,

    search: "",
    activeMatchIndex: -1,
  };

  private terminalRef = React.createRef<HTMLDivElement>();
  private searchInputRef = React.createRef<HTMLInputElement>();
  private list: FixedSizeList<ListData> | null = null;
  private listEl: HTMLDivElement | null = null;

  private isMouseInside = false;
  private windowKeyDownListener: (this: Window, ev: KeyboardEvent) => any;

  private scroller = new Scroller(() => {
    const list = this.list;
    const listEl = this.listEl;
    if (!list || !listEl) return null;
    return {
      set scrollTop(value: number) {
        list.scrollTo(value);
      },
      get scrollHeight() {
        return listEl.scrollHeight;
      },
      get clientHeight() {
        return listEl.clientHeight;
      },
    };
  });

  componentDidMount() {
    this.initialScrollToEnd();
    window.addEventListener("keydown", (this.windowKeyDownListener = this.onWindowKeyDown.bind(this)));
  }

  componentWillUnmount() {
    window.removeEventListener("keydown", this.windowKeyDownListener);
  }

  componentDidUpdate(_prevProps: TerminalProps, prevState: State, snapshot: Snapshot | null) {
    this.initialScrollToEnd();
    // If the active match changed, scroll to it.
    if (this.state.activeMatchIndex !== prevState.activeMatchIndex) {
      this.scrollToActiveMatch();
    } else if (
      snapshot &&
      this.listEl &&
      snapshot.scrollTop === snapshot.scrollHeight - snapshot.clientHeight &&
      snapshot.scrollHeight !== this.listEl.scrollHeight
    ) {
      // If we are already scrolled to the bottom, keep the scroll position stuck
      // there as new logs come in.
      this.scrollToEnd();
    }
  }

  getSnapshotBeforeUpdate(): Snapshot | null {
    if (!this.listEl) return null;

    return {
      scrollHeight: this.listEl.scrollHeight,
      scrollTop: this.listEl.scrollTop,
      clientHeight: this.listEl.clientHeight,
    };
  }

  private setListElement(el: HTMLDivElement) {
    this.listEl = el;
    // updateLineLengthLimit() triggers a component update which in turn causes
    // this setListElement() ref callback to be fired again, so we need a
    // conditional check around this update here to avoid an infinite loop.
    if (this.state.lineLengthLimit === null) {
      this.updateLineLengthLimit();
    }
  }
  private setList(list: FixedSizeList<ListData>) {
    this.list = list;
  }

  private searchTimeout: number | null = null;
  private onSearchChange(e: React.ChangeEvent<HTMLInputElement>) {
    const search = e.target.value;
    window.clearTimeout(this.searchTimeout);
    this.searchTimeout = window.setTimeout(
      () => {
        const content = this.getContent();
        const match = this.state.activeMatchIndex === -1 ? null : content.matches[this.state.activeMatchIndex];
        const nextContent = this.getContent(this.props.value, search);
        this.setState({
          search,
          activeMatchIndex: updatedMatchIndexForSearch(nextContent, search, match, this.getRowRangeInView()),
        });
      },
      // If logs are small, no need to debounce.
      this.getContent().rows.length < SEARCH_DEBOUNCE_THRESHOLD_ROWS ? 0 : SEARCH_DEBOUNCE_TIMEOUT_MS
    );
  }
  private onSearchKeyPress(e: React.KeyboardEvent<HTMLInputElement>) {
    // If not currently searching, do nothing.
    if (this.state.activeMatchIndex === -1) return;

    // Pressing Enter/Shift+Enter goes to the Next/Previous match, wrapping around when at the end.
    if (e.key === "Enter") {
      this.shiftActiveMatchIndex(e.shiftKey ? -1 : 1);
    }
  }
  private onClearSearchClick() {
    this.setState({ search: "", activeMatchIndex: -1 });
    const input = this.searchInputRef.current;
    if (input) {
      input.value = "";
      input.focus();
    }
  }

  /**
   * Wrapper around `this.memoizedGetContent` that provides useful default args.
   */
  private getContent(
    text = this.props.value || DEFAULT_VALUE,
    search = this.state.search,
    lineLengthLimit = this.state.lineLengthLimit
  ) {
    return this.memoizedGetContent(text, search, lineLengthLimit);
  }
  /**
   * memoizes getContent for a single output value per component instance. This
   * helps avoid the complexity of manually keeping the content in sync with
   * props and state.
   */
  private memoizedGetContent = memoizeOne(getContent);

  private getWrapPreference(): boolean {
    return localStorage.getItem(WRAP_LOCAL_STORAGE_KEY) === WRAP_LOCAL_STORAGE_VALUE;
  }
  private updateLineLengthLimit(): number | null {
    if (!this.listEl) return;
    this.setState({
      lineLengthLimit: this.getWrapPreference()
        ? Math.floor(this.listEl.clientWidth / CHARACTER_WIDTH_PX)
        : Number.MAX_SAFE_INTEGER,
    });
  }

  /**
   * Returns the start (inclusive) and end (exclusive) indexes of the range of
   * lines that are in fully in view (indexes are post-wrap).
   */
  private getRowRangeInView(): Range {
    if (!this.listEl) return null;

    const start = Math.ceil(this.listEl.scrollTop / ROW_HEIGHT_PX);
    const end = Math.floor((this.listEl.scrollTop + this.listEl.scrollHeight) / ROW_HEIGHT_PX);
    const content = this.getContent();
    return { start, end: Math.min(content.rows.length, end) };
  }

  private scrollToActiveMatch() {
    if (this.state.activeMatchIndex === -1) return;

    const content = this.getContent();
    this.scrollToRow(content.matches[this.state.activeMatchIndex].rowIndex);
  }

  /**
   * Scrolls the row into view. The resulting alignment of the row depends on
   * whether the line is already in view.
   */
  private scrollToRow(index: number) {
    if (!this.list || !this.listEl) return;

    // NOTE: Not using `FixedSizeList.scrollTo()` here because it doesn't take
    // the bottom horizontal scrollbar into account, so when the line we're
    // scrolling to is near the bottom, the scrollbar winds up covering part of
    // the line.

    // Note: to avoid confusion, the term "scrollTop" is abbreviated here as
    // "y".
    const yListTop = this.listEl.scrollTop;
    const yListBottom = yListTop + this.listEl.clientHeight;
    const yRowTop = index * ROW_HEIGHT_PX;
    const yRowBottom = yRowTop + ROW_HEIGHT_PX;

    // By default (if the line is already fully in view) do nothing.
    let target = yListTop;
    // Otherwise vertically center the line within the viewport.
    if (yRowTop < yListTop || yRowBottom > yListBottom) {
      target = yRowTop + ROW_HEIGHT_PX / 2 - this.listEl.clientHeight / 2;
    }

    this.scroller.scrollTo(target);
  }

  private didInitialScrollToEnd = false;
  private initialScrollToEnd() {
    if (this.didInitialScrollToEnd || this.props.loading || !this.listEl || !this.list) {
      return;
    }
    this.didInitialScrollToEnd = true;
    this.scrollToEnd();
  }

  private scrollToEnd() {
    this.scroller.scrollTo(this.scroller.getMax(), { animate: false });
  }

  private onMouseEnter() {
    this.isMouseInside = true;
  }
  private onMouseLeave() {
    this.isMouseInside = false;
  }
  private onWindowKeyDown(e: KeyboardEvent) {
    // When pressing Ctrl+F with the mouse inside the terminal, focus the search box.
    if (this.isMouseInside && this.searchInputRef.current && (e.ctrlKey || e.metaKey) && !e.shiftKey && e.key === "f") {
      e.preventDefault();
      this.searchInputRef.current.focus();
    }
  }

  private onPreviousMatchClick() {
    this.shiftActiveMatchIndex(-1);
  }
  private onNextMatchClick() {
    this.shiftActiveMatchIndex(+1);
  }
  private shiftActiveMatchIndex(offset: number) {
    const content = this.getContent();
    const activeMatchIndex = mod(this.state.activeMatchIndex + offset, content.matches.length);
    this.setState({ activeMatchIndex });
  }

  private onWrapClick() {
    const wrap = !this.getWrapPreference();
    localStorage.setItem(WRAP_LOCAL_STORAGE_KEY, wrap ? WRAP_LOCAL_STORAGE_VALUE : undefined);
    this.updateLineLengthLimit();
  }
  private onDownloadClick() {
    const serveLog = (log: string) => {
      const element = document.createElement("a");
      const plaintext = toPlainText(log);
      element.setAttribute("href", "data:text/plain;charset=utf-8," + encodeURIComponent(plaintext));
      element.setAttribute("download", "build_logs.txt");
      element.click();
    };
    if (this.props.fullLogsFetcher) {
      this.setState({ isLoadingFullLog: true });
      this.props
        .fullLogsFetcher()
        .then(serveLog)
        .catch((e) => errorService.handleError(e))
        .finally(() => {
          this.setState({ isLoadingFullLog: false });
        });
      return;
    }
    serveLog(this.props.value);
  }

  render() {
    const content = this.getContent();
    const iconClass = this.props.lightTheme ? "" : "white";

    return (
      <div
        className={`terminal ${this.props.lightTheme ? "light-terminal" : ""}`}
        onMouseEnter={this.onMouseEnter.bind(this)}
        onMouseLeave={this.onMouseLeave.bind(this)}>
        <div className="terminal-top-bar">
          {this.props.title && <div className="terminal-titles">{this.props.title}</div>}
          <div className="terminal-actions">
            <div className="terminal-search">
              <TextInput
                ref={this.searchInputRef}
                className={`terminal-search-input ${this.props.lightTheme ? "" : "dark"}`}
                placeholder="Search"
                onChange={this.onSearchChange.bind(this)}
                onKeyPress={this.onSearchKeyPress.bind(this)}
                spellCheck={false}
              />
              <div className="search-result-count">
                {content.matches?.length > 0 ? (
                  <span>
                    {this.state.activeMatchIndex + 1} of {content.matches.length}
                  </span>
                ) : (
                  <span className="no-results">No results</span>
                )}
              </div>
              <div className="search-navigation">
                <button
                  title="Previous match (Shift+Enter)"
                  disabled={content.matches.length <= 1}
                  className={`terminal-action ${content.matches.length ? "active" : ""}`}
                  onClick={this.onPreviousMatchClick.bind(this)}>
                  <ArrowUp className={`icon ${iconClass}`} />
                </button>
                <button
                  title="Next match (Enter)"
                  disabled={content.matches.length <= 1}
                  className={`terminal-action ${content.matches.length ? "active" : ""}`}
                  onClick={this.onNextMatchClick.bind(this)}>
                  <ArrowDown className={`icon ${iconClass}`} />
                </button>
                <button
                  title="Clear search"
                  disabled={!this.state.search}
                  className={`terminal-action ${this.state.search ? "active" : ""}`}
                  onClick={this.onClearSearchClick.bind(this)}>
                  <X className={`icon ${iconClass}`} />
                </button>
              </div>
            </div>
            <button
              title="Wrap"
              onClick={this.onWrapClick.bind(this)}
              className={`terminal-action ${this.getWrapPreference() ? "active" : ""}`}>
              <WrapText className={`icon ${iconClass}`} />
            </button>
            <button
              title="Download"
              onClick={this.onDownloadClick.bind(this)}
              className="terminal-action active"
              disabled={this.state.isLoadingFullLog}>
              {this.state.isLoadingFullLog ? (
                <Spinner className={iconClass} />
              ) : (
                <Download className={`icon ${iconClass}`} />
              )}
            </button>
          </div>
        </div>
        <div className="terminal-text" ref={this.terminalRef}>
          {this.props.loading ? (
            <div className={`loading ${this.props.lightTheme ? "" : "loading-dark-terminal"}`} />
          ) : (
            <AutoSizer>
              {({ height, width }) => (
                <FixedSizeList<ListData>
                  ref={(list) => this.setList(list)}
                  outerRef={(el) => this.setListElement(el)}
                  height={height}
                  width={width}
                  className="lines-list"
                  itemSize={ROW_HEIGHT_PX}
                  itemCount={content.rows.length}
                  itemData={{
                    rows: content.rows,
                    rowLength: this.state.lineLengthLimit,
                    search: this.state.search,
                    activeMatchIndex: this.state.activeMatchIndex,
                  }}>
                  {Row}
                </FixedSizeList>
              )}
            </AutoSizer>
          )}
        </div>
      </div>
    );
  }
}

export { TerminalComponent };
