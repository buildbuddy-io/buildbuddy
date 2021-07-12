import React from "react";

export type LogProps = {
  dark?: boolean;
  initialLines: string[];
  fetcher?: Fetcher;
  tailPollIntervalMs: number;
};

export interface Fetcher {
  hasPreviousChunk(): boolean;
  fetchPreviousChunk(): Promise<string[]>;
  hasNextChunk(): boolean;
  fetchNextChunk(): Promise<string[]>;
}

const SCROLL_TOP_THRESHOLD_PX = 512;

export class Log extends React.Component<LogProps> {
  constructor(props: LogProps) {
    super(props);
  }

  private containerRef = React.createRef<HTMLDivElement>();
  private linesContainerRef = React.createRef<HTMLDivElement>();
  private didScrollToBottom = false;
  private scrollBottomRestorePosition: number | null = null;
  private tailPollInterval: number | null = null;
  private isFetching = false;

  shouldComponentUpdate() {
    // Never let React re-render this component (for performance reasons).
    return false;
  }

  componentDidMount() {
    this.appendLines(this.props.initialLines);
    this.updateScrollPosition();
    this.onScroll();

    this.tailPollInterval = window.setInterval(() => {
      if (!this.props.fetcher?.hasNextChunk()) {
        clearInterval(this.tailPollInterval);
        this.tailPollInterval = null;
        return;
      }
      this.fetchTail();
    }, 500);
  }

  private updateScrollPosition() {
    this.scrollToBottomInitially();
    this.restoreScrollPosition();
  }

  private prependLines(lines: string[]) {
    for (let i = lines.length - 1; i >= 0; i--) {
      const el = this.createLineElement(lines[i]);
      this.linesContainerRef.current.prepend(el);
    }
  }

  private appendLines(lines: string[]) {
    for (const line of lines) {
      const el = this.createLineElement(line);
      this.linesContainerRef.current.append(el);
    }
  }

  private createLineElement(line: string): HTMLElement {
    const el = document.createElement("div");
    el.className = "log-line";
    el.innerHTML = line;
    return el;
  }

  private scrollToBottomInitially() {
    if (!this.props.initialLines || this.didScrollToBottom) return;
    this.containerRef.current.scrollTop = this.containerRef.current.scrollHeight;
    this.didScrollToBottom = true;
  }

  private restoreScrollPosition() {
    if (this.scrollBottomRestorePosition === null) return;

    this.containerRef.current.scrollTop = this.containerRef.current.scrollHeight - this.scrollBottomRestorePosition;
    this.scrollBottomRestorePosition = null;
  }

  private onScroll() {
    if (
      this.isFetching ||
      this.containerRef.current.scrollTop > SCROLL_TOP_THRESHOLD_PX ||
      !this.props.fetcher?.hasPreviousChunk()
    ) {
      return;
    }

    this.isFetching = true;
    this.props.fetcher
      ?.fetchPreviousChunk()
      .then((lines) => {
        this.scrollBottomRestorePosition = this.containerRef.current.scrollHeight - this.containerRef.current.scrollTop;
        this.prependLines(lines);
        this.updateScrollPosition();
      })
      .finally(() => {
        this.isFetching = false;
      });
  }

  private fetchTail() {
    if (this.isFetching) return;

    this.isFetching = true;
    this.props.fetcher
      ?.fetchNextChunk()
      .then((lines) => {
        if (
          this.containerRef.current.scrollTop + this.containerRef.current.clientHeight >=
          this.containerRef.current.scrollHeight
        ) {
          this.scrollBottomRestorePosition = 0;
        }
        this.appendLines(lines);
        this.updateScrollPosition();
      })
      .finally(() => {
        this.isFetching = false;
      });
  }

  render() {
    return (
      <div
        ref={this.containerRef}
        onScroll={this.onScroll.bind(this)}
        className={`log code ${this.props.dark ? "dark-log" : "light-log"}`}>
        <div ref={this.linesContainerRef} className="log-lines" />
      </div>
    );
  }
}
