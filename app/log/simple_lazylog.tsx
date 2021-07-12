import React from "react";
import { LazyLog } from "react-lazylog";
import { Fetcher } from "./log";

const SCROLL_TOP_THRESHOLD_PX = 0;

export type SimpleLazyLogProps = {
  initialValue?: string;
  fetcher?: Fetcher;
  tailPollIntervalMs?: number;
};

export type SimpleLazyLogState = {
  value?: string;
};

export default class SimpleLazyLog extends React.Component<SimpleLazyLogProps, SimpleLazyLogState> {
  state: SimpleLazyLogState = {};

  private containerRef = React.createRef<HTMLDivElement>();
  private isFetching = false;

  private scrollBottomRestorePosition: number | null = null;
  private didScrollToBottom = false;
  private tailPollInterval: number | null = null;

  constructor(props: SimpleLazyLogProps) {
    super(props);
    this.state.value = props.initialValue || "";
  }

  componentDidMount() {
    this.scrollToBottomInitially();
  }

  componentDidUpdate() {
    this.scrollToBottomInitially();
    this.restoreScrollPosition();
  }

  // scrollToBottomInitially scrolls the logs to the bottom when they are first
  // rendered. This is needed because we can't use `follow={true}` on LazyLog.
  // `follow={true}` causes the logs to scroll to the bottom every time we
  // append a value.
  private scrollToBottomInitially() {
    if (!this.props.initialValue || this.didScrollToBottom) return;

    // TODO: See if we can remove this hacky timeout
    setTimeout(() => {
      const scrollingElement = this.getScrollingElement();
      scrollingElement.scrollTop = scrollingElement.scrollHeight;
      this.didScrollToBottom = true;
    }, 50);
  }

  private getScrollingElement() {
    console.log({ scrollingElement: this.containerRef.current.querySelector(".react-lazylog") });
    return this.containerRef.current.querySelector(".react-lazylog");
  }

  private restoreScrollPosition() {
    if (this.scrollBottomRestorePosition === null) return;

    const scrollingElement = this.getScrollingElement();
    scrollingElement.scrollTop = scrollingElement.scrollHeight - this.scrollBottomRestorePosition;
    this.scrollBottomRestorePosition = null;
  }

  private onScroll() {
    if (this.scrollBottomRestorePosition !== null || !this.didScrollToBottom) return;

    const scrollingElement = this.getScrollingElement();
    if (
      this.isFetching ||
      !this.props.fetcher?.hasPreviousChunk() ||
      scrollingElement.scrollTop > SCROLL_TOP_THRESHOLD_PX
    ) {
      return;
    }

    this.isFetching = true;
    this.props.fetcher
      ?.fetchPreviousChunk()
      .then((lines) => {
        this.scrollBottomRestorePosition = scrollingElement.scrollHeight - scrollingElement.scrollTop;
        this.setState({ value: lines.join("\n") + this.state.value });
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
      })
      .finally(() => {
        this.isFetching = false;
      });
  }

  render() {
    return (
      <div className="terminal" ref={this.containerRef} onScroll={this.onScroll.bind(this)}>
        <LazyLog
          selectableLines={true}
          caseInsensitive={true}
          rowHeight={20}
          lineClassName="terminal-line"
          // `follow={true}` causes the logs to scroll to the bottom every time we
          // change the value (not desirable when prepending).
          follow={false}
          text={this.state.value || "(No logs)"}
        />
      </div>
    );
  }
}
