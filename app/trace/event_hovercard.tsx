import React from "react";
import { TraceEvent } from "./trace_events";
import { truncateDecimals } from "../util/math";

export interface EventHovercardProps {
  buildDuration: number;
}

interface EventHovercardState {
  data?: HovercardData | null;
}

export interface HovercardData {
  x: number;
  y: number;
  event: TraceEvent;
}

/**
 * Hovercard displaying info about the hovered event in the trace viewer.
 */
export default class EventHovercard extends React.Component<EventHovercardProps, EventHovercardState> {
  state: EventHovercardState = {};
  private ref = React.createRef<HTMLDivElement>();

  private width = 0;

  componentDidUpdate(prevProps: EventHovercardProps, prevState: EventHovercardState) {
    const card = this.ref.current;
    if (!card) return;

    if (this.state.data && this.state.data.event !== prevState.data?.event) {
      // When the event that we're rendering changes, recompute the card
      // dimensions by letting the card render at full width, invisible, at the
      // upper-left hand corner of the window.
      card.style.opacity = "0";
      card.style.top = "0";
      card.style.width = "";
      this.width = card.getBoundingClientRect().width;
    }

    if (this.state.data) {
      const mouseMargin = 8;
      const top = this.state.data.y + mouseMargin;
      const left = this.state.data.x + mouseMargin;
      const right = left + this.width;
      const overflow = -Math.min(document.body.clientWidth - right, 0);
      card.style.left = `${left - overflow}px`;
      card.style.top = `${top}px`;
      card.style.opacity = "1";
    } else {
      card.style.opacity = "0";
    }
  }

  render() {
    if (!this.state.data?.event) return <></>;

    const event = this.state.data.event;
    const buildFraction = ((event.dur / this.props.buildDuration) * 100).toFixed(2);
    const percentage = buildFraction ? `${buildFraction}%` : "< 0.01%";
    const duration = truncateDecimals(event.dur / 1e6, 3);
    const displayedDuration = duration === 0 ? "< 0.001" : `${duration}`;

    return (
      <div
        className="trace-viewer-hovercard"
        ref={this.ref}
        style={{
          position: "fixed",
          pointerEvents: "none",
        }}>
        <div className="hovercard-title">{event.name}</div>
        <div className="hovercard-details">
          <div>
            {event.cat}
            {(event.args?.target && <div>Target: {event.args?.target}</div>) || ""}
            {(event.out && <div>Out: {event.out}</div>) || ""}
          </div>

          <div className="duration">
            <span className="data">{displayedDuration}</span> seconds total (<span className="data">{percentage}</span>{" "}
            of total build duration)
          </div>
          <div>
            @ {truncateDecimals(event.ts / 1e6, 3)}s &ndash; {truncateDecimals((event.ts + event.dur) / 1e6, 3)}s
          </div>
        </div>
      </div>
    );
  }
}
