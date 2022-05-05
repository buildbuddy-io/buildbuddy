import React from "react";
import ReactDOM from "react-dom";
import { clamp } from "../../util/math";

type TooltipProps = JSX.IntrinsicElements["div"] & {
  /** Renders the tooltip content if the tooltip is hovered. */
  renderContent: () => React.ReactNode;
  /** Specifies the pin position of the tooltip. */
  pin?: PinPositionFunc;
};

/**
 * `Tooltip` renders a `<div>` element that shows a tooltip when hovered.
 *
 * Example:
 *
 * ```tsx
 * <Tooltip renderContent={() => <div className="tooltip">Tooltip content</div>}>
 *   I will show a tooltip when hovered
 * </Tooltip>
 * ```
 */
export class Tooltip extends React.Component<TooltipProps> {
  private windowScrollListener?: (e: Event) => any;
  private contentRef = React.createRef<TooltipContent>();

  componentDidMount() {
    this.windowScrollListener = (e: Event) => {
      this.onMouseEvent(e as any);
    };
    window.addEventListener("wheel", this.windowScrollListener);
  }
  componentWillUnmount() {
    window.removeEventListener("wheel", this.windowScrollListener!);
  }

  onMouseEvent(e: React.MouseEvent) {
    const content = this.contentRef.current;
    if (!content) return;

    if (e.type === "mousemove" || e.type === "mouseenter") {
      content.setMousePosition(e.clientX, e.clientY);
      content.setState({ visible: true });
      return;
    }

    content.setState({ visible: false });
  }

  render() {
    const { renderContent, pin, ...props } = this.props;
    return (
      <>
        <div
          onMouseMove={this.onMouseEvent.bind(this)}
          onMouseEnter={this.onMouseEvent.bind(this)}
          onMouseLeave={this.onMouseEvent.bind(this)}
          {...props}></div>
        <TooltipContent ref={this.contentRef} render={this.props.renderContent} pin={this.props.pin} />
      </>
    );
  }
}

/**
 * Function that returns the screen coordinates of the position to which the
 * *upper left* corner of the tooltip should be pinned.
 */
type PinPositionFunc = (context: PinContext) => [left: number, top: number];

type PinContext = {
  tooltip: { clientWidth: number; clientHeight: number };
  mouse: { clientX: number; clientY: number };
};

type TooltipContentProps = {
  pin?: PinPositionFunc;
  render: () => React.ReactNode;
};

type TooltipContentState = {
  visible: boolean;
};

/** Pins the upper left corner of the tooltip to the mouse position. */
export const defaultPinPosition: PinPositionFunc = (ctx) => [ctx.mouse.clientX, ctx.mouse.clientY];

/** Pins the bottom-middle of the tooltip to the mouse position. */
export const pinBottomMiddleToMouse: PinPositionFunc = (ctx) => [
  ctx.mouse.clientX - ctx.tooltip.clientWidth / 2,
  ctx.mouse.clientY - ctx.tooltip.clientHeight,
];

/**
 * `TooltipContent` renders the actual content for the tooltip when the
 * corresponding `<HasTooltip>` component is being hovered.
 */
class TooltipContent extends React.Component<TooltipContentProps> {
  state: TooltipContentState = { visible: false };
  ref = React.createRef<HTMLDivElement>();

  private mouseX: number = 0;
  private mouseY: number = 0;
  private isLayoutCalculated = false;

  componentDidUpdate() {
    this.isLayoutCalculated = false;
    if (!this.state.visible) return;

    const el = this.ref.current;
    if (!el) return;
    // Position to upper left of screen, but hidden, so the browser can render
    // the component and determine its size. Then once the layout is calculated
    // and we know the dimensions of the element, update the position of the
    // card, constraining it to the viewport.
    el.style.opacity = "0";
    el.style.left = "0";
    el.style.top = "0";
    this.isLayoutCalculated = true;
    this.updatePosition();
  }

  setMousePosition(x: number, y: number) {
    this.mouseX = x;
    this.mouseY = y;

    if (!this.isLayoutCalculated) return false;
    this.updatePosition();
  }

  private updatePosition() {
    const el = this.ref.current;
    if (!el) return;

    const layoutContext: PinContext = {
      mouse: { clientX: this.mouseX, clientY: this.mouseY },
      tooltip: { clientWidth: el.clientWidth, clientHeight: el.clientHeight },
    };
    const layoutFunc = this.props.pin || defaultPinPosition;
    const [x, y] = layoutFunc(layoutContext);

    const viewportWidth = document.scrollingElement!.clientWidth;
    const viewportHeight = document.scrollingElement!.clientHeight;

    const left = clamp(x, 0, viewportWidth - el.clientWidth - 1);
    const top = clamp(y, 0, viewportHeight - el.clientHeight);

    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    el.style.opacity = "1";
  }

  private portalElement: HTMLElement | null = null;
  private getPortalElement() {
    if (this.portalElement) return this.portalElement;

    // Note: all tooltips share a single portal.
    let portal = document.getElementById("tooltip-portal") as HTMLDivElement | null;
    if (!portal) {
      portal = document.createElement("div");
      portal.id = "tooltip-portal";
      portal.style.position = "fixed";
      portal.style.zIndex = "1";
      document.body.appendChild(portal);
    }
    this.portalElement = portal;
    return portal;
  }

  render() {
    if (!this.state.visible) return null;

    return ReactDOM.createPortal(
      <div
        ref={this.ref}
        style={{
          position: "fixed",
          top: "0",
          left: "0",
          opacity: "0",
          pointerEvents: "none",
        }}>
        {this.state.visible ? this.props.render() : null}
      </div>,
      this.getPortalElement()
    );
  }
}
