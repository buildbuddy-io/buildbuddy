import React from "react";

const VIEWPORT_MARGIN = 8;

/** Props for a popup anchored to its parent element. */
export type PopupProps = React.ComponentPropsWithoutRef<"div"> & {
  isOpen: boolean;
  onRequestClose: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void;
  anchor?: "left" | "right" | "center" | "center-right";
};

/**
 * A fixed popup anchored to its parent element.
 *
 * Keep the popup and its trigger in the same wrapper.
 * The popup stays in the DOM below that wrapper to preserve inherited styles,
 * but uses viewport coordinates to escape scrolling containers.
 */
export const Popup = React.forwardRef<HTMLDivElement, PopupProps>(({ isOpen, ...props }, ref) =>
  isOpen ? <OpenPopup {...props} forwardedRef={ref} /> : null
);

class OpenPopup extends React.Component<
  Omit<PopupProps, "isOpen"> & { forwardedRef: React.ForwardedRef<HTMLDivElement> }
> {
  private element: HTMLDivElement | null = null;
  private animationFrame = 0;

  componentDidMount() {
    // Reflow and animations can move the anchor without resizing it or firing scroll events.
    this.trackPosition();
  }

  componentDidUpdate() {
    this.updatePosition();
  }

  componentWillUnmount() {
    cancelAnimationFrame(this.animationFrame);
  }

  private trackPosition = () => {
    this.updatePosition();
    this.animationFrame = requestAnimationFrame(this.trackPosition);
  };

  private updatePosition() {
    const element = this.element;
    if (!element?.parentElement) return;

    const bounds = element.parentElement.getBoundingClientRect();
    const viewportHeight = document.documentElement.clientHeight;
    const anchorTop = Math.max(VIEWPORT_MARGIN, Math.min(bounds.top, viewportHeight - VIEWPORT_MARGIN));
    const anchorBottom = Math.max(VIEWPORT_MARGIN, Math.min(bounds.bottom, viewportHeight - VIEWPORT_MARGIN));
    const spaceAbove = Math.max(0, anchorTop - VIEWPORT_MARGIN);
    const spaceBelow = Math.max(0, viewportHeight - VIEWPORT_MARGIN - anchorBottom);

    // Fixed popups can't extend the page's scrollable area. Prefer the side with
    // more room when content doesn't fit below, and let oversized content scroll.
    const popupHeight = element.scrollHeight + element.offsetHeight - element.clientHeight;
    const above = popupHeight > spaceBelow && spaceAbove > spaceBelow;
    const top = above ? "auto" : `${anchorBottom}px`;
    const bottom = above ? `${viewportHeight - anchorTop}px` : "auto";
    const maxHeight = `${above ? spaceAbove : spaceBelow}px`;
    const anchor = this.props.anchor ?? "right";
    let left = "auto";
    let right = "auto";
    if (anchor === "left" || anchor === "center") {
      left = `${bounds.left + (anchor === "center" ? bounds.width / 2 : 0)}px`;
    } else {
      right = `${document.documentElement.clientWidth - bounds.right}px`;
    }

    // Avoid invalidating styles on every frame when the anchor hasn't moved.
    if (element.style.top !== top) element.style.top = top;
    if (element.style.bottom !== bottom) element.style.bottom = bottom;
    if (element.style.maxHeight !== maxHeight) element.style.maxHeight = maxHeight;
    if (element.style.left !== left) element.style.left = left;
    if (element.style.right !== right) element.style.right = right;
  }

  render() {
    const { onRequestClose, className, anchor = "right", forwardedRef, ...props } = this.props;
    return (
      <>
        <div className="popup-shade" onClick={onRequestClose} />
        <div
          ref={(element) => {
            this.element = element;
            if (typeof forwardedRef === "function") {
              forwardedRef(element);
            } else if (forwardedRef) {
              forwardedRef.current = element;
            }
          }}
          className={`popup anchor-${anchor} ${className || ""}`}
          onClick={(e) => e.preventDefault()}
          {...props}
        />
      </>
    );
  }
}

/** Props for the wrapper shared by a popup and its trigger. */
export type PopupContainerProps = JSX.IntrinsicElements["div"];

/**
 * PopupContainer wraps an element that triggers a popup, as well the popup
 * itself.
 */
// TODO: Replace callers with plain divs and remove PopupContainer and its CSS,
// since fixed positioning no longer needs a positioned parent.
export const PopupContainer = React.forwardRef(
  ({ className, ...props }: PopupContainerProps, ref: React.Ref<HTMLDivElement>) => {
    return <div ref={ref} className={`popup-container ${className || ""}`} {...props} />;
  }
);

export default Popup;
