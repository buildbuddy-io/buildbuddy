import React from "react";

export type PopupProps = JSX.IntrinsicElements["div"] & {
  isOpen: boolean;
  onRequestClose: () => void;
  anchor?: "left" | "right";
};

/**
 * A popup positioned relative to a parent element.
 *
 * The positioning is done with `position: absolute`, so in most cases the parent
 * should specify `position: relative`.
 *
 * NOTE: Currently this popup anchors its top right corner to its parent's
 * bottom right corner.
 */
export const Popup = React.forwardRef(
  ({ isOpen, onRequestClose, className, anchor = "right", ...props }: PopupProps, ref: React.Ref<HTMLDivElement>) => {
    return (
      <>
        {isOpen && (
          <>
            <div className="popup-shade" onClick={onRequestClose} />
            <div
              ref={ref}
              className={`popup anchor-${anchor} ${className || ""}`}
              onClick={(e) => e.preventDefault()}
              {...props}
            />
          </>
        )}
      </>
    );
  }
);

export type PopupContainerProps = JSX.IntrinsicElements["div"];

/**
 * PopupContainer wraps an element that triggers a popup, as well the popup
 * itself. It is required to correctly anchor the popup to the popup trigger.
 */
export const PopupContainer = React.forwardRef(
  ({ className, ...props }: PopupContainerProps, ref: React.Ref<HTMLDivElement>) => {
    return <div ref={ref} className={`popup-container ${className || ""}`} {...props} />;
  }
);

export default Popup;
