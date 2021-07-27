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

export default Popup;
