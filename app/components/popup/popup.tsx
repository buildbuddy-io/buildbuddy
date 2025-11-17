import React from "react";

export type PopupProps = JSX.IntrinsicElements["div"] & {
  isOpen: boolean;
  onRequestClose: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void;
  anchor?: "left" | "right" | "center" | "center-right";
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
export const Popup: React.ForwardRefExoticComponent<PopupProps & React.RefAttributes<HTMLDivElement>> =
  React.forwardRef<HTMLDivElement, PopupProps>(
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
export const PopupContainer: React.ForwardRefExoticComponent<
  PopupContainerProps & React.RefAttributes<HTMLDivElement>
> = React.forwardRef<HTMLDivElement, PopupContainerProps>(
  ({ className, ...props }: PopupContainerProps, ref: React.Ref<HTMLDivElement>) => {
    return <div ref={ref} className={`popup-container ${className || ""}`} {...props} />;
  }
);

export default Popup;
