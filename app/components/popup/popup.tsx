import React from "react";

export type PopupProps = JSX.IntrinsicElements["div"] & {
  isOpen: boolean;
  onRequestClose: () => void;
};

// TODO(bduffany): Currently this popup anchors its top right corner to its parent's
// bottom right corner. Eventually we'll need more flexible popup configurations.
export const Popup = React.forwardRef(
  ({ isOpen, onRequestClose, className, ...props }: PopupProps, ref: React.Ref<HTMLDivElement>) => {
    return (
      <>
        {isOpen && (
          <>
            <div className="bb-popup-shade" onClick={onRequestClose} />
            <div ref={ref} className={`bb-popup ${className || ""}`} onClick={(e) => e.preventDefault()} {...props} />
          </>
        )}
      </>
    );
  }
);

export default Popup;
