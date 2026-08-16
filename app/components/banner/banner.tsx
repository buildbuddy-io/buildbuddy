import { AlertCircle, CheckCircle, Info, X, XCircle } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../button/button";

const ICONS = {
  info: <Info className="blue" />,
  success: <CheckCircle className="green" />,
  warning: <AlertCircle className="orange" />,
  error: <XCircle className="red" />,
} as const;

type BannerType = keyof typeof ICONS;

export type BannerProps = JSX.IntrinsicElements["div"] & {
  /** The banner type. */
  type: BannerType;
  /** Called when the dismiss button is clicked. If null/undefined, no dismiss button is shown. */
  onDismiss?: (() => void) | null;
};

/**
 * A banner shows a message that draws the attention of the user, using a
 * colorful icon and background.
 */
export const Banner = React.forwardRef((props: BannerProps, ref: React.Ref<HTMLDivElement>) => {
  const { type = "info", className, children, onDismiss, ...rest } = props;
  return (
    <div className={`banner banner-${type} ${className || ""}`} {...rest} ref={ref}>
      {ICONS[type]}
      <div className="banner-content">{children}</div>
      {onDismiss && (
        <OutlinedButton className="icon-button banner-dismiss-button" onClick={onDismiss} title="Dismiss" type="button">
          <X />
        </OutlinedButton>
      )}
    </div>
  );
});

export default Banner;
