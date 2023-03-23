import React from "react";
import { XCircle, CheckCircle, AlertCircle, Info } from "lucide-react";

const ICONS = {
  info: <Info className="icon blue" />,
  success: <CheckCircle className="icon green" />,
  warning: <AlertCircle className="icon orange" />,
  error: <XCircle className="icon red" />,
} as const;

type BannerType = keyof typeof ICONS;

export type BannerProps = JSX.IntrinsicElements["div"] & {
  /** The banner type. */
  type: BannerType;
};

/**
 * A banner shows a message that draws the attention of the user, using a
 * colorful icon and background.
 */
export const Banner = React.forwardRef((props: BannerProps, ref: React.Ref<HTMLDivElement>) => {
  const { type = "info", className, children, ...rest } = props;
  return (
    <div className={`banner banner-${type} ${className || ""}`} {...rest} ref={ref}>
      {ICONS[type]}
      <div className="banner-content">{children}</div>
    </div>
  );
});

export default Banner;
