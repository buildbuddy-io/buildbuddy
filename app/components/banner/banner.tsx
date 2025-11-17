import { AlertCircle, CheckCircle, Info, XCircle } from "lucide-react";
import React from "react";

type BannerType = "info" | "success" | "warning" | "error";

const ICONS: Record<BannerType, JSX.Element> = {
  info: <Info className="icon blue" />,
  success: <CheckCircle className="icon green" />,
  warning: <AlertCircle className="icon orange" />,
  error: <XCircle className="icon red" />,
};

export type BannerProps = JSX.IntrinsicElements["div"] & {
  /** The banner type. */
  type: BannerType;
};

/**
 * A banner shows a message that draws the attention of the user, using a
 * colorful icon and background.
 */
export const Banner: React.ForwardRefExoticComponent<BannerProps & React.RefAttributes<HTMLDivElement>> =
  React.forwardRef<HTMLDivElement, BannerProps>((props: BannerProps, ref: React.Ref<HTMLDivElement>) => {
    const { type = "info", className, children, ...rest } = props;
    return (
      <div className={`banner banner-${type} ${className || ""}`} {...rest} ref={ref}>
        {ICONS[type]}
        <div className="banner-content">{children}</div>
      </div>
    );
  });

export default Banner;
