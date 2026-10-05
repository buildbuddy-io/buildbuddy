import React from "react";
import router from "../lib/router";

export type LinkProps = Omit<JSX.IntrinsicElements["a"], "href" | "ref"> & {
  /** An in-app path, relative to the base path. */
  to: string;
};

/** An in-app link: plain left clicks navigate without a page load. */
export const Link = React.forwardRef((props: LinkProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { to, onClick, ...rest } = props;
  return (
    <a
      ref={ref}
      href={to}
      onClick={(e) => {
        onClick?.(e);
        if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) {
          return;
        }
        e.preventDefault();
        router.navigateTo(to);
      }}
      {...rest}
    />
  );
});

export default Link;
