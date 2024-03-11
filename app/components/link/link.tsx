import React from "react";
import router from "../../router/router";

export type LinkProps = {
  /** Prevents existing filter state from being preserved when navigating. */
  resetFilters?: boolean;
} & React.AnchorHTMLAttributes<HTMLAnchorElement>;

/**
 * `Link` renders an unstyled, router-aware `<a>` element.
 *
 * It handles app-internal navigation via the router, to avoid full page
 * reloads.
 *
 * External links are opened in a new tab, but this behavior can be overridden
 * by explicitly setting a `target`.
 *
 * It respects any `onClick` handlers registered, and invokes them before
 * navigation. If `e.preventDefault()` is called from the registered `onClick`
 * handler, navigation is canceled, as is the case for normal `<a>` elements.
 */
export const Link = React.forwardRef((props: LinkProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, href, target, onClick, resetFilters, ...rest } = props;
  const isExternal = Boolean(target) || Boolean(href && (href.startsWith("http://") || href.startsWith("https://")));
  const onClickWrapped = isExternal
    ? onClick
    : (e: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => {
        if (onClick) {
          onClick(e);
          if (e.defaultPrevented) return;
        }
        if (e.metaKey || e.ctrlKey) {
          return;
        }
        e.preventDefault();
        if (href) router.navigateTo(href, resetFilters);
      };
  return (
    <a
      ref={ref}
      className={`link-wrapper ${className || ""}`}
      onClick={onClickWrapped}
      href={href}
      target={target ?? (isExternal ? "_blank" : undefined)}
      {...rest}
    />
  );
});

export type TextLinkProps = LinkProps;

/**
 * TextLink renders an inline text `<Link>` with underline styling.
 */
export const TextLink = React.forwardRef((props: TextLinkProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, ...rest } = props;
  return <Link ref={ref} className={`text-link ${className}`} {...rest} />;
});

export default Link;
