import React from "react";
import router from "../../router/router";

const CONTROL_CHARACTER_PATTERN = /[\u0000-\u001F\u007F-\u009F]/;
const SCHEME_PATTERN = /^[A-Za-z][A-Za-z0-9+.-]*:/;
const SAFE_LINK_HREF_PROTOCOLS = new Set(["http:", "https:", "blob:"]);

export type LinkProps = {
  /** Link destination. Raw strings are sanitized before rendering. */
  href?: string;
  /** Prevents existing filter state from being preserved when navigating. */
  resetFilters?: boolean;
} & Omit<React.AnchorHTMLAttributes<HTMLAnchorElement>, "href">;

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
  const { className, href: unvalidatedHref, target, onClick, resetFilters, ...rest } = props;

  const href = sanitizeLinkHref(unvalidatedHref);
  const shouldHandleWithRouter = !!href && !target && !SCHEME_PATTERN.test(href) && !href.startsWith("//");
  const onClickWrapped = shouldHandleWithRouter
    ? (e: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => {
        if (onClick) {
          onClick(e);
          if (e.defaultPrevented) return;
        }
        if (e.metaKey || e.ctrlKey) {
          return;
        }
        e.preventDefault();
        if (href) router.navigateTo(href, resetFilters);
      }
    : onClick;
  return (
    <a
      ref={ref}
      className={`link-wrapper ${className || ""}`}
      onClick={onClickWrapped}
      href={href}
      target={target ?? (href?.startsWith("http://") || href?.startsWith("https://") ? "_blank" : undefined)}
      {...rest}
    />
  );
});

export type TextLinkProps = LinkProps & {
  /**
   * Don't apply color styling to the link except when hovering.
   *
   * This style is used for links rendered in lists of items, where the blue
   * link color can be overwhelming.
   */
  plain?: boolean;
};

/**
 * TextLink renders an inline text `<Link>` with underline styling.
 */
export const TextLink = React.forwardRef((props: TextLinkProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, plain, ...rest } = props;
  return <Link ref={ref} className={`text-link ${plain ? "plain" : ""} ${className}`} {...rest} />;
});

export default Link;

/**
 * sanitizeLinkHref returns a safe href for app-rendered links. It rejects
 * disallowed URL schemes such as "javascript:" and also rejects potentially
 * unsafe control characters.
 */
export function sanitizeLinkHref(href: string | null | undefined): string | undefined {
  const cleaned = (href ?? "").trim() || undefined;
  if (!cleaned || cleaned.startsWith("//")) return undefined;
  // Reject potentially unsafe control characters.
  if (CONTROL_CHARACTER_PATTERN.test(cleaned)) return undefined;
  if (!SCHEME_PATTERN.test(cleaned)) return cleaned;

  try {
    const parsed = new URL(cleaned);
    if (!SAFE_LINK_HREF_PROTOCOLS.has(parsed.protocol)) return undefined;
    return parsed.href;
  } catch (e) {
    return undefined;
  }
}
