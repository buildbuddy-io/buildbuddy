import { ChevronRight } from "lucide-react";
import React from "react";
import { directReactChildren, joinReactNodes } from "../../util/react";

/** Props for the Breadcrumbs component. */
export type BreadcrumbsProps = JSX.IntrinsicElements["div"];

/**
 * Renders breadcrumbs to indicate page hierarchy.
 *
 * Example:
 *
 * ```tsx
 * <Breadcrumbs className="example-breadcrumbs">
 *   <Link href="/">Root</Link>
 *   <>
 *     {isAtChildPage && <Link href="/child">Child</Link>}
 *     {isAtGrandchildPage && <Link href="/grandchild">Grandchild</Link>}
 *   </>
 * </Breadcrumbs>
 * ```
 *
 * Fragments are treated like direct children, but wrapper elements are not.
 * So, avoid wrapping children for styling purposes. Instead, style the
 * `<Breadcrumbs>` directly.
 *
 * ```tsx
 * <Breadcrumbs>
 *   <div className="wrapper"> // Don't do this!
 *     <Link href="/">Root</Link>
 *     {isAtChildPage && <Link href="/child">Child</Link>}
 *   </div>
 * </Breadcrumbs>
 * ```
 */
export function Breadcrumbs({ className, children, ...rest }: BreadcrumbsProps) {
  return (
    <div className={`breadcrumbs ${className ?? ""}`} {...rest}>
      {joinReactNodes(directReactChildren(children), <ChevronRight className="breadcrumbs-chevron" />)}
    </div>
  );
}

export default Breadcrumbs;
