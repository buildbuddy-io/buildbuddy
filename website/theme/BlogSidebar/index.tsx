import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import type { Props } from "@theme/BlogSidebar";
import styles from "./styles.module.css";

export default function BlogSidebar({ sidebar }: Props): JSX.Element | null {
  if (sidebar.items.length === 0) {
    return null;
  }
  return (
    <div className="blog-sidebar">
      <div className={clsx(styles.sidebar, "thin-scrollbar")}>
        <h3 className={styles.sidebarItemTitle}>{sidebar.title}</h3>
        <ul className={styles.sidebarItemList}>
          {sidebar.items.map((item) => {
            return (
              <li key={item.permalink} className={styles.sidebarItem}>
                <Link
                  isNavLink
                  to={item.permalink}
                  className={styles.sidebarItemLink}
                  activeClassName={styles.sidebarItemLinkActive}>
                  {item.title}
                </Link>
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}
