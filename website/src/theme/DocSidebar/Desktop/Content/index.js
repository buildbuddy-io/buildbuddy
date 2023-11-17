import React, { useState } from "react";
import clsx from "clsx";
import { ThemeClassNames } from "@docusaurus/theme-common";
import { useAnnouncementBar, useScrollPosition } from "@docusaurus/theme-common/internal";
import useIsBrowser from "@docusaurus/useIsBrowser";
import { translate } from "@docusaurus/Translate";
import DocSidebarItems from "@theme/DocSidebarItems";
import styles from "./styles.module.css";
function useShowAnnouncementBar() {
  const { isActive } = useAnnouncementBar();
  const [showAnnouncementBar, setShowAnnouncementBar] = useState(isActive);
  useScrollPosition(
    ({ scrollY }) => {
      if (isActive) {
        setShowAnnouncementBar(scrollY === 0);
      }
    },
    [isActive]
  );
  return isActive && showAnnouncementBar;
}
export default function DocSidebarDesktopContent({ path, sidebar, className }) {
  const showAnnouncementBar = useShowAnnouncementBar();
  const shouldShowScrollbar = useIsBrowser() ? !/(Mac|iPhone|iPod|iPad)/i.test(navigator.platform) : false;
  return (
    <nav
      aria-label={translate({
        id: "theme.docs.sidebar.navAriaLabel",
        message: "Docs sidebar",
        description: "The ARIA label for the sidebar navigation",
      })}
      className={clsx(
        "menu",
        styles.menu,
        showAnnouncementBar && styles.menuWithAnnouncementBar,
        className,
        shouldShowScrollbar && "thin-scrollbar"
      )}>
      <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, "menu__list")}>
        <DocSidebarItems items={sidebar} activePath={path} level={1} />
      </ul>
    </nav>
  );
}
