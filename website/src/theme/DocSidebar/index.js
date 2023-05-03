/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import { NavbarSecondaryMenuFiller, ThemeClassNames, useThemeConfig, useWindowSize } from "@docusaurus/theme-common";
import { useAnnouncementBar, useScrollPosition } from "@docusaurus/theme-common/internal";
import { translate } from "@docusaurus/Translate";
import useIsBrowser from "@docusaurus/useIsBrowser";
import DocSidebarItems from "@theme/DocSidebarItems";
import IconArrow from "@theme/Icon/Arrow";
import Logo from "@theme/Logo";
import clsx from "clsx";
import React, { useState } from "react";

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

function HideableSidebarButton({ onClick }) {
  return (
    <button
      type="button"
      title={translate({
        id: "theme.docs.sidebar.collapseButtonTitle",
        message: "Collapse sidebar",
        description: "The title attribute for collapse button of doc sidebar",
      })}
      aria-label={translate({
        id: "theme.docs.sidebar.collapseButtonAriaLabel",
        message: "Collapse sidebar",
        description: "The title attribute for collapse button of doc sidebar",
      })}
      className={clsx("button button--secondary button--outline", styles.collapseSidebarButton)}
      onClick={onClick}>
      <IconArrow className={styles.collapseSidebarButtonIcon} />
    </button>
  );
}

function DocSidebarDesktop({ path, sidebar, onCollapse, isHidden }) {
  const shouldShowScrollbar = useIsBrowser() ? !/(Mac|iPhone|iPod|iPad)/i.test(navigator.platform) : false;
  const showAnnouncementBar = useShowAnnouncementBar();
  const {
    navbar: { hideOnScroll },
    hideableSidebar,
  } = useThemeConfig();
  return (
    <div
      className={clsx(styles.sidebar, {
        [styles.sidebarWithHideableNavbar]: hideOnScroll,
        [styles.sidebarHidden]: isHidden,
      })}>
      {" "}
      {hideOnScroll && <Logo tabIndex={-1} className={styles.sidebarLogo} />}
      <nav
        className={clsx("menu", styles.menu, {
          [styles.menuWithAnnouncementBar]: showAnnouncementBar,
          "thin-scrollbar": shouldShowScrollbar,
        })}>
        <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, "menu__list")}>
          <DocSidebarItems items={sidebar} activePath={path} level={1} />
        </ul>
      </nav>
      {hideableSidebar && <HideableSidebarButton onClick={onCollapse} />}
    </div>
  );
} // eslint-disable-next-line react/function-component-definition

const DocSidebarMobileSecondaryMenu = ({ toggleSidebar, sidebar, path }) => (
  <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, "menu__list")}>
    <DocSidebarItems
      items={sidebar}
      activePath={path}
      onItemClick={(item) => {
        // Mobile sidebar should only be closed if the category has a link
        if (item.type === "category" && item.href) {
          toggleSidebar();
        }

        if (item.type === "link") {
          toggleSidebar();
        }
      }}
      level={1}
    />
  </ul>
);

function DocSidebarMobile(props) {
  return <NavbarSecondaryMenuFiller component={DocSidebarMobileSecondaryMenu} props={props} />;
}

const DocSidebarDesktopMemo = React.memo(DocSidebarDesktop);
const DocSidebarMobileMemo = React.memo(DocSidebarMobile);
export default function DocSidebar(props) {
  const windowSize = useWindowSize(); // Desktop sidebar visible on hydration: need SSR rendering

  const shouldRenderSidebarDesktop = windowSize === "desktop" || windowSize === "ssr"; // Mobile sidebar not visible on hydration: can avoid SSR rendering

  const shouldRenderSidebarMobile = windowSize === "mobile";
  return (
    <>
      {shouldRenderSidebarDesktop && <DocSidebarDesktopMemo {...props} />}
      {shouldRenderSidebarMobile && <DocSidebarMobileMemo {...props} />}
    </>
  );
}
