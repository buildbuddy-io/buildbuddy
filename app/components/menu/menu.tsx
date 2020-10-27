import React from "react";

export const Menu = React.forwardRef(
  ({ className, ...props }: JSX.IntrinsicElements["ul"], ref: React.Ref<HTMLUListElement>) => (
    <ul ref={ref} role="menu" className={`bb-menu ${className || ""}`} {...props} />
  )
);

export type MenuItemProps = JSX.IntrinsicElements["li"] & {
  disabled?: boolean;
};

export const MenuItem = React.forwardRef(
  ({ className, disabled, ...props }: MenuItemProps, ref: React.Ref<HTMLLIElement>) => (
    <li
      ref={ref}
      role="menuitem"
      className={`bb-menu-item ${disabled ? "disabled" : ""} ${className || ""}`}
      {...props}
    />
  )
);

export default Menu;
