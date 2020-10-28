import React from "react";

export const Menu = React.forwardRef(
  ({ className, ...props }: JSX.IntrinsicElements["ul"], ref: React.Ref<HTMLUListElement>) => (
    <ul ref={ref} role="menu" className={`menu-list ${className || ""}`} {...props} />
  )
);

export type MenuItemProps = JSX.IntrinsicElements["li"] & {
  disabled?: boolean;
};

export const MenuItem = React.forwardRef(
  ({ className, disabled, onClick, ...props }: MenuItemProps, ref: React.Ref<HTMLLIElement>) => (
    <li
      ref={ref}
      role="menuitem"
      className={`menu-list-item ${disabled ? "disabled" : ""} ${className || ""}`}
      onClick={disabled ? undefined : onClick}
      {...props}
    />
  )
);

export default Menu;
