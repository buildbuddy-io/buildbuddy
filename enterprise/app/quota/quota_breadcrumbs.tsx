import React from "react";
import { TextLink } from "../../../app/components/link/link";

export interface BreadcrumbItem {
  label: string;
  href?: string;
}

export interface QuotaBreadcrumbsProps {
  items: BreadcrumbItem[];
}

export default function QuotaBreadcrumbs({ items }: QuotaBreadcrumbsProps) {
  return (
    <div className="quota-breadcrumbs">
      {items.map((item) => (
        <div className="quota-breadcrumb">
          {item.href && <TextLink href={item.href}>{item.label}</TextLink>}
          {!item.href && <b>{item.label}</b>}
        </div>
      ))}
    </div>
  );
}
