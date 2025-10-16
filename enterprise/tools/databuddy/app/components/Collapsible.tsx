import React from "react";

type CollapsibleContextType = {
  get expanded(): boolean;
  setExpanded: (visible: boolean) => void;
  /** Toggle expanded and return the new value. */
  toggleExpanded: () => boolean;
};

const CollapsibleContext = React.createContext<CollapsibleContextType | undefined>(undefined);

export type CollapsibleProps = React.PropsWithChildren<{
  defaultExpanded?: boolean;
}>;

/**
 * Collapsible content component.
 *
 * This component is not styled and is only responsible for managing the
 * expanded/collapsed className on the content container that it renders.
 */
export default function Collapsible({ defaultExpanded, children }: CollapsibleProps) {
  const [expanded, setExpanded] = React.useState<boolean>(defaultExpanded ?? true);
  const toggleExpanded = () => {
    setExpanded(!expanded);
    return !expanded;
  };
  return (
    <CollapsibleContext.Provider value={{ expanded, setExpanded, toggleExpanded }}>
      {children}
    </CollapsibleContext.Provider>
  );
}

/**
 * Collapsible context consumer.
 * Can be used to render expand/collapse controls.
 */
Collapsible.Consumer = CollapsibleContext.Consumer;

export type CollapsibleContentProps = JSX.IntrinsicElements["div"];

/**
 * Collapsible content that should be rendered as a child of `<Collapsible>`.
 */
Collapsible.Content = function CollapsibleContent({ className, children, ...rest }: CollapsibleContentProps) {
  const context = React.useContext(CollapsibleContext);
  if (!context) {
    console.error(
      "Collapsible.Content must be rendered as a child of a <Collapsible>. Collapse functionality will not work."
    );
  }
  return (
    <div
      className={`collapsible-content ${(context?.expanded ?? true) ? "expanded" : "collapsed"} ${className ?? ""}`}
      {...rest}>
      {children}
    </div>
  );
};
