import { Search } from "lucide-react";
import React from "react";

export type FilterInputProps = React.InputHTMLAttributes<HTMLInputElement> & {
  // Optional element rendered on the right-hand side of the input (e.g. match counter).
  rightElement?: React.ReactNode;
};

/**
 * `FilterInput` renders an input intended to be placed at the top of a list of
 * results, filtering the results list as text is entered.
 */
export const FilterInput = React.forwardRef((props: FilterInputProps, ref: React.Ref<HTMLInputElement>) => {
  const { className, placeholder, rightElement, ...rest } = props;
  return (
    <div className={`filter-input-container ${props.value ? "is-filtering" : ""} ${className || ""}`}>
      <input
        ref={ref}
        className="filter-input"
        type="text"
        placeholder={placeholder || "Filter..."}
        {...{ spellcheck: "false" }}
        {...rest}
      />
      <Search className="icon" />
      {rightElement && <span className="right-element">{rightElement}</span>}
    </div>
  );
});
