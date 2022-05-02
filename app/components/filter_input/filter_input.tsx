import { Filter } from "lucide-react";
import React from "react";

export type FilterInputProps = React.InputHTMLAttributes<HTMLInputElement>;

/**
 * `FilterInput` renders an input intended to be placed at the top of a list of
 * results, filtering the results list as text is entered.
 */
export const FilterInput = React.forwardRef((props: FilterInputProps, ref: React.Ref<HTMLInputElement>) => {
  const { className, placeholder, ...rest } = props;
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
      <Filter className="icon" />
    </div>
  );
});
