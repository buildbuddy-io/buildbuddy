import { ChevronDown } from "lucide-react";
import React from "react";

export type SelectProps = JSX.IntrinsicElements["select"];

export const Select = React.forwardRef((props: SelectProps, ref: React.Ref<HTMLSelectElement>) => {
  const { className, ...rest } = props;
  return (
    <div className="select-wrapper">
      <select ref={ref} className={`select ${className || ""}`} {...rest} />
      <div className="dropdown-icon-wrapper">
        <ChevronDown className="icon" />
      </div>
    </div>
  );
});

export function Option(props: JSX.IntrinsicElements["option"]) {
  const { className, ...rest } = props;
  return <option className={`select-option ${className || ""}`} {...rest} />;
}

export default Select;
