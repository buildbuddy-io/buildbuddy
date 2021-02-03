import React from "react";

export type TextInputProps = Omit<JSX.IntrinsicElements["input"], "type"> & {
  type?: "text" | "password";
};

export const TextInput = React.forwardRef((props: TextInputProps, ref: React.Ref<HTMLInputElement>) => {
  const { type, className, ...rest } = props;
  return <input ref={ref} type={type || "text"} className={`text-input ${className}`} {...rest} />;
});

export default TextInput;
