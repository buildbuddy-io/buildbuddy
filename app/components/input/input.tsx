import React from "react";

export type TextInputProps = Omit<JSX.IntrinsicElements["input"], "type">;

export const TextInput = React.forwardRef((props: TextInputProps, ref: React.Ref<HTMLInputElement>) => {
  const { className, ...rest } = props;
  return <input ref={ref} type="text" className={`text-input ${className}`} {...rest} />;
});

export default TextInput;
