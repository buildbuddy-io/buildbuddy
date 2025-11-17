import React from "react";

export type TextInputProps = React.InputHTMLAttributes<HTMLInputElement> & {
  type?: "text" | "password" | "number";
};

export const TextInput: React.ForwardRefExoticComponent<TextInputProps & React.RefAttributes<HTMLInputElement>> =
  React.forwardRef<HTMLInputElement, TextInputProps>((props: TextInputProps, ref: React.Ref<HTMLInputElement>) => {
    const { type, className, ...rest } = props;
    return <input ref={ref} type={type || "text"} className={`text-input ${className}`} {...rest} />;
  });

export default TextInput;
