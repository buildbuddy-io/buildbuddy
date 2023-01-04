import React from "react";
import ReactSlider from "react-slider";

export type SliderProps = Omit<JSX.IntrinsicElements["input"], "type">;

export function Slider({ className, ...rest }: SliderProps) {
  return (
    <ReactSlider className="horizontal-slider" thumbClassName="slider-thumb" trackClassName="slider-track" {...rest} />
  );
}

export default Slider;
