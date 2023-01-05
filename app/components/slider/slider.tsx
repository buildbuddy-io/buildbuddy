import React from "react";
import ReactSlider, { ReactSliderProps } from "react-slider";

export type SliderProps = ReactSliderProps;

export function Slider({ className, ...rest }: SliderProps) {
  return (
    <ReactSlider
      className={`horizontal-slider ${className || ""}`}
      thumbClassName="slider-thumb"
      trackClassName="slider-track"
      {...rest}
    />
  );
}

export default Slider;
