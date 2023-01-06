import React from "react";
import ReactSlider, { ReactSliderProps } from "react-slider";

export function Slider({ className, ...rest }: ReactSliderProps<ReadonlyArray<number>>) {
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
