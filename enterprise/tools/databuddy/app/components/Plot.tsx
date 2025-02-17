import React from "react";
import uPlot from "uplot";

export type PlotProps = {
  opts: uPlot.Options;
  data: uPlot.AlignedData;
} & JSX.IntrinsicElements["div"];

/**
 * uPlot component.
 *
 * See https://github.com/leeoniya/uPlot/tree/master/docs
 */
export default function Plot({ opts, data, ...rest }: PlotProps) {
  const containerRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    const root = containerRef.current;
    if (!root) return;

    const plot = new uPlot(opts, data, root);
    return () => {
      plot.destroy();
    };
  }, [opts, data]);

  return <div ref={containerRef} {...rest} />;
}
