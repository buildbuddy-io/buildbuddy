import { magnitude } from "./math";

export function createSvgElement(name: string) {
  return document.createElementNS("http://www.w3.org/2000/svg", name);
}

export function getGridlineGap(startX: number, endX: number, viewportClientWidth: number) {
  const width = endX - startX;
  return magnitude(width) / 10;
}

export function drawGridlines(
  g: SVGGElement,
  startX: number,
  endX: number,
  viewportClientWidth: number
) {
  g.innerHTML = "";

  const gap = getGridlineGap(startX, endX, viewportClientWidth);

  for (let i = Math.floor(startX); i < endX + 1; i += gap) {
    const line = createSvgElement("line");
    line.setAttribute("x1", String(i));
    line.setAttribute("x2", String(i));
    line.setAttribute("y1", "0");
    line.setAttribute("y2", "1");
    line.setAttribute("vector-effect", "non-scaling-stroke");
    line.setAttribute("stroke", "#ccc");
    line.setAttribute("stroke-width", "1");
    line.setAttribute("shape-rendering", "crispEdges");
    g.append(line);
  }
}
