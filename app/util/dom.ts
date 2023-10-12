/**
 * Common interface for DOM objects containing clientX and clientY coordinates,
 * such as `MouseEvent`.
 */
export interface ClientXY {
  clientX: number;
  clientY: number;
}

/**
 * Returns whether a `DOMRect` contains a given point.
 */
export function domRectContains(rect: DOMRect, x: number, y: number) {
  return x >= rect.left && x < rect.left + rect.width && y >= rect.top && y < rect.top + rect.height;
}

export function createSvgElement(name: string) {
  return document.createElementNS("http://www.w3.org/2000/svg", name);
}
