/** Draws a filled circle. */
export function fillCircle(
  ctx: CanvasRenderingContext2D,
  xCenter: number,
  yCenter: number,
  radius: number,
  fillColor: string = "black"
): void {
  ctx.beginPath();
  ctx.arc(xCenter, yCenter, radius, 0, 2 * Math.PI);
  ctx.fillStyle = fillColor;
  ctx.fill();
}

/** Text box properties. */
export type TextBoxProps = {
  font?: string;
  textColor?: string;
  boxColor?: string;
  boxRadius?: number;
  boxPadding?: number;
  /**
   * x position anchoring. If 'right', the right edge of the text is anchored to
   * the x position rather than the left edge.
   */
  xAnchor?: "left" | "right";
};

/**
 * Draws a filled text box. The `(x, y)` coordinates specify the bottom-left
 * coordinates of the text, as they are passed directly to `canvas.fillText`.
 * The box is drawn around the text based on the specified padding. Text
 * wrapping is not supported.
 */
export function fillTextBox(
  ctx: CanvasRenderingContext2D,
  text: string,
  x: number,
  y: number,
  { textColor = "black", boxColor = "white", boxRadius = 0, boxPadding = 0, xAnchor = "left" }: TextBoxProps = {}
) {
  const textMetrics = ctx.measureText(text);
  const radiusPadding = boxRadius / 2;
  const boxWidth = textMetrics.width + boxPadding * 2 + radiusPadding * 2;
  const xOffset = xAnchor === "right" ? -textMetrics.width : 0;
  fillRoundedRect(
    ctx,
    x - boxPadding - radiusPadding + xOffset,
    y - textMetrics.actualBoundingBoxAscent - boxPadding,
    boxWidth,
    textMetrics.actualBoundingBoxAscent + 1 + boxPadding * 2,
    boxRadius,
    boxColor
  );
  ctx.fillStyle = textColor;
  ctx.fillText(text, x + xOffset, y);
}

/**
 * Draws text centered within the given rectangular region using the context's
 * current font style and fill color. Centering can be vertical, horizontal, or
 * both.
 */
export function fillCenteredText(
  ctx: CanvasRenderingContext2D,
  text: string,
  left: number,
  top: number,
  width: number,
  height: number,
  {
    vertical = false,
    horizontal = false,
    maxWidth = undefined,
  }: {
    vertical?: boolean;
    horizontal?: boolean;
    maxWidth?: number;
  } = {}
) {
  let x = left;
  let y = top + height;
  const textMetrics = ctx.measureText(text);
  if (horizontal) {
    x += width / 2;
    x -= textMetrics.width / 2;
  }
  if (vertical) {
    y -= height / 2;
    y += textMetrics.actualBoundingBoxAscent / 2;
  }
  ctx.fillText(text, x, y, maxWidth);
}

/**
 * Draws a filled rounded rectangle.
 */
export function fillRoundedRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number,
  fillStyle: string = ""
) {
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + width, y, x + width, y + height, radius);
  ctx.arcTo(x + width, y + height, x, y + height, radius);
  ctx.arcTo(x, y + height, x, y, radius);
  ctx.arcTo(x, y, x + width, y, radius);
  ctx.closePath();
  if (fillStyle) ctx.fillStyle = fillStyle;
  ctx.fill();
}
