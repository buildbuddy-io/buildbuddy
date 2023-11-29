import { fillCenteredText, fillCircle, fillTextBox } from "../util/canvas";
import { ClientXY, domRectContains } from "../util/dom";
import { truncateDecimals } from "../util/math";
import * as constants from "./constants";
import { TraceEvent } from "./trace_events";
import { LinePlotModel, PanelModel, SectionModel, TrackModel } from "./trace_viewer_model";

/**
 * Draws the data from a `PanelModel` to a canvas.
 */
export default class Panel {
  /** The element containing the canvas. */
  readonly container: HTMLElement;

  private ctx: CanvasRenderingContext2D;
  private dpr = window.devicePixelRatio;

  /** Canvas x units per model x unit. */
  canvasXPerModelX: number = 1;

  scrollY = 0;
  scrollX = 0;

  mouse: ClientXY = { clientX: 0, clientY: 0 };
  /** Whether to draw a gridline for the mouse timestamp. */
  showMouseXGridline = false;

  /**
   * Canvas width and height numbers that account for device pixel ratio (don't
   * use canvas.width / canvas.height directly!)
   */
  canvasWidth = 0;
  canvasHeight = 0;

  constructor(readonly model: PanelModel, readonly canvas: HTMLCanvasElement, private fontFamily: string) {
    this.ctx = canvas.getContext("2d")!;
    this.container = canvas.parentElement!;
  }

  private isSectionVisible(section: SectionModel) {
    return !(
      constants.TIMESTAMP_HEADER_SIZE + section.y > this.scrollY + this.model.height ||
      constants.TIMESTAMP_HEADER_SIZE + section.y + section.height < this.scrollY
    );
  }

  private isSectionFullyVisible(section: SectionModel) {
    // TODO: incorporate timestamp header size into section.y instead of having to account for it here
    return (
      section.y + constants.TIMESTAMP_HEADER_SIZE >= this.scrollY + constants.TIMESTAMP_HEADER_SIZE &&
      section.y + constants.TIMESTAMP_HEADER_SIZE + section.height <=
        this.scrollY + this.model.height - constants.SCROLLBAR_SIZE
    );
  }

  containsClientXY(c: ClientXY) {
    return domRectContains(this.canvas.getBoundingClientRect(), c.clientX, c.clientY);
  }

  /**
   * Resizes the canvas to its parent's client width and height, and also
   * updates the scaling to match the current device pixel ratio (note: the
   * pixel ratio can change when zooming in and out).
   */
  resize() {
    this.dpr = window.devicePixelRatio;
    this.canvasWidth = this.container.clientWidth;
    this.canvasHeight = this.container.clientHeight;
    this.canvas.width = this.canvasWidth * this.dpr;
    this.canvas.height = this.canvasHeight * this.dpr;
    this.canvas.style.width = `${this.canvasWidth}px`;
    this.canvas.style.height = `${this.canvasHeight}px`;
    this.ctx.scale(this.dpr, this.dpr);
  }

  draw() {
    const ctx = this.ctx;
    ctx.clearRect(0, 0, this.canvasWidth, this.canvasHeight);

    const ticks = this.computeTicks();
    this.drawGridlines(ticks);
    this.drawSections();
    this.drawTimeline(ticks);
    this.drawMouseSelection();
  }

  getMouseModelCoordinates() {
    const scrollLeft = this.mouse.clientX - this.canvas.getBoundingClientRect().left + this.scrollX;
    const x = scrollLeft / this.canvasXPerModelX;
    const y =
      this.mouse.clientY - this.canvas.getBoundingClientRect().top + this.scrollY - constants.TIMESTAMP_HEADER_SIZE;

    return { x, y };
  }

  isHovering() {
    return this.canvas === document.elementFromPoint(this.mouse.clientX, this.mouse.clientY);
  }

  getHoveredSection(): SectionModel | null {
    if (!this.isHovering()) return null;

    const modelMouse = this.getMouseModelCoordinates();
    for (const section of this.model.sections) {
      if (modelMouse.y >= section.y && modelMouse.y <= section.y + section.height) {
        return section;
      }
      if (section.y > modelMouse.y) return null;
    }
    return null;
  }

  getHoveredTrack(): TrackModel | null {
    if (!this.isHovering()) return null;

    const section = this.getHoveredSection();
    if (!section?.tracks) return null;

    const modelMouse = this.getMouseModelCoordinates();
    let trackY = section.y + constants.SECTION_LABEL_HEIGHT + constants.SECTION_LABEL_PADDING_BOTTOM;
    const trackYIncrement = constants.TRACK_HEIGHT + constants.EVENT_HORIZONTAL_GAP;
    for (const track of section.tracks) {
      if (modelMouse.y >= trackY && modelMouse.y <= trackY + trackYIncrement) {
        if (modelMouse.y > trackY + constants.TRACK_HEIGHT) {
          return null; // We're in the horizontal gap between tracks.
        }
        return track;
      }
      if (trackY > modelMouse.y) return null;
      trackY += trackYIncrement;
    }
    return null;
  }

  getHoveredEvent(): TraceEvent | null {
    if (!this.isHovering()) return null;

    const track = this.getHoveredTrack();
    if (!track) return null;

    const modelMouse = this.getMouseModelCoordinates();

    const onePx = 1 / this.canvasXPerModelX;
    for (const event of track.events) {
      if (modelMouse.x >= event.ts && modelMouse.x <= event.ts + Math.max(event.dur, onePx)) {
        return event;
      }
      if (event.ts > modelMouse.x) return null;
    }
    return null;
  }

  /** Computes timing measures in model coordinates (microseconds). */
  private computeTicks(): Ticks {
    const canvasWidth = this.canvasWidth;
    const displayedDurationUsec = canvasWidth / this.canvasXPerModelX;
    const idealCount = canvasWidth / constants.GRIDLINE_IDEAL_GAP;

    const idealSize = displayedDurationUsec / idealCount;
    let duration = Math.pow(10, Math.ceil(Math.log10(idealSize)));
    // If the duration is very large in terms of canvas pixels, subdivide the
    // gridlines by 5.
    if (duration * this.canvasXPerModelX > 250) {
      duration /= 5;
    }

    const idealStart = this.scrollX / this.canvasXPerModelX;
    const start = idealStart - (idealStart % duration);
    const count = Math.ceil(this.canvasWidth / this.canvasXPerModelX / duration);

    return { start, size: duration, count };
  }

  private drawGridlines(ticks: Ticks, height = this.canvasHeight) {
    const ctx = this.ctx;
    ctx.lineWidth = 1;
    ctx.beginPath();
    let tick = ticks.start;
    for (let i = 0; i < ticks.count; i++, tick += ticks.size) {
      if (tick === 0) continue;
      const x = Math.floor(this.canvasXPerModelX * tick) - this.scrollX;
      ctx.moveTo(x + 0.5, 0);
      ctx.lineTo(x + 0.5, height);
    }
    ctx.strokeStyle = constants.GRIDLINE_COLOR;
    ctx.stroke();
  }

  private drawMouseSelection() {
    if (!this.showMouseXGridline) return;
    const x = Math.floor(this.mouse.clientX - this.canvas.getBoundingClientRect().left);
    const ctx = this.ctx;
    ctx.beginPath();
    ctx.moveTo(x - 0.5, 0);
    ctx.lineTo(x - 0.5, this.canvasHeight);
    ctx.lineWidth = 1;
    ctx.setLineDash([2, 4]);
    ctx.strokeStyle = `${constants.MOUSE_GRIDLINE_COLOR}`;
    ctx.stroke();
    ctx.setLineDash([]);

    // TODO: show labels to left when near the right edge.
    const mouseModelX = (this.scrollX + x) / this.canvasXPerModelX;
    const text = (mouseModelX / constants.MODEL_X_PER_SECOND).toFixed(3) + "s";

    let textBoxX = x + 8;
    let xAnchor: "left" | "right" = "left";
    const isNearRightEdge = this.canvas.clientWidth - x < 80;
    if (isNearRightEdge) {
      textBoxX = x - 8;
      xAnchor = "right";
    }

    fillTextBox(ctx, text, textBoxX, constants.TIMESTAMP_HEADER_SIZE - 4, {
      textColor: constants.TIMESTAMP_FONT_COLOR,
      font: `${constants.TIMESTAMP_FONT_SIZE} ${this.fontFamily}`,
      boxColor: "#E0E0E0",
      boxRadius: 8,
      boxPadding: 2,
      xAnchor,
    });

    // For each plot, draw value labels at the mouse timestamp.
    for (const section of this.model.sections) {
      if (!section.linePlot) break; // assume no line plots in this panel for now.
      if (!this.isSectionFullyVisible(section)) continue;

      // If between two values, interpolate to get the approximate value.
      const mouseModelY = interpolate(mouseModelX, section.linePlot.xs, section.linePlot.ys) || 0;
      const sectionClientTop = constants.TIMESTAMP_HEADER_SIZE + section.y - this.scrollY;
      const plotClientBottom =
        sectionClientTop +
        constants.SECTION_LABEL_HEIGHT +
        constants.SECTION_LABEL_PADDING_BOTTOM +
        constants.TIME_SERIES_HEIGHT;
      const pointClientY = plotClientBottom - (mouseModelY / section.linePlot.yMax) * constants.TIME_SERIES_HEIGHT;
      fillCircle(ctx, x, pointClientY, 2, section.linePlot.darkColor);
      fillTextBox(ctx, String(truncateDecimals(mouseModelY, 2)), textBoxX, pointClientY - 6, {
        boxPadding: 1,
        boxRadius: 8,
        xAnchor,
      });
    }
  }

  private drawTimeline(ticks: Ticks) {
    const ctx = this.ctx;
    ctx.fillStyle = constants.TIMESTAMP_HEADER_COLOR;
    ctx.fillRect(0, 0, this.canvasWidth, constants.TIMESTAMP_HEADER_SIZE);
    ctx.fillStyle = constants.SECTION_LABEL_BORDER_COLOR;
    ctx.fillRect(0, constants.TIMESTAMP_HEADER_SIZE, this.canvasWidth, 1);
    this.drawGridlines(ticks, constants.TIMESTAMP_HEADER_SIZE);
    let tick = ticks.start;
    ctx.fillStyle = constants.TIMESTAMP_FONT_COLOR;
    ctx.font = `${constants.TIMESTAMP_FONT_SIZE} ${this.fontFamily}`;
    for (let i = 0; i < ticks.count; i++, tick += ticks.size) {
      const x = Math.floor(this.canvasXPerModelX * tick) - this.scrollX;
      ctx.fillText(formatMicroseconds(tick), x + 2, constants.TIMESTAMP_HEADER_SIZE - 4);
    }
  }

  private drawSections() {
    const ctx = this.ctx;
    const xMin = this.scrollX / this.canvasXPerModelX;
    const xMax = (this.scrollX + this.canvasWidth) / this.canvasXPerModelX;
    let i = 0;
    for (; i < this.model.sections.length; i++) {
      if (this.isSectionVisible(this.model.sections[i])) break;
    }
    for (; i < this.model.sections.length; i++) {
      if (!this.isSectionVisible(this.model.sections[i])) break;
      const section = this.model.sections[i];
      const y = constants.TIMESTAMP_HEADER_SIZE + section.y - this.scrollY;
      // Section header BG
      ctx.fillStyle = constants.SECTION_LABEL_BG_COLOR;
      ctx.fillRect(0, y, this.canvasWidth, constants.SECTION_LABEL_HEIGHT);
      ctx.fillStyle = constants.SECTION_LABEL_BORDER_COLOR;
      // Section header top border
      ctx.fillRect(0, y, this.canvasWidth, 1);
      // Section header text (always pinned to the left)
      ctx.font = `600 ${constants.SECTION_LABEL_FONT_SIZE} ${this.fontFamily}`;
      ctx.fillStyle = constants.SECTION_LABEL_FONT_COLOR;
      fillCenteredText(ctx, section.name, 8, y, 0, constants.SECTION_LABEL_HEIGHT, { vertical: true });

      // Draw tracks
      let trackIndex = 0;
      for (const track of section.tracks ?? []) {
        const trackY =
          y +
          constants.SECTION_LABEL_HEIGHT +
          constants.SECTION_LABEL_PADDING_BOTTOM +
          trackIndex * (constants.TRACK_HEIGHT + constants.TRACK_VERTICAL_GAP);
        // TODO: skip drawing track if not visible *within* the current section.
        // This may be needed if we have to render very tall sections.
        this.drawTrack(track, trackY, xMin, xMax);
        trackIndex++;
      }

      // Draw line plots
      if (section.linePlot) {
        const yTop = y + constants.SECTION_LABEL_HEIGHT + constants.SECTION_LABEL_PADDING_BOTTOM;
        this.drawLinePlot(section.linePlot, yTop, xMin, xMax);
      }
    }
  }

  private drawLinePlot(plot: LinePlotModel, yTop: number, xMin: number, xMax: number) {
    const ctx = this.ctx;
    const yBottom = yTop + constants.TIME_SERIES_HEIGHT;
    // TODO: reduce plot resolution based on zoom level
    // TODO: render the point *before* xMin and *after* xMax since we connect to those
    let i = 0;
    for (; i < plot.xs.length; i++) {
      if (plot.xs[i] >= xMin) break;
    }
    // Make sure we include the point just before xMin if it exists, since we'll
    // be drawing a line to it.
    i = Math.max(0, i - 1);
    const i0 = i;
    const canvasYPerModelY = constants.TIME_SERIES_HEIGHT / plot.yMax;
    // Draw the background (lighter color).
    let started = false;
    let done = false;
    for (let i = i0; i < plot.xs.length && !done; i++) {
      const x = plot.xs[i];
      if (x > xMax || i === plot.xs.length - 1) {
        // Include this point even though it's not in view, since we'll be
        // drawing a line to it.
        done = true;
      }
      const canvasX = x * this.canvasXPerModelX - this.scrollX;
      if (!started) {
        ctx.beginPath();
        // Start from the bottom left.
        ctx.moveTo(canvasX, yBottom);
        started = true;
      }
      let y = plot.ys[i];
      // TODO: support negative y values?
      if (y < 0) y = 0;
      const canvasY = yBottom - y * canvasYPerModelY;
      ctx.lineTo(canvasX, canvasY);
      if (done) {
        ctx.lineTo(canvasX, yTop + constants.TIME_SERIES_HEIGHT);
      }
    }
    ctx.closePath();
    ctx.fillStyle = plot.lightColor;
    ctx.fill();

    // Draw the outline (darker color).
    started = false;
    done = false;
    for (let i = i0; i < plot.xs.length && !done; i++) {
      const x = plot.xs[i];
      if (x > xMax || i === plot.xs.length - 1) {
        // Include this point even though it's not in view, since we'll be
        // drawing a line to it.
        done = true;
      }
      const canvasX = x * this.canvasXPerModelX - this.scrollX;
      if (!started) {
        ctx.beginPath();
        ctx.moveTo(canvasX, yBottom);
        started = true;
      }
      let y = plot.ys[i];
      if (y < 0) y = 0;
      const canvasY = yBottom - y * canvasYPerModelY;
      ctx.lineTo(canvasX, canvasY);
      if (done) {
        ctx.lineTo(canvasX, yTop + constants.TIME_SERIES_HEIGHT);
      }
    }
    ctx.closePath();
    ctx.lineWidth = 1;
    ctx.strokeStyle = plot.darkColor;
    ctx.stroke();
  }

  private drawTrack(track: TrackModel, y: number, xMin: number, xMax: number) {
    let i = 0;
    let lastEventRendered = false;
    for (; i < track.xs.length; i++) {
      let modelX = track.xs[i];
      if (modelX > xMax) break;

      let modelWidth = track.widths[i];
      if (modelX + modelWidth < xMin) continue;

      let color = track.colors[i];

      this.ctx.fillStyle = color;
      // TODO: only apply the horizontal gap if there's an event just after us.
      let width = modelWidth * this.canvasXPerModelX - constants.EVENT_HORIZONTAL_GAP;
      if (width <= 0) {
        // If the event is less than 1px side, and less than 1px away from the previous
        // event start that rendered, don't render it.
        if (lastEventRendered && Math.abs(modelX - track.xs[i - 1]) * this.canvasXPerModelX <= 1) {
          lastEventRendered = false;
          continue;
        }
        width = 1;
      }
      const x = modelX * this.canvasXPerModelX - this.scrollX;
      this.ctx.fillRect(x, y, width, constants.TRACK_HEIGHT);
      lastEventRendered = true;

      const visibleWidth = width + Math.min(0, x);
      if (visibleWidth > constants.EVENT_LABEL_WIDTH_THRESHOLD) {
        let name = track.events[i].name;
        this.ctx.font = `${constants.EVENT_LABEL_FONT_SIZE} ${this.fontFamily}`;
        this.ctx.fillStyle = constants.EVENT_LABEL_FONT_COLOR;
        this.ctx.save();
        this.ctx.beginPath();
        this.ctx.rect(x, y, width, constants.TRACK_HEIGHT);
        this.ctx.clip();
        this.ctx.fillText(
          name,
          // Pin label to left edge if out of view.
          Math.max(0, x) + 2,
          y + constants.TRACK_HEIGHT - 4
        );
        this.ctx.restore();
      }
    }
  }
}

/** Displayed timing measures, in model coordinates. */
type Ticks = {
  start: number;
  size: number;
  count: number;
};

function formatMicroseconds(microseconds: number) {
  return `${truncateDecimals(microseconds / 1e6, 6)}s`;
}

/**
 * Returns the value of y corresponding to the given x value by linearly
 * interpolating between the two nearest neighbors in the given dataset.
 *
 * The given x values are expected to be sorted in increasing order.
 *
 * Returns undefined if x is out of bounds.
 */
function interpolate(x: number, xs: number[], ys: number[]): number | undefined {
  if (!xs.length) return undefined;
  if (x < xs[0]) return undefined;

  for (let i = 0; i < xs.length - 1; i++) {
    const x1 = xs[i + 1];
    // Scan until we find the first x1 >= x.
    if (x1 < x) continue;

    const x0 = xs[i];
    const y0 = ys[i];

    const y1 = ys[i + 1];
    const t = (x - x0) / (x1 - x0);
    return y0 + t * (y1 - y0);
  }

  return undefined;
}
