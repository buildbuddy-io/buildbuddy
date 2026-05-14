/**
 * How many units in the model X dimension there are per second. Model X units
 * are just microseconds, but the term "model x" coordinates is used to make
 * some of the transformation calculations more readable.
 */
export const MODEL_X_PER_SECOND = 1_000_000;

export const EVENTS_PANEL_HEIGHT = 540;
export const LINE_PLOTS_PANEL_HEIGHT = 400;

export const TIMESTAMP_HEADER_SIZE = 16;
export const TIMESTAMP_FONT_SIZE = "11px";

export const GRIDLINE_IDEAL_GAP = 80;

export const SECTION_LABEL_HEIGHT = 20;
export const SECTION_LABEL_FONT_SIZE = "13px";
export const SECTION_LABEL_PADDING_BOTTOM = 1;
export const SECTION_PADDING_BOTTOM = 1;

export const TRACK_HEIGHT = 16;
export const TRACK_VERTICAL_GAP = 1;

export const EVENT_HORIZONTAL_GAP = 0.5;
export const EVENT_LABEL_WIDTH_THRESHOLD = 20;
export const EVENT_LABEL_FONT_SIZE = "11px";

// The minimum pixel width an event should be rendered at. Events smaller than
// this will be clamped to this width, unless they are explicitly skipped.
//
// When jumping to a matched span via the search bar, if the span is smaller
// than this in pixel width, the viewer will zoom in to make it at least this wide.
export const MIN_RENDER_PIXEL_WIDTH = 1;

export const TIME_SERIES_HEIGHT = 24;
export const TIME_SERIES_POINT_RADIUS = 2;

export const BOTTOM_CONTROLS_HEIGHT = 48;

export const SCROLLBAR_SIZE = 13;

export const DARK_LINE_PLOT_FILL_OPACITY = 0.35;
export const DARK_LINE_PLOT_STROKE_OPACITY = 0.6;
