export const SORT_MODE_PARAM = "sort";
export const SORT_DIRECTION_PARAM = "direction";
export const COLOR_MODE_PARAM = "color";

export type ColorMode = "status" | "timing";

export type SortMode = "target" | "count" | "pass" | "avgDuration" | "maxDuration" | "flake" | "cached";

export type SortDirection = "asc" | "desc";
