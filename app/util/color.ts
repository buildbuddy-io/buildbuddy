/**
 * A subset of material colors arranged so that adjacent colors in the list are
 * visually distinct.
 */
const MATERIAL_CHART_COLORS = [
  "#4CAF50", // green-500
  "#03A9F4", // blue-500
  "#FF9800", // orange-500
  "#9C27B0", // purple-500
  "#F44336", // red-500
  "#009688", // teal-500
  "#3F51B5", // indigo-500
];

/**
 * Returns a color suitable for use in charts. The input parameter is an index
 * representing the color's position relative to other chart elements, starting
 * from 0. Indexes that are one apart from each other will have visually
 * distinct colors.
 *
 * If the index exceeds the number of pre-configured material colors, the
 * returned color falls back to a less pretty set of colors but with uniform
 * perceived brightness.
 */
export function getChartColor(index: number) {
  if (index < 0 || index >= MATERIAL_CHART_COLORS.length) {
    return getUniformBrightnessColor(String(index));
  }
  return MATERIAL_CHART_COLORS[index];
}

// See https://perceived-brightness.vercel.app/
// These colors were generated using a perceived brightness of 156 +/- 3
// We want the perceived brightness to be constant so that text always
// displays consistently against flame chart colors.
const UNIFORM_BRIGHTNESS_COLORS = [
  "#17b51a",
  "#36a9c5",
  "#64ad02",
  "#669ce6",
  "#23b08f",
  "#06b610",
  "#20a9d3",
  "#d778ab",
  "#4bb109",
  "#24b08c",
  "#d38527",
  "#d678a4",
  "#2aa8d3",
  "#c17ae5",
  "#63ad05",
  "#699ce1",
  "#9f9c1b",
  "#38b071",
  "#d57c8c",
  "#3ab31d",
  "#ca8912",
  "#07aacf",
  "#e960e7",
  "#36a4ec",
  "#f96c23",
  "#ec7445",
  "#e07d1c",
  "#ed63c6",
  "#06aace",
  "#ac9802",
  "#28ae9c",
  "#32a7d2",
  "#f36c5c",
  "#27b51b",
  "#5dae25",
  "#79a710",
  "#c88a28",
  "#7f98d6",
  "#d96fd7",
  "#af80f6",
  "#32ad97",
  "#c37bdd",
  "#a7982c",
  "#1faf9e",
  "#b39429",
  "#d67c82",
  "#64ad0b",
  "#d67f68",
  "#2bae9a",
  "#ba7afb",
  "#fa6191",
  "#0db371",
  "#d677b6",
  "#de7e0f",
  "#13a5f5",
  "#05b633",
  "#c08d3f",
  "#80a61e",
  "#0ea6f3",
  "#d68236",
  "#25adac",
  "#0fa8db",
  "#17a4f7",
  "#12a5f4",
  "#56a3d0",
  "#37a4eb",
  "#539ef3",
  "#4ba5c5",
  "#42a0fa",
  "#f357e7",
  "#00b09a",
  "#48a6cb",
  "#fa6a04",
  "#1ab531",
  "#5ead1d",
  "#61ad13",
  "#e07966",
  "#34a6dc",
  "#7c93fd",
  "#56af27",
  "#e87739",
  "#679ed9",
  "#f652e8",
  "#ed7514",
  "#7fa624",
  "#609aff",
  "#f56d44",
  "#39b320",
  "#c38d2c",
  "#13b61c",
  "#e2785e",
  "#e963d8",
  "#1badac",
  "#1da8dd",
  "#6caa36",
  "#43b201",
  "#eb7706",
  "#9f9b30",
  "#05abc9",
  "#e862d9",
  "#f061c7",
  "#f253f4",
  "#f96098",
  "#ac85ea",
  "#00adb6",
  "#d57aa5",
  "#65ac2a",
  "#37b156",
  "#f76a5f",
  "#8a93dc",
  "#fd6739",
  "#dd7d4f",
  "#08b368",
  "#c38c41",
  "#09b462",
  "#00a4fc",
  "#bd9012",
  "#85a429",
  "#6399fb",
  "#de71ba",
  "#04b637",
  "#15b453",
  "#e97274",
  "#1ab454",
  "#6a9ed2",
  "#df7978",
  "#21a3fb",
  "#1cacb8",
];

/**
 * Returns a random color for the given ID.
 *
 * Calls for the same ID will return the same color.
 *
 * All colors returned have the same approximate perceived brightness
 * to avoid issues with color contrast.
 */
export function getUniformBrightnessColor(id: string) {
  return UNIFORM_BRIGHTNESS_COLORS[Math.abs(hash(id) % UNIFORM_BRIGHTNESS_COLORS.length)];
}

function hash(value: string) {
  let hash = 0;
  for (let i = 0; i < value?.length; i++) {
    hash = ((hash << 5) - hash + value.charCodeAt(i)) | 0;
  }
  return hash;
}
