// Router params. These are in their own file to avoid circular dependencies.

// Query params for the global filter.
// These should be preserved when navigating between pages in the app.
export const ROLE_PARAM_NAME = "role";
export const STATUS_PARAM_NAME = "status";
export const START_DATE_PARAM_NAME = "start";
export const END_DATE_PARAM_NAME = "end";
export const LAST_N_DAYS_PARAM_NAME = "days";

export const USER_PARAM_NAME = "user";
export const REPO_PARAM_NAME = "repo";
export const BRANCH_PARAM_NAME = "branch";
export const COMMIT_PARAM_NAME = "commit";
export const HOST_PARAM_NAME = "host";
export const COMMAND_PARAM_NAME = "command";
export const PATTERN_PARAM_NAME = "pattern";
export const TAG_PARAM_NAME = "tag";
export const MINIMUM_DURATION_PARAM_NAME = "min-dur";
export const MAXIMUM_DURATION_PARAM_NAME = "max-dur";

// Sort params for the global filter.
export const SORT_BY_PARAM_NAME = "sort-by";
export const SORT_ORDER_PARAM_NAME = "sort-order";
export const DEFAULT_SORT_BY_VALUE = "end-time";
export const DEFAULT_SORT_ORDER_VALUE = "desc";

export const GLOBAL_FILTER_PARAM_NAMES = [
  ROLE_PARAM_NAME,
  STATUS_PARAM_NAME,
  START_DATE_PARAM_NAME,
  END_DATE_PARAM_NAME,
  LAST_N_DAYS_PARAM_NAME,

  USER_PARAM_NAME,
  REPO_PARAM_NAME,
  BRANCH_PARAM_NAME,
  COMMIT_PARAM_NAME,
  HOST_PARAM_NAME,
  COMMAND_PARAM_NAME,
  PATTERN_PARAM_NAME,
  TAG_PARAM_NAME,
  MINIMUM_DURATION_PARAM_NAME,
  MAXIMUM_DURATION_PARAM_NAME,
];

export const GLOBAL_SORT_PARAM_NAMES = [SORT_BY_PARAM_NAME, SORT_ORDER_PARAM_NAME];

export const PERSISTENT_URL_PARAMS = [...GLOBAL_FILTER_PARAM_NAMES, ...GLOBAL_SORT_PARAM_NAMES];
