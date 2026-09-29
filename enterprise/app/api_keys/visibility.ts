import { api_key } from "../../../proto/api_key_ts_proto";

/** Returns the visibility for a newly-created API key. */
export function defaultVisibility(): api_key.Visibility[] {
  return [api_key.Visibility.VISIBLE_TO_GROUP_ADMINS];
}

/**
 * Returns the visibility to populate the update form with for an existing API
 * key. Keys whose visibility has not yet been migrated are returned by the
 * server with an empty visibility, so derive it from visible_to_developers the
 * same way the server does.
 */
export function initialVisibility(apiKey: api_key.IApiKey): api_key.Visibility[] {
  if (apiKey.visibility?.length) {
    return [...apiKey.visibility];
  }
  return setDevelopersVisible(defaultVisibility(), Boolean(apiKey.visibleToDevelopers));
}

/**
 * Returns a copy of the given visibility with only the developers bit changed,
 * so that other bits survive an edit.
 */
export function setDevelopersVisible(
  visibility: api_key.Visibility[] | null | undefined,
  visible: boolean
): api_key.Visibility[] {
  const result: api_key.Visibility[] = (visibility ?? []).filter((v) => v !== api_key.Visibility.VISIBLE_TO_DEVELOPERS);
  if (visible) {
    result.push(api_key.Visibility.VISIBLE_TO_DEVELOPERS);
  }
  return result;
}
