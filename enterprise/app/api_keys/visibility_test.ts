import { api_key } from "../../../proto/api_key_ts_proto";
import { defaultVisibility, initialVisibility, setDevelopersVisible } from "./visibility";

const DEVELOPERS = api_key.Visibility.VISIBLE_TO_DEVELOPERS;
const ADMINS = api_key.Visibility.VISIBLE_TO_GROUP_ADMINS;
// A bit with no corresponding Visibility value, standing in for a visibility
// added in the future.
const FUTURE = (1 << 2) as api_key.Visibility;

function sorted(visibility: api_key.Visibility[]): api_key.Visibility[] {
  return [...visibility].sort((a, b) => a - b);
}

interface KeyCase {
  name: string;
  key: api_key.IApiKey;
  // The visibility the update form should start with.
  initial: api_key.Visibility[];
}

const KEY_CASES: KeyCase[] = [
  {
    name: "unmigrated, admins only",
    key: { visibility: [], visibleToDevelopers: false },
    initial: [ADMINS],
  },
  {
    name: "unmigrated, visible to developers",
    key: { visibility: [], visibleToDevelopers: true },
    initial: [ADMINS, DEVELOPERS],
  },
  {
    name: "unmigrated, fields unset",
    key: {},
    initial: [ADMINS],
  },
  {
    name: "migrated, admins only",
    key: { visibility: [ADMINS], visibleToDevelopers: false },
    initial: [ADMINS],
  },
  {
    name: "migrated, visible to developers",
    key: { visibility: [ADMINS, DEVELOPERS], visibleToDevelopers: true },
    initial: [ADMINS, DEVELOPERS],
  },
  {
    // Visibility takes precedence over the deprecated field.
    name: "inconsistent, visibility lacks developers",
    key: { visibility: [ADMINS], visibleToDevelopers: true },
    initial: [ADMINS],
  },
  {
    name: "inconsistent, visibility has developers",
    key: { visibility: [ADMINS, DEVELOPERS], visibleToDevelopers: false },
    initial: [ADMINS, DEVELOPERS],
  },
  {
    name: "migrated, future bit",
    key: { visibility: [ADMINS, FUTURE], visibleToDevelopers: false },
    initial: [ADMINS, FUTURE],
  },
  {
    name: "migrated, future bit, visible to developers",
    key: { visibility: [ADMINS, DEVELOPERS, FUTURE], visibleToDevelopers: true },
    initial: [ADMINS, DEVELOPERS, FUTURE],
  },
];

// Sequences of checkbox states, as if the user clicked the checkbox that many
// times. Each entry is the new checked state.
const TOGGLE_SEQUENCES: boolean[][] = [[], [true], [false], [true, false], [false, true], [true, true], [false, false]];

describe("defaultVisibility", () => {
  it("is visible only to group admins", () => {
    expect(defaultVisibility()).toEqual([ADMINS]);
  });

  it("returns a new array each call", () => {
    const first = defaultVisibility();
    first.push(DEVELOPERS);
    expect(defaultVisibility()).toEqual([ADMINS]);
  });
});

describe("initialVisibility", () => {
  for (const c of KEY_CASES) {
    it(`handles ${c.name}`, () => {
      expect(sorted(initialVisibility(c.key))).toEqual(sorted(c.initial));
    });
  }

  it("does not alias the key's visibility", () => {
    const key = { visibility: [ADMINS] };
    initialVisibility(key).push(DEVELOPERS);
    expect(key.visibility).toEqual([ADMINS]);
  });
});

describe("setDevelopersVisible", () => {
  it("keeps group admins when an unmigrated key is made visible to developers", () => {
    const visibility = initialVisibility({ visibility: [], visibleToDevelopers: false });
    expect(sorted(setDevelopersVisible(visibility, true))).toEqual(sorted([ADMINS, DEVELOPERS]));
  });

  it("treats null and undefined as empty", () => {
    expect(setDevelopersVisible(null, true)).toEqual([DEVELOPERS]);
    expect(setDevelopersVisible(undefined, false)).toEqual([]);
  });

  it("does not mutate its input", () => {
    const visibility = [ADMINS, DEVELOPERS];
    setDevelopersVisible(visibility, false);
    setDevelopersVisible(visibility, true);
    expect(visibility).toEqual([ADMINS, DEVELOPERS]);
  });

  for (const c of KEY_CASES) {
    for (const toggles of TOGGLE_SEQUENCES) {
      it(`handles ${c.name} after toggling [${toggles.join(", ")}]`, () => {
        let visibility = initialVisibility(c.key);
        for (const checked of toggles) {
          visibility = setDevelopersVisible(visibility, checked);
        }

        const developersVisible = toggles.length ? toggles[toggles.length - 1] : c.initial.includes(DEVELOPERS);
        const expected: api_key.Visibility[] = c.initial.filter((v) => v !== DEVELOPERS);
        if (developersVisible) expected.push(DEVELOPERS);

        expect(sorted(visibility)).toEqual(sorted(expected));
        // Group admins can always see keys managed through this UI.
        expect(visibility).toContain(ADMINS);
        // Other bits survive edits.
        expect(visibility.includes(FUTURE)).toBe(c.initial.includes(FUTURE));
        expect(new Set(visibility).size).toBe(visibility.length);
      });
    }
  }
});
