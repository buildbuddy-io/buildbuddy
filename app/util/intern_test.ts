import { StringInterner } from "./intern";

describe("StringInterner", () => {
  it("should return stable IDs for interned strings", () => {
    const interner = new StringInterner();

    const buildID = interner.intern("build");
    const testID = interner.intern("test");

    expect(buildID).toBe(0);
    expect(testID).toBe(1);
    expect(buildID).toBe(interner.intern("build"));
    expect(testID).toBe(interner.intern("test"));
    expect(interner.get(buildID)).toBe("build");
    expect(interner.get(testID)).toBe("test");
  });

  it("should reject new intern calls after freezing", () => {
    const interner = new StringInterner();

    const buildID = interner.intern("build");
    interner.freeze();

    expect(interner.get(buildID)).toBe("build");
    expect(() => interner.intern("build")).toThrowError("StringInterner is frozen");
  });
});
