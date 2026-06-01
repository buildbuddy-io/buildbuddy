/**
 * Deduplicates repeated strings by assigning each unique string a numeric ID.
 * Callers can store IDs instead of many copies of the same string, reducing
 * memory usage. IDs are one-based, so callers can use 0 as a sentinel for
 * missing values in dense numeric arrays.
 */
export class StringInterner {
  private strings: string[] = [];
  private ids: Map<string, number> | null = new Map();

  /** Returns the interned ID for the given string. */
  intern(value: string): number {
    if (!this.ids) {
      throw new Error("StringInterner is frozen");
    }

    const id = this.ids.get(value);
    if (id !== undefined) return id;

    const nextID = this.strings.length + 1;
    this.strings.push(value);
    this.ids.set(value, nextID);
    return nextID;
  }

  /**
   * Releases the string to ID lookup map and prevents further calls to intern.
   * Call this when no more strings will be added to reduce idle memory usage.
   */
  freeze(): void {
    this.ids = null;
  }

  /** Returns the string for the given interned ID. */
  get(id: number): string {
    return this.strings[id - 1];
  }
}
