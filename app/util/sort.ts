import Long from "long";

export type Comparator<T = any> = (a: T, b: T) => number;

export function compareStrings(a: string, b: string): number {
  return a.localeCompare(b);
}

export function compareLongs(a: Long, b: Long): number {
  return a.compare(b);
}

export function compareNumbers(a: number, b: number): number {
  return a - b;
}

export function compareStringRepresentations(a: any, b: any): number {
  return String(a).localeCompare(String(b));
}

export function reverseComparator<T>(comp: Comparator<T>): Comparator<T> {
  return (a: T, b: T) => comp(b, a);
}
