import DiffMatchPatch from "diff-match-patch";
import React from "react";

const dmp = new DiffMatchPatch.diff_match_patch();

export interface Diff {
  type: number; // -1 = removed, 0 = equal, 1 = added
  text: string;
}

/**
 * Computes semantic diffs between two strings.
 */
export function computeDiffs(text1: string, text2: string): Diff[] {
  const diffs = dmp.diff_main(text1, text2);
  dmp.diff_cleanupSemantic(diffs);
  return diffs.map(([type, text]: [number, string]) => ({ type, text }));
}

/**
 * Renders diff components for the left side (showing removals).
 */
export function renderLeftDiffs(diffs: Diff[]): React.ReactNode {
  return diffs.map((d, index) => {
    if (d.type === -1) {
      return (
        <span key={index} className="difference-left">
          {d.text}
        </span>
      );
    }
    if (d.type === 0) {
      return <React.Fragment key={index}>{d.text}</React.Fragment>;
    }
    return null;
  });
}

/**
 * Renders diff components for the right side (showing additions).
 */
export function renderRightDiffs(diffs: Diff[]): React.ReactNode {
  return diffs.map((d, index) => {
    if (d.type === 1) {
      return (
        <span key={index} className="difference-right">
          {d.text}
        </span>
      );
    }
    if (d.type === 0) {
      return <React.Fragment key={index}>{d.text}</React.Fragment>;
    }
    return null;
  });
}

/**
 * Renders a comparison row with diff highlighting.
 */
export function renderComparisonRow(
  name: string,
  valueA: string | undefined,
  valueB: string | undefined,
  options?: {
    showChangesOnly?: boolean;
    multiline?: boolean;
    className?: string;
  }
): React.ReactNode | null {
  const different = valueA !== valueB;

  if (!different && options?.showChangesOnly) {
    return null;
  }

  if (!valueA && !valueB) {
    return null;
  }

  let diffsA: React.ReactNode = valueA || "";
  let diffsB: React.ReactNode = valueB || "";

  if (different && valueA && valueB) {
    const diffs = computeDiffs(valueA, valueB);
    diffsA = renderLeftDiffs(diffs);
    diffsB = renderRightDiffs(diffs);
  }

  const classNames = [
    "compare-row",
    different ? "different" : "",
    options?.multiline ? "multiline" : "",
    options?.className || "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={classNames}>
      <div className="facet-name">{name}</div>
      <div className="facet-value">{diffsA}</div>
      <div className="facet-value">{diffsB}</div>
    </div>
  );
}

interface ComparisonFacet<T> {
  name: string;
  facet: (item?: T) => string | undefined;
  type?: string;
  link?: (item?: T) => string | undefined;
}

/**
 * Renders a list of comparison facets.
 */
export function renderComparisonFacets<T>(
  facets: ComparisonFacet<T>[],
  itemA: T | undefined,
  itemB: T | undefined,
  options?: {
    showChangesOnly?: boolean;
    filterType?: string;
  }
): React.ReactNode {
  return facets.map((f, index) => {
    // Filter by type if specified
    if (options?.filterType && f.type !== options.filterType) {
      return null;
    }

    const valueA = f.facet(itemA);
    const valueB = f.facet(itemB);
    const different = valueA !== valueB;

    if (!different && options?.showChangesOnly) {
      return null;
    }

    if (!valueA && !valueB) {
      return null;
    }

    let diffsA: React.ReactNode = valueA || "";
    let diffsB: React.ReactNode = valueB || "";

    if (different && valueA && valueB) {
      const diffs = computeDiffs(valueA, valueB);
      diffsA = renderLeftDiffs(diffs);
      diffsB = renderRightDiffs(diffs);
    }

    // Wrap in links if link function is provided
    if (f.link) {
      const linkA = f.link(itemA);
      const linkB = f.link(itemB);
      if (linkA) {
        diffsA = (
          <a target="_blank" href={linkA}>
            {diffsA}
          </a>
        );
      }
      if (linkB) {
        diffsB = (
          <a target="_blank" href={linkB}>
            {diffsB}
          </a>
        );
      }
    }

    return (
      <div key={index} className={`compare-row ${different ? "different" : ""}`}>
        <div>{f.name}</div>
        <div className={f.link ? "link" : ""}>{diffsA}</div>
        <div className={f.link ? "link" : ""}>{diffsB}</div>
      </div>
    );
  });
}
