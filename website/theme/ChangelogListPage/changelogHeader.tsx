import Link from "@docusaurus/Link";
import clsx from "clsx";
import React from "react";

import styles from "./styles.module.css";

export type ChangelogTag = {
  label: string;
  url: string;
};

export const CHANGELOG_TAGS: ChangelogTag[] = [
  { label: "featured", url: "/changelog/tags/featured" },
  { label: "bazel", url: "/changelog/tags/bazel" },
  { label: "platform", url: "/changelog/tags/platform" },
  { label: "performance", url: "/changelog/tags/performance" },
  { label: "testing", url: "/changelog/tags/testing" },
  { label: "remote runners", url: "/changelog/tags/remote-runners" },
  { label: "debugging", url: "/changelog/tags/debugging" },
  { label: "AI", url: "/changelog/tags/ai" },
];

type ChangelogHeaderProps = {
  selectedTagUrl?: string | null;
};

export function ChangelogHeader({ selectedTagUrl = null }: ChangelogHeaderProps): JSX.Element {
  const normalizedSelectedTagUrl = selectedTagUrl?.toLowerCase() ?? null;

  return (
    <>
      <header className={styles.header}>
        <h1 className={styles.title}>Changelog</h1>
      </header>
      <section className={styles.filters}>
        <Link
          to="/changelog"
          className={clsx(styles.filterButton, !normalizedSelectedTagUrl && styles.filterButtonActive)}>
          all
        </Link>
        {CHANGELOG_TAGS.map((tag) => (
          <Link
            key={tag.label}
            to={tag.url}
            data-noBrokenLinkCheck={true}
            className={clsx(
              styles.filterButton,
              normalizedSelectedTagUrl === tag.url.toLowerCase() && styles.filterButtonActive
            )}>
            {tag.label}
          </Link>
        ))}
      </section>
    </>
  );
}
