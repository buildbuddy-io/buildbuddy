import Link from "@docusaurus/Link";
import clsx from "clsx";
import React from "react";

import CHANGELOG_TAGS_DATA from "../../changelog/changelog-tags.json";
import styles from "./styles.module.css";

export type ChangelogTag = {
  label: string;
  url: string;
};

export const CHANGELOG_TAGS: ChangelogTag[] = CHANGELOG_TAGS_DATA;

type ChangelogHeaderProps = {
  selectedTagUrl?: string | null;
};

export function ChangelogHeader({ selectedTagUrl = null }: ChangelogHeaderProps): JSX.Element {
  const normalizedSelectedTagUrl = selectedTagUrl?.toLowerCase() ?? null;

  return (
    <>
      <header className={styles.header}>
        <h1 className={styles.title}>Changelog</h1>
        <p className={styles.subtitle}>The latest improvements to BuildBuddy.</p>
      </header>
      <nav className={styles.filters} aria-label="Filter changelog">
        <Link
          to="/changelog"
          aria-current={!normalizedSelectedTagUrl ? "page" : undefined}
          className={clsx(styles.filterButton, !normalizedSelectedTagUrl && styles.filterButtonActive)}>
          all
        </Link>
        {CHANGELOG_TAGS.map((tag) => (
          <Link
            key={tag.label}
            to={tag.url}
            data-noBrokenLinkCheck={true}
            aria-current={normalizedSelectedTagUrl === tag.url.toLowerCase() ? "page" : undefined}
            className={clsx(
              styles.filterButton,
              normalizedSelectedTagUrl === tag.url.toLowerCase() && styles.filterButtonActive
            )}>
            {tag.label}
          </Link>
        ))}
      </nav>
    </>
  );
}
