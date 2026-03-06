import clsx from "clsx";
import React from "react";

import { ChangelogHeader } from "./changelogHeader";
import styles from "./styles.module.css";

type Props = {
  selectedTagUrl: string;
  className?: string;
};

export default function ChangelogEmptyFilterState({ selectedTagUrl, className }: Props): JSX.Element {
  return (
    <main className={clsx("container margin-vert--lg", styles.wrapper, className)}>
      <ChangelogHeader selectedTagUrl={selectedTagUrl} />
      <p className={styles.emptyState}>No changelog entries found for the selected filter.</p>
    </main>
  );
}
