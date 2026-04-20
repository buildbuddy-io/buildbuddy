// Default changelog list page (/changelog).
// Configured by the docusaurus `blogListComponent` with BlogListPageProps.
import type { Props as BlogListPageProps } from "@theme/BlogListPage";
import React from "react";

import ChangelogPostsLayout, { type ChangelogItem } from "./ChangelogPostsLayout";

export default function ChangelogListPage(props: BlogListPageProps): JSX.Element {
  return (
    <ChangelogPostsLayout
      items={props.items as readonly ChangelogItem[]}
      metadata={props.metadata}
      selectedTagUrl={null}
    />
  );
}
