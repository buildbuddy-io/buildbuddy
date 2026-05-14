// Tag-filtered changelog list page (Ex: /changelog/tags/ai).
// Configured by the docusaurus `blogTagsPostsComponent` with BlogTagsPostsPageProps.
import type { Props as BlogTagsPostsPageProps } from "@theme/BlogTagsPostsPage";
import Layout from "@theme/Layout";
import React from "react";

import ChangelogEmptyFilterState from "../ChangelogListPage/ChangelogEmptyFilterState";
import ChangelogPostsLayout, { type ChangelogItem } from "../ChangelogListPage/ChangelogPostsLayout";

export default function FilteredChangelogListPage(props: BlogTagsPostsPageProps): JSX.Element {
  if (props.items.length === 0) {
    return (
      <Layout title="BuildBuddy Changelog" description="Changelog">
        <ChangelogEmptyFilterState selectedTagUrl={props.tag.permalink} />
      </Layout>
    );
  }

  return (
    <ChangelogPostsLayout
      items={props.items as readonly ChangelogItem[]}
      metadata={props.listMetadata}
      selectedTagUrl={props.tag.permalink}
    />
  );
}
