import React from "react";

import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import type { Props } from "@theme/BlogTagsListPage";
import BlogSidebar from "../BlogSidebar";
import Translate from "@docusaurus/Translate";

function getCategoryOfTag(tag_label: string) {
  // tag's category should be customizable
  return tag_label[0].toUpperCase();
}

function BlogTagsListPage(props: Props): JSX.Element {
  const { tags, sidebar } = props;

  const tagCategories: { [category: string]: string[] } = {};
  Object.keys(tags).forEach((tag) => {
    const category = getCategoryOfTag(tags[tag].label);
    tagCategories[category] = tagCategories[category] || [];
    tagCategories[category].push(tag);
  });
  const tagsList = Object.entries(tagCategories).sort(([a], [b]) => {
    if (a === b) {
      return 0;
    }
    return a > b ? 1 : -1;
  });
  const tagsSection = tagsList
    .map(([category, tagsForCategory]) => (
      <div key={category}>
        <h3>{category}</h3>
        {tagsForCategory.map((tag) => (
          <Link className="padding-right--md" href={tags[tag].permalink} key={tag}>
            {tags[tag].label} ({tags[tag].count})
          </Link>
        ))}
        <hr />
      </div>
    ))
    .filter((item) => item != null);

  return (
    <Layout title="Tags" description="Blog Tags" wrapperClassName="blog-wrapper">
      <div className="blog-container margin-vert--lg">
        <div className="row">
          <main className="col col--8">
            <h1>
              <Translate id="theme.tags.tagsPageTitle" description="The title of the tag list page">
                Tags
              </Translate>
            </h1>
            <div className="margin-vert--lg">{tagsSection}</div>
          </main>
          <div className="col col--1"></div>
          <div className="col col--3">
            <BlogSidebar sidebar={sidebar} />
          </div>
        </div>
      </div>
    </Layout>
  );
}

export default BlogTagsListPage;
