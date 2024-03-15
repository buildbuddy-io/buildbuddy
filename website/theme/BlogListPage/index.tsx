import React from "react";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import BlogPostItem from "../BlogPostItem";
import BlogListPaginator from "@theme/BlogListPaginator";
import type { Props } from "@theme/BlogListPage";
import Link from "@docusaurus/Link";

function BlogListPage(props: Props): JSX.Element {
  const { metadata, items, sidebar } = props;
  const {
    siteConfig: { title: siteTitle },
  } = useDocusaurusContext();
  const { blogDescription, blogTitle, permalink } = metadata;
  const isBlogOnlyMode = permalink === "/";
  const title = isBlogOnlyMode ? siteTitle : blogTitle;
  return (
    <Layout title={title} description={blogDescription} wrapperClassName="blog-wrapper">
      <div className="blog-container margin-vert--lg">
        <div className="row">
          <main className="blog-post-list col col--12">
            {items.map(({ content: BlogPostContent }) => (
              <Link href={BlogPostContent.metadata.permalink}>
                <BlogPostItem
                  key={BlogPostContent.metadata.permalink}
                  frontMatter={BlogPostContent.frontMatter}
                  metadata={BlogPostContent.metadata}
                  truncated={BlogPostContent.metadata.hasTruncateMarker}>
                  <BlogPostContent />
                </BlogPostItem>
              </Link>
            ))}
            <BlogListPaginator metadata={metadata} />
          </main>
        </div>
      </div>
    </Layout>
  );
}

export default BlogListPage;
