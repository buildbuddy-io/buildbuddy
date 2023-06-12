import React from "react";

import Layout from "@theme/Layout";
import { BlogPostProvider } from "@docusaurus/theme-common/internal";
import BlogPostItem from "@theme/BlogPostItem";
import Link from "@docusaurus/Link";
import type { Props } from "@theme/BlogTagsPostsPage";
import BlogSidebar from "../BlogSidebar";
import Translate, { translate } from "@docusaurus/Translate";

// Very simple pluralization: probably good enough for now
function pluralizePosts(count: number): string {
  return count === 1
    ? translate(
        {
          id: "theme.blog.post.onePost",
          description: "Label to describe one blog post",
          message: "One post",
        },
        { count }
      )
    : translate(
        {
          id: "theme.blog.post.nPosts",
          description: "Label to describe multiple blog posts",
          message: "{count} posts",
        },
        { count }
      );
}

function BlogTagsPostPage(props: Props): JSX.Element {
  const { listMetadata, items, sidebar, tag } = props;
  const { allTagsPath, label, count } = tag;

  return (
    <Layout title={`Posts tagged "${label}"`} description={`Blog | Tagged "${label}"`} wrapperClassName="blog-wrapper">
      <div className="container margin-vert--lg">
        <div className="row">
          <div className="col col--1"></div>
          <main className="col col--8">
            <h1>
              <Translate
                id="theme.blog.tagTitle"
                description="The title of the page for a blog tag"
                values={{ nPosts: pluralizePosts(count), label }}>
                {'{nPosts} tagged with "{label}"'}
              </Translate>
            </h1>
            <Link href={allTagsPath}>
              <Translate id="theme.tags.tagsPageLink" description="The label of the link targeting the tag list page">
                View All Tags
              </Translate>
            </Link>
            <div className="margin-vert--xl">
              {items.map(({ content: BlogPostContent }) => (
                <BlogPostProvider
                  key={BlogPostContent.metadata.permalink}
                  frontMatter={BlogPostContent.frontMatter}
                  assets={BlogPostContent.assets}
                  metadata={BlogPostContent.metadata}
                  content={BlogPostContent}
                  truncated>
                  <BlogPostItem truncated>
                    <BlogPostContent />
                  </BlogPostItem>
                </BlogPostProvider>
              ))}
            </div>
          </main>
          <div className="col col--3">
            <BlogSidebar sidebar={sidebar} />
          </div>
        </div>
      </div>
    </Layout>
  );
}

export default BlogTagsPostPage;
