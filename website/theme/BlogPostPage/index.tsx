import React from "react";
import Layout from "@theme/Layout";
import BlogPostItem from "../BlogPostItem";
import BlogPostPaginator from "@theme/BlogPostPaginator";
import type { Props } from "@theme/BlogPostPage";
import BlogSidebar from "../BlogSidebar";
import EditThisPage from "@theme/EditThisPage";

import styles from "./styles.module.css";

function BlogPostPage(props: Props): JSX.Element {
  const { content: BlogPostContents, sidebar } = props;
  const { frontMatter, metadata } = BlogPostContents;
  const { title, description, nextItem, prevItem, editUrl } = metadata;

  return (
    <Layout title={title} description={description} wrapperClassName="blog-wrapper">
      {BlogPostContents && (
        <div className="container margin-vert--lg">
          <div className="row">
            <div className="col col--1"></div>
            <main className="col col--8">
              <BlogPostItem frontMatter={frontMatter} metadata={metadata} isBlogPostPage>
                <BlogPostContents />
              </BlogPostItem>
              <div>{editUrl && <EditThisPage editUrl={editUrl} />}</div>
              {(nextItem || prevItem) && (
                <div className="margin-vert--xl">
                  <BlogPostPaginator nextItem={nextItem} prevItem={prevItem} />
                </div>
              )}
            </main>
            {
              <div className="col col--3">
                <BlogSidebar sidebar={sidebar} />
              </div>
            }
          </div>
        </div>
      )}
    </Layout>
  );
}

export default BlogPostPage;
