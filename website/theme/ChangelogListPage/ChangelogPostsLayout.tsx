import Link from "@docusaurus/Link";
import type { BlogPaginatedMetadata } from "@docusaurus/plugin-content-blog";
import { MDXProvider } from "@mdx-js/react";
import BlogListPaginator from "@theme/BlogListPaginator";
import Layout from "@theme/Layout";
import MDXComponents from "@theme/MDXComponents";
import clsx from "clsx";
import React from "react";

import { ChangelogHeader } from "./changelogHeader";
import styles from "./styles.module.css";

// These fields are set by docusaurus.
export type ChangelogItem = {
  readonly content: {
    frontMatter: {
      title: string;
    };
    metadata: {
      date: string;
      permalink: string;
      tags: ChangelogPostTag[];
      authors: { name: string }[];
    };
  };
};

type ChangelogPostTag = {
  label: string;
  permalink: string;
};

type Props = {
  items: readonly ChangelogItem[];
  metadata: BlogPaginatedMetadata;
  selectedTagUrl?: string | null;
};

export default function ChangelogPostsLayout({ items, metadata, selectedTagUrl = null }: Props): JSX.Element {
  return (
    <Layout title="BuildBuddy Changelog" description="Changelog">
      <div className={clsx("container margin-vert--lg", styles.wrapper)}>
        <ChangelogHeader selectedTagUrl={selectedTagUrl} />

        <main className={styles.list}>
          {items.map(({ content: ChangelogContent }) => {
            const { title } = ChangelogContent.frontMatter;
            const { date, permalink: url, tags, authors } = ChangelogContent.metadata;
            const author = authors[0]?.name;

            return (
              <article key={url} className={styles.item}>
                <time dateTime={date} className={styles.date}>
                  {formatDate(date)}
                </time>
                <h2 className={styles.itemTitle}>
                  <Link to={url}>{title}</Link>
                </h2>
                {author && <span className={styles.author}>{author}</span>}
                <div className={styles.preview}>
                  <MDXProvider components={MDXComponents}>
                    <ChangelogContent />
                  </MDXProvider>
                </div>
                <div className={styles.footer}>
                  {tags.length > 0 && (
                    <div className={styles.tagRow}>
                      {tags.map(({ label, permalink: url }) => (
                        <Link to={url} key={label} className={styles.tag}>
                          {label}
                        </Link>
                      ))}
                    </div>
                  )}
                </div>
              </article>
            );
          })}
        </main>
        <div className={styles.paginator}>
          <BlogListPaginator metadata={metadata} />
        </div>
      </div>
    </Layout>
  );
}

function formatDate(rawDate: string) {
  return new Date(rawDate).toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}
