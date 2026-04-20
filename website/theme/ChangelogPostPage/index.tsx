import Link from "@docusaurus/Link";
import { MDXProvider } from "@mdx-js/react";
import type { Props } from "@theme/BlogPostPage";
import Layout from "@theme/Layout";
import MDXComponents from "@theme/MDXComponents";
import React from "react";

import styles from "./styles.module.css";

export default function ChangelogPostPage(props: Props): JSX.Element {
  const { content: ChangelogContent } = props;
  const { frontMatter, metadata } = ChangelogContent;
  const author = metadata.authors[0]?.name;

  return (
    <Layout title={frontMatter.title} description={metadata.description}>
      <div className="container margin-vert--lg">
        <article className={styles.article}>
          <Link to="/changelog" className={styles.backLink}>
            &larr; Back to changelog
          </Link>
          <h1>{frontMatter.title}</h1>
          <div className={styles.meta}>
            <time dateTime={metadata.date}>{formatDate(metadata.date)}</time>
            {author && (
              <>
                <span className={styles.metaDot}>&middot;</span>
                <span>{author}</span>
              </>
            )}
          </div>
          {metadata.tags.length > 0 && (
            <div className={styles.tags}>
              {metadata.tags.map(({ label }) => (
                <span key={label} className={styles.tag}>
                  {label}
                </span>
              ))}
            </div>
          )}
          <div className="markdown">
            <MDXProvider components={MDXComponents}>
              <ChangelogContent />
            </MDXProvider>
          </div>
        </article>
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
