import React from "react";
import clsx from "clsx";
import { MDXProvider } from "@mdx-js/react";
import Translate, { translate } from "@docusaurus/Translate";
import Link from "@docusaurus/Link";
import MDXComponents from "@theme/MDXComponents";
import Seo from "@theme/Seo";
import type { Props } from "@theme/BlogPostItem";

import styles from "./styles.module.css";

const MONTHS = [
  translate({
    id: "theme.common.month.january",
    description: "January month translation",
    message: "January",
  }),
  translate({
    id: "theme.common.month.february",
    description: "February month translation",
    message: "February",
  }),
  translate({
    id: "theme.common.month.march",
    description: "March month translation",
    message: "March",
  }),
  translate({
    id: "theme.common.month.april",
    description: "April month translation",
    message: "April",
  }),
  translate({
    id: "theme.common.month.may",
    description: "May month translation",
    message: "May",
  }),
  translate({
    id: "theme.common.month.june",
    description: "June month translation",
    message: "June",
  }),
  translate({
    id: "theme.common.month.july",
    description: "July month translation",
    message: "July",
  }),
  translate({
    id: "theme.common.month.august",
    description: "August month translation",
    message: "August",
  }),
  translate({
    id: "theme.common.month.september",
    description: "September month translation",
    message: "September",
  }),
  translate({
    id: "theme.common.month.october",
    description: "October month translation",
    message: "October",
  }),
  translate({
    id: "theme.common.month.november",
    description: "November month translation",
    message: "November",
  }),
  translate({
    id: "theme.common.month.december",
    description: "December month translation",
    message: "December",
  }),
];

function BlogPostItem(props: Props): JSX.Element {
  const { children, frontMatter, metadata, truncated, isBlogPostPage = false } = props;
  const { date, permalink, tags, readingTime } = metadata;
  const { author, title, keywords, subtitle, authorURL, authorTitle, authorImageURL, coverImage, seoImage } = mapKeys(
    underscoreToCamelCase,
    frontMatter
  );

  const renderPostHeader = () => {
    const TitleHeading = isBlogPostPage ? "h1" : "h2";
    const SubtitleHeading = isBlogPostPage ? "h2" : "h3";
    const match = date.substring(0, 10).split("-");
    const year = match[0];
    const month = MONTHS[parseInt(match[1], 10) - 1];
    const day = parseInt(match[2], 10);

    return (
      <header>
        {isBlogPostPage && coverImage && <img className={styles.coverImage} alt="" src={coverImage} />}
        <TitleHeading className={clsx("margin-bottom--sm", styles.blogPostTitle)}>
          {isBlogPostPage ? title : <Link to={permalink}>{title}</Link>}
        </TitleHeading>
        {subtitle && <SubtitleHeading className={styles.subtitle}>{subtitle}</SubtitleHeading>}
        <div className="margin-vert--md">
          <time dateTime={date} className={styles.blogPostDate}>
            <Translate
              id="theme.blog.post.date"
              description="The label to display the blog post date"
              values={{ day, month, year }}>
              {"{month} {day}, {year}"}
            </Translate>{" "}
            {readingTime && (
              <>
                {" · "}
                <Translate
                  id="theme.blog.post.readingTime"
                  description="The label to display reading time of the blog post"
                  values={{
                    readingTime: Math.ceil(readingTime),
                  }}>
                  {"{readingTime} min read"}
                </Translate>
              </>
            )}
          </time>
        </div>
      </header>
    );
  };

  return (
    <>
      <Seo {...{ keywords, image: seoImage || coverImage }} />

      <article className={!isBlogPostPage ? "margin-bottom--xl" : undefined}>
        {renderPostHeader()}
        <div className="markdown">
          <MDXProvider components={MDXComponents}>{children}</MDXProvider>
        </div>
        {author && <h4 className={styles.blogAuthorTitle}>Author:</h4>}
        <div className="avatar margin-vert--md">
          {authorImageURL && (
            <Link className="avatar__photo-link avatar__photo" href={authorURL}>
              <img src={authorImageURL} alt={author} />
            </Link>
          )}
          <div className="avatar__intro">
            {author && (
              <>
                <h4 className="avatar__name">
                  <Link href={authorURL}>{author}</Link>
                </h4>
                <small className="avatar__subtitle">{authorTitle}</small>
              </>
            )}
          </div>
        </div>
        {(tags.length > 0 || truncated) && (
          <footer className="row margin-vert--lg">
            {tags.length > 0 && (
              <div className="col">
                <strong>
                  <Translate id="theme.tags.tagsListLabel" description="The label alongside a tag list">
                    Tags:
                  </Translate>
                </strong>
                {tags.map(({ label, permalink: tagPermalink }) => (
                  <Link key={tagPermalink} className="margin-horiz--sm" to={tagPermalink}>
                    {label}
                  </Link>
                ))}
              </div>
            )}
            {truncated && (
              <div className="col text--right">
                <Link to={metadata.permalink} aria-label={`Read more about ${title}`}>
                  <strong>
                    <Translate
                      id="theme.blog.post.readMore"
                      description="The label used in blog post item excerpts to link to full blog posts">
                      Read More
                    </Translate>
                  </strong>
                </Link>
              </div>
            )}
          </footer>
        )}
      </article>
    </>
  );
}

function mapKeys<K extends string, V = any>(mapper: (_: string) => string, record: Record<K, V>): Record<K, V> {
  const out = {} as Record<K, V>;
  for (const [k, v] of Object.entries(record)) {
    out[mapper(k)] = v;
  }
  return out;
}

function underscoreToCamelCase(text: string): string {
  const tokens = text.split("_");
  const [first, ...rest] = tokens;
  let buffer = first.toLocaleLowerCase();
  for (const token of rest) {
    if (token === "url") {
      buffer += "URL";
      continue;
    }
    buffer += token[0].toLocaleUpperCase() + token.substring(1).toLocaleLowerCase();
  }
  return buffer;
}

export default BlogPostItem;
