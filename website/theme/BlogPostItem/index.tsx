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
  const { author, title, subtitle, image, keywords } = frontMatter;

  const authorURL = frontMatter.author_url || frontMatter.authorURL;
  const authorTitle = frontMatter.author_title || frontMatter.authorTitle;
  const authorImageURL = frontMatter.author_image_url || frontMatter.authorImageURL;

  const renderPostHeader = () => {
    const TitleHeading = isBlogPostPage ? "h1" : "h2";
    const SubtitleHeading = isBlogPostPage ? "h2" : "h3";
    const match = date.substring(0, 10).split("-");
    const year = match[0];
    const month = MONTHS[parseInt(match[1], 10) - 1];
    const day = parseInt(match[2], 10);

    return (
      <header>
        <TitleHeading className={clsx("margin-bottom--sm", styles.blogPostTitle)}>
          {isBlogPostPage ? title : <Link to={permalink}>{title}</Link>}
        </TitleHeading>
        {subtitle && <SubtitleHeading className={styles.subtitle}>{subtitle}</SubtitleHeading>}
        <div className="margin-vert--md">
          <div className={styles.heading}>
            <div className={styles.headingPhoto}>
              {authorImageURL && (
                <Link className={`avatar__photo-link avatar__photo ${styles.avatarImage}`} href={authorURL}>
                  <img src={authorImageURL} alt={author} />
                </Link>
              )}
            </div>
            <div className={styles.headingDetails}>
              <span>
                <Link className={styles.authorName} href={authorURL}>
                  {author}
                </Link>
                {", "}
                <span className={styles.authorTitle}>{authorTitle}</span>
              </span>
              <time dateTime={date} className={styles.blogPostDate}>
                <br />
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
          </div>
        </div>
      </header>
    );
  };

  return (
    <>
      <Seo {...{ keywords, image }} />

      <article className={!isBlogPostPage ? "margin-bottom--xl" : undefined}>
        {renderPostHeader()}
        <div className="markdown">
          <MDXProvider components={MDXComponents}>{children}</MDXProvider>
        </div>
        {(tags.length > 0 || truncated) && (
          <footer className="row margin-vert--lg">
            {tags.length > 0 && !truncated && (
              <div className={styles.tags}>
                <strong>
                  <Translate id="theme.tags.tagsListLabel" description="The label alongside a tag list">
                    Tags:
                  </Translate>
                </strong>
                {tags.map(({ label, permalink: tagPermalink }) => (
                  <Link key={tagPermalink} className={styles.tag} to={tagPermalink}>
                    {label}
                  </Link>
                ))}
              </div>
            )}
            {truncated && (
              <div className={styles.readMore}>
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

export default BlogPostItem;
