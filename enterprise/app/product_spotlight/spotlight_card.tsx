import React from "react";

interface SpotlightMetadata {
  title: string;
  description: string;
  category: string;
  date: string;
  tags: string[];
  author: string;
}

interface Props {
  metadata: SpotlightMetadata;
  content: string;
  onClick?: () => void;
}

export default class SpotlightCard extends React.Component<Props> {
  formatDate(dateString: string): string {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  }

  render() {
    const { metadata, onClick } = this.props;
    const { title, description, category, date, tags, author } = metadata;

    return (
      <div className="spotlight-card" onClick={onClick}>
        <div className="spotlight-content">
          <div className="spotlight-header">
            <div className="spotlight-meta">
              <span className="spotlight-category">{category}</span>
            </div>
            <h3 className="spotlight-title">{title}</h3>
            <p className="spotlight-description">{description}</p>
          </div>
          
          <div className="spotlight-footer">
            <div className="spotlight-tags">
              {tags.map((tag, index) => (
                <span key={index} className="spotlight-tag">
                  {tag}
                </span>
              ))}
            </div>
            <div className="spotlight-details">
              <span className="spotlight-author">{author}</span>
              <span className="spotlight-date">{this.formatDate(date)}</span>
            </div>
          </div>
        </div>
      </div>
    );
  }
}