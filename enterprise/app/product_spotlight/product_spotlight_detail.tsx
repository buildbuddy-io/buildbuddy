import { ArrowLeft, Calendar, ExternalLink, Tag, User } from "lucide-react";
import React from "react";
import ReactMarkdown from "react-markdown";
import { User as AuthUser } from "../../../app/auth/auth_service";
import router from "../../../app/router/router";
import SpotlightsService from "./spotlight_service";

interface SpotlightMetadata {
  title: string;
  description: string;
  date: string;
  image?: string;
  tags: string[];
  author: string;
}

interface Spotlight {
  id: string;
  metadata: SpotlightMetadata;
  content: string;
}

interface Props {
  user: AuthUser;
  spotlightId: string;
  search: URLSearchParams;
}

interface State {
  spotlight: Spotlight | null;
  allSpotlights: Spotlight[];
  loading: boolean;
  notFound: boolean;
}

export default class ProductSpotlightDetailComponent extends React.Component<Props, State> {
  state: State = {
    spotlight: null,
    allSpotlights: [],
    loading: true,
    notFound: false,
  };

  componentWillMount() {
    this.loadSpotlight();
  }

  componentDidUpdate(prevProps: Props) {
    if (prevProps.spotlightId !== this.props.spotlightId) {
      this.loadSpotlight();
    }
  }

  async loadSpotlight() {
    this.setState({ loading: true, notFound: false });

    try {
      const spotlightsService = SpotlightsService.getInstance();

      // Load the specific spotlight and all spotlights for related content
      const [spotlight, allSpotlights] = await Promise.all([
        spotlightsService.getSpotlight(this.props.spotlightId),
        spotlightsService.getAllSpotlights(),
      ]);

      if (spotlight) {
        document.title = `${spotlight.metadata.title} | Product Spotlight | BuildBuddy`;
        this.setState({
          spotlight,
          allSpotlights,
          loading: false,
        });
      } else {
        this.setState({
          loading: false,
          notFound: true,
        });
      }
    } catch (error) {
      console.error("Failed to load spotlight:", error);
      this.setState({
        loading: false,
        notFound: true,
      });
    }
  }

  handleBackToList = () => {
    router.navigateTo("/product-spotlight/");
  };

  formatDate(dateString: string): string {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  }

  renderMarkdownContent(content: string): JSX.Element {
    return (
      <div className="markdown-content">
        <ReactMarkdown>{content}</ReactMarkdown>
      </div>
    );
  }

  getRelatedSpotlights(): Spotlight[] {
    const { spotlight, allSpotlights } = this.state;
    if (!spotlight) return [];
    if (!spotlight.metadata.tags || spotlight.metadata.tags.length == 0) return [];

    // Randomly select a spotlight with at least one matching tag
    return allSpotlights
      .filter(
        (s) =>
          s.metadata.tags &&
          s.id !== spotlight.id &&
          s.metadata.tags.some((tag) => spotlight.metadata.tags.includes(tag))
      )
      .sort(() => Math.random() - 0.5)
      .slice(0, 2);
  }

  render() {
    const { loading, spotlight, notFound } = this.state;

    if (loading) {
      return (
        <div className="product-spotlight-detail">
          <div className="container">
            <div className="loading">Loading spotlight...</div>
          </div>
        </div>
      );
    }

    if (notFound || !spotlight) {
      return (
        <div className="product-spotlight-detail">
          <div className="container">
            <div className="spotlight-not-found">
              <h1>Spotlight Not Found</h1>
              <p>The requested spotlight could not be found.</p>
              <button className="back-btn" onClick={this.handleBackToList}>
                <ArrowLeft size={16} />
                Back to Product Spotlight
              </button>
            </div>
          </div>
        </div>
      );
    }

    const { title, date, tags, author } = spotlight.metadata;
    const relatedSpotlights = this.getRelatedSpotlights();

    return (
      <div className="product-spotlight-detail">
        <div className="container">
          <div className="spotlight-detail-header">
            <button className="back-btn" onClick={this.handleBackToList}>
              <ArrowLeft size={16} />
              Back to Product Spotlight
            </button>

            <div className="spotlight-detail-meta">
              <h1 className="spotlight-detail-title">{title}</h1>

              <div className="spotlight-detail-info">
                <div className="info-item">
                  <User size={16} />
                  <span>{author}</span>
                </div>
                <div className="info-item">
                  <Calendar size={16} />
                  <span>{this.formatDate(date)}</span>
                </div>
                {tags && tags.length > 0 && (
                  <div className="info-item">
                    <Tag size={16} />
                    <div className="spotlight-tags">
                      {tags.map((tag, index) => (
                        <span key={index} className="spotlight-tag">
                          {tag}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div className="spotlight-detail-content">{this.renderMarkdownContent(spotlight.content)}</div>

          {relatedSpotlights.length > 0 && (
            <div className="related-spotlights">
              <h3>Related Spotlights</h3>
              <div className="related-spotlights-grid">
                {relatedSpotlights.map((related) => (
                  <a key={related.id} href={`/product-spotlight/${related.id}`} className="related-spotlight-card">
                    <h4>{related.metadata.title}</h4>
                    <p>{related.metadata.description}</p>
                    <ExternalLink size={16} />
                  </a>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
}
