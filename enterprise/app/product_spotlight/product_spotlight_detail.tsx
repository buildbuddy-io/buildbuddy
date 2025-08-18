import React from "react";
import { ArrowLeft, Calendar, User, Tag, ExternalLink } from "lucide-react";
import { User as AuthUser } from "../../../app/auth/auth_service";
import router from "../../../app/router/router";
import SpotlightsService from "./highlights_service";

interface SpotlightMetadata {
  title: string;
  description: string;
  category: string;
  priority: "high" | "medium" | "low";
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
        spotlightsService.getAllSpotlights()
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
    // Simple markdown rendering - in a real implementation you'd use a proper markdown parser
    const lines = content.split('\n');
    const elements: JSX.Element[] = [];
    
    lines.forEach((line, index) => {
      if (line.startsWith('# ')) {
        elements.push(<h1 key={index}>{line.substring(2)}</h1>);
      } else if (line.startsWith('## ')) {
        elements.push(<h2 key={index}>{line.substring(3)}</h2>);
      } else if (line.startsWith('### ')) {
        elements.push(<h3 key={index}>{line.substring(4)}</h3>);
      } else if (line.startsWith('- **') && line.includes('**')) {
        // Handle bold list items
        const boldMatch = line.match(/- \*\*(.*?)\*\*(.*)/);
        if (boldMatch) {
          elements.push(
            <li key={index}>
              <strong>{boldMatch[1]}</strong>{boldMatch[2]}
            </li>
          );
        }
      } else if (line.startsWith('- ')) {
        elements.push(<li key={index}>{line.substring(2)}</li>);
      } else if (line.startsWith('```')) {
        // Handle code blocks
        if (line.length > 3) {
          elements.push(<div key={index} className="code-block-lang">{line.substring(3)}</div>);
        }
      } else if (line.trim() === '') {
        elements.push(<br key={index} />);
      } else if (line.trim()) {
        // Handle inline formatting
        let processedLine = line;
        
        // Bold text
        processedLine = processedLine.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
        
        // Code spans
        processedLine = processedLine.replace(/`([^`]+)`/g, '<code>$1</code>');
        
        elements.push(
          <p key={index} dangerouslySetInnerHTML={{ __html: processedLine }} />
        );
      }
    });

    return <div className="markdown-content">{elements}</div>;
  }

  getRelatedSpotlights(): Spotlight[] {
    const { spotlight, allSpotlights } = this.state;
    if (!spotlight) return [];

    return allSpotlights
      .filter((s) => s.id !== spotlight.id && s.metadata.category === spotlight.metadata.category)
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

    const { title, category, date, tags, author } = spotlight.metadata;
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
              <div className="category-and-status">
                <span className="spotlight-category">{category}</span>
              </div>
              
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
              </div>
            </div>
          </div>

          <div className="spotlight-detail-content">
            {this.renderMarkdownContent(spotlight.content)}
          </div>

          {relatedSpotlights.length > 0 && (
            <div className="related-spotlights">
              <h3>Related Spotlights</h3>
              <div className="related-spotlights-grid">
                {relatedSpotlights.map((related) => (
                  <a
                    key={related.id}
                    href={`/product-spotlight/${related.id}`}
                    className="related-spotlight-card"
                  >
                    <div className="related-spotlight-meta">
                      <span className="spotlight-category">{related.metadata.category}</span>
                    </div>
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