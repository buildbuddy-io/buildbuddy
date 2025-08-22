import React from "react";
import { User } from "../../../app/auth/auth_service";
import router from "../../../app/router/router";
import SpotlightCard from "./spotlight_card";
import SpotlightsService from "./spotlight_service";

const CATEGORIES = ["bazel", "platform", "performance", "testing", "remote runners", "security"];

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
  user: User;
  search: URLSearchParams;
}

interface State {
  spotlights: Spotlight[];
  loading: boolean;
  filterCategory: string;
}

export default class ProductSpotlightComponent extends React.Component<Props, State> {
  state: State = {
    spotlights: [],
    loading: true,
    filterCategory: "all",
  };

  componentWillMount() {
    document.title = `Product Spotlight | BuildBuddy`;
    this.loadSpotlights();
  }

  async loadSpotlights() {
    try {
      const spotlightsService = SpotlightsService.getInstance();
      const spotlights = await spotlightsService.getAllSpotlights();

      this.setState({
        spotlights,
        loading: false,
      });
    } catch (error) {
      console.error("Failed to load spotlights:", error);
      this.setState({
        spotlights: [],
        loading: false,
      });
    }
  }

  handleSpotlightClick = (spotlight: Spotlight) => {
    router.navigateTo(`/product-spotlight/${spotlight.id}`);
  };

  handleFilterChange = (category: string) => {
    this.setState({ filterCategory: category });
  };

  getFilteredSpotlights(): Spotlight[] {
    const { spotlights, filterCategory } = this.state;
    if (filterCategory === "all") {
      return spotlights;
    }
    return spotlights.filter((s) => s.metadata.tags.includes(filterCategory));
  }

  render() {
    const { loading } = this.state;
    const filteredSpotlights = this.getFilteredSpotlights();

    if (loading) {
      return (
        <div className="product-spotlight">
          <div className="container">
            <div className="loading">Loading spotlights...</div>
          </div>
        </div>
      );
    }

    return (
      <div className="product-spotlight">
        <div className="shelf">
          <div className="container">
            <div className="title">Product Spotlight</div>
          </div>
        </div>
        <div className="container">
          <div className="content">
            <div className="section">
              <p>
                Welcome to Product Spotlight! Here you'll find highlighted features and the latest improvements to
                BuildBuddy.
              </p>

              <div className="spotlight-filters">
                <button
                  className={`filter-btn ${this.state.filterCategory === "all" ? "active" : ""}`}
                  onClick={() => this.handleFilterChange("all")}>
                  All
                </button>
                {CATEGORIES.map((category) => (
                  <button
                    key={category}
                    className={`filter-btn ${this.state.filterCategory === category ? "active" : ""}`}
                    onClick={() => this.handleFilterChange(category)}>
                    {category}
                  </button>
                ))}
              </div>

              <div className="spotlights-grid">
                {filteredSpotlights.map((spotlight) => (
                  <SpotlightCard
                    key={spotlight.id}
                    metadata={spotlight.metadata}
                    content={spotlight.content}
                    onClick={() => this.handleSpotlightClick(spotlight)}
                  />
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
