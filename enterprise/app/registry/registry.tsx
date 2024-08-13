import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import rpcService from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import Spinner from "../../../app/components/spinner/spinner";
import RepositoryComponent from "./repository";
import ImageComponent from "./image";

interface State {
  loading: boolean;
  catalogResponse?: registry.GetCatalogResponse;
  imageResponse?: registry.Image;
  image: string;
}

interface Props {
  search: URLSearchParams;
}

// Extremely copied from codesearch.tsx. All bugs probably originate there.
export default class RegistryComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    catalogResponse: undefined,
    imageResponse: undefined,
    image: this.getImage(),
  };

  getImage() {
    return this.props.search.get("image") || "";
  }

  get() {
    if (this.getImage()) {
      this.setState({ loading: true, catalogResponse: undefined, imageResponse: undefined, image: this.getImage() });
      rpcService.service
        .getImage(new registry.GetImageRequest({ fullname: this.getImage() }))
        .then((response) => {
          this.setState({ imageResponse: response });
        })
        .catch((e) => errorService.handleError(e))
        .finally(() => this.setState({ loading: false }));
      return;
    }

    this.setState({ loading: true, catalogResponse: undefined, imageResponse: undefined, image: "" });
    rpcService.service
      .getCatalog(new registry.GetCatalogRequest({}))
      .then((response) => {
        this.setState({ catalogResponse: response });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  componentWillMount() {
    document.title = `Registry | BuildBuddy`;
  }

  componentDidMount() {
    this.get();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.search.get("image") != prevProps.search.get("image")) {
      this.get();
    }
  }

  renderTheRestOfTheOwl() {
    if (!this.state.catalogResponse) {
      return <div>You should never see this!</div>;
    }

    if (this.state.catalogResponse.repository.length === 0) {
      return <div className="registry-no-results">No repositories.</div>;
    }

    return (
      <div>
        {this.state.catalogResponse.repository.map((repository) => (
          <RepositoryComponent repository={repository}></RepositoryComponent>
        ))}
      </div>
    );
  }

  renderTheRestOfTheHawk() {
    if (!this.state.imageResponse) {
      return <div>You should never see this!</div>;
    }

    return <ImageComponent image={this.state.imageResponse}></ImageComponent>;

    return <div>TODO</div>;
  }

  render() {
    // Image page.
    if (this.state.image) {
      return (
        <div className="registry">
          <div className="shelf">
            <div className="title-bar">
              <div className="registry-title">Default's Container Registry</div>
            </div>
          </div>
          <div>
            {this.state.loading && (
              <div className="spinner-center">
                <Spinner></Spinner>
              </div>
            )}
            {!this.state.loading && this.renderTheRestOfTheHawk()}
          </div>
        </div>
      );
    }

    // Catalog page.
    return (
      <div className="registry">
        <div className="shelf">
          <div className="title-bar">
            <div className="registry-title">Default's Container Registry</div>
          </div>
        </div>
        <div>
          {this.state.loading && (
            <div className="spinner-center">
              <Spinner></Spinner>
            </div>
          )}
          {!this.state.loading && this.renderTheRestOfTheOwl()}
        </div>
      </div>
    );
  }
}
