import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import rpcService from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import Spinner from "../../../app/components/spinner/spinner";
import RepositoryComponent from "./repository";

interface State {
  loading: boolean;
  response?: registry.GetCatalogResponse
  inputText: string;
}

interface Props {
  path: string;
  search: URLSearchParams;
}

export default class RegistryComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
  };

getCatalog() {
  this.setState({ loading: true, response: undefined });
  rpcService.service
    .getCatalog(new registry.GetCatalogRequest({}))
    .then((response) => {
      this.setState({ response: response });
    })
    .catch((e) => errorService.handleError(e))
    .finally(() => this.setState({ loading: false })); 
}

componentDidMount() {
  this.getCatalog();
}

renderTheRestOfTheOwl() {
  if (!this.state.response) {
    return <div>You should never see this!</div>;
  }

  if (this.state.response.repository.length === 0) {
    return (
      <div className="no-results">
        <div className="circle">
          <h2>Nothin' here</h2>
        </div>
      </div>
    );
  }


  return (
    <div>
      {this.state.response.repository.map((repository) => (
        <RepositoryComponent repository={repository}></RepositoryComponent>
      ))}
    </div>
  );
}

  render() {
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
