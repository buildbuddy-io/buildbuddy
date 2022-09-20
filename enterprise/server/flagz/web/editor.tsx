import React from "react";
import ReactDOM from "react-dom";
import rpcService from "../../../../app/service/rpc_service";
import { flagz } from "../../../../proto/flagz_ts_proto";
import { BuildBuddyError } from "../../../../app/util/errors";
import { User } from "../../../../app/auth/auth_service";

interface State {
  user: User;
  loading: boolean;
  error: BuildBuddyError | null;
}

interface Props {
  flags: string;
}

class Editor extends React.Component<Props, State> {
  state: State = {
    user: null,
    loading: true,
    error: null,
  };

  props: Props = {
    flags: "",
  };
  private textAreaRef = React.createRef<HTMLTextAreaElement>();

  fetchFlags() {
    let request = new flagz.GetFlagzRequest();
    rpcService.service
      .getFlagz(request)
      .then((response: flagz.GetFlagzResponse) => {
        console.log(response);
        this.props.flags = new TextDecoder().decode(response.yamlConfig);
        this.setState({
          loading: false,
          error: null,
        });
        this.textAreaRef.current.value = this.props.flags;
      })
      .catch((error: any) => {
        console.error(error);
        this.props.flags = "";
        this.setState({
          error: BuildBuddyError.parse(error),
          loading: false,
        });
      });
  }

  pushFlags() {
    let request = new flagz.SetFlagzRequest();
    this.props.flags = this.textAreaRef.current.value;
    request.yamlUpdate = new TextEncoder().encode(this.props.flags);
    rpcService.service
      .setFlagz(request)
      .then((response: flagz.SetFlagzResponse) => {
        console.log(response);
      })
      .catch((error: any) => {
        console.error(error);
        this.setState({
          error: BuildBuddyError.parse(error),
          loading: false,
        });
      });
  }

  componentWillMount() {
    this.fetchFlags();
  }

  componentDidMount() {}

  render() {
    return (
      <div>
        <textarea ref={this.textAreaRef} className="editor-root" cols={120} rows={40} /> <br />{" "}
        <button onClick={this.pushFlags.bind(this)}>Push</button>
      </div>
    );
  }
}

ReactDOM.render(<Editor />, document.getElementById("root") as HTMLElement);
